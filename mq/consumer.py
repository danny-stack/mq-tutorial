"""通用消费者框架：手动 ACK、重试计数、幂等去重、随机失败模拟"""

import asyncio
import json
import logging
import random
import signal
import time
from collections.abc import Awaitable, Callable

import aio_pika

from config import settings
from mq.error_policy import ErrorClass, classify
from mq.idempotency import IdempotencyStore, InMemoryStore

logger = logging.getLogger("mq-tutorial")


def _colorize(tag: str, color_code: str) -> str:
    return f"\033[{color_code}m[{tag}]\033[0m"


COLOR_RED = "31"
COLOR_GREEN = "32"
COLOR_YELLOW = "33"
COLOR_BLUE = "34"
COLOR_MAGENTA = "35"
COLOR_CYAN = "36"


class ProcessingError(Exception):
    """业务处理失败，触发重试或进入 DLX"""


def get_retry_count(message: aio_pika.IncomingMessage) -> int:
    """从 message headers 读取 x-retry-count"""
    if message.headers and "x-retry-count" in message.headers:
        return int(message.headers["x-retry-count"])
    if message.headers and "x-death" in message.headers:
        deaths = message.headers["x-death"]
        if deaths:
            return int(deaths[0].get("count", 1))
    return 0


async def run_consumer(
    queue_name: str,
    tag: str,
    color: str,
    process_fn: Callable[[dict], Awaitable[None]],
    *,
    simulate_seconds: float = 0.0,
    max_retries: int | None = None,
    failure_rate: float = 0.0,
    idempotent: bool = True,
    worker_id: str = "",
    retry_exchange_name: str | None = None,
    error_policy: dict | None = None,
    idempotency_store: IdempotencyStore | None = None,
) -> None:
    """生产级消费者框架。

    Args:
        queue_name: 监听的队列名
        tag: 日志标签
        color: ANSI 颜色代码
        process_fn: 异步业务处理函数
        simulate_seconds: 模拟处理耗时
        max_retries: 最大重试次数（超过后进入 DLQ）
        failure_rate: 随机失败概率 (0.0~1.0)
        idempotent: 是否开启幂等去重
        worker_id: Worker 实例标识（Competing Consumers）
        retry_exchange_name: 延迟重试用的 exchange 名（如 "retry.exchange"）。
            仅对 topic exchange 上的队列（cv/nlp）传入：失败时把消息副本 publish 到此
            exchange 并保留原 routing_key，TTL 后死信回流到原队列。为 None 时失败
            直接 reject 进 DLQ（inventory/customs 走此路径）。
        error_policy: 自定义「异常类型→ErrorClass」映射，覆盖默认策略；None 用默认。
        idempotency_store: 幂等存储；None 用进程内 InMemoryStore（重启即失），
            生产环境注入 SqliteStore 或 Redis 实现以持久化跨重启去重。
    """
    if max_retries is None:
        max_retries = settings.max_retries

    store = idempotency_store if idempotency_store is not None else InMemoryStore()
    wid = f"-W{worker_id}" if worker_id else ""
    full_tag = f"{tag}{wid}"

    logger.info(
        "%s 正在连接 RabbitMQ，监听 [%s]%s",
        _colorize(full_tag, color),
        queue_name,
        wid,
    )

    connection = await aio_pika.connect_robust(settings.amqp_url)
    async with connection:
        # 优雅关停：收到 SIGINT/SIGTERM 后停止接收新消息。
        # prefetch=1 下，当前消息处理完后循环开头检查 stopping 即可干净退出；
        # 空闲等待消息时收到信号会在下一条消息到来时退出；robust 连接断开后，
        # broker 会把未 ACK 消息 at-least-once 重入队列，不丢消息。
        stopping = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, stopping.set)
            except NotImplementedError:
                pass  # Windows 等不支持 loop.add_signal_handler 的平台

        channel = await connection.channel()
        await channel.set_qos(prefetch_count=1)
        queue = await channel.get_queue(queue_name)
        # 按需取延迟重试 exchange；为 None 时失败直接进 DLQ（inventory/customs）。
        retry_exchange = (
            await channel.get_exchange(retry_exchange_name) if retry_exchange_name else None
        )

        async def _handle_failure(
            message: aio_pika.IncomingMessage, exc: BaseException, error_class: ErrorClass
        ) -> None:
            """按错误分类处理失败消息：TRANSIENT + 有 retry → 延迟重试；否则 → DLQ。"""
            rc = get_retry_count(message)
            if (
                error_class == ErrorClass.TRANSIENT
                and retry_exchange is not None
                and rc < max_retries
            ):
                # 副本 publish 到 retry.exchange(fanout) 携带原 routing_key；TTL 后
                # 死信回流到 order.compliance(topic)，按原 rk 回到原队列。
                new_headers = dict(message.headers or {})
                new_headers["x-retry-count"] = rc + 1
                await retry_exchange.publish(
                    aio_pika.Message(
                        body=message.body,
                        headers=new_headers,
                        delivery_mode=message.delivery_mode,
                        priority=message.priority,
                        message_id=message.message_id,
                        correlation_id=message.correlation_id,
                    ),
                    routing_key=message.routing_key,  # 保留原 rk
                    mandatory=False,  # fanout 必能投递
                )
                logger.info(
                    "%s → 重试 %d/%d，副本入 retry.exchange，等待 %dms 后回流",
                    _colorize(tag, COLOR_YELLOW),
                    rc + 1,
                    max_retries,
                    settings.retry_ttl_ms,
                )
                await message.ack()  # 原消息确认，避免与回流副本重复消费
            else:
                logger.error(
                    "%s ✗ 进入死信队列（class=%s, retry=%d/%d）",
                    _colorize(tag, COLOR_RED),
                    error_class.value,
                    rc,
                    max_retries,
                )
                await message.reject(requeue=False)

        info_parts = [f"耗时 {simulate_seconds}s"]
        if failure_rate > 0:
            info_parts.append(f"失败率 {int(failure_rate * 100)}%")
        if idempotent:
            info_parts.append("幂等已开启")
        logger.info(
            "%s 已就绪%s，等待消息... (%s)",
            _colorize(full_tag, color),
            wid,
            ", ".join(info_parts),
        )

        async with queue.iterator() as queue_iter:
            while True:
                # 用 wait_for 轮询：空闲时每 1s 回查 stopping，让 SIGTERM 在无消息流时
                # 也能及时退出（async for 会无限阻塞等下一条消息，错过关停信号）。
                try:
                    message = await asyncio.wait_for(queue_iter.__anext__(), timeout=1.0)
                except TimeoutError:
                    if stopping.is_set():
                        break
                    continue
                except StopAsyncIteration:
                    break

                if stopping.is_set():
                    # 取出消息后才发现已关停：回队列（broker 重投给其他实例）
                    await message.nack(requeue=True)
                    logger.info("%s 收到关停信号，消息回队列，停止消费", _colorize(full_tag, color))
                    break

                body = json.loads(message.body.decode())
                order_id = body.get("order_id", "unknown")
                retry_count = get_retry_count(message)

                # 幂等占位：首次 check_and_mark 返回 True 并标记；重复返回 False 跳过。
                # 处理失败时会 unmark 释放占位，保证重试消息能再次进入处理。
                if idempotent and not await store.check_and_mark(order_id):
                    logger.info(
                        "%s 订单 %s 已处理过，跳过（幂等）",
                        _colorize(tag, color),
                        order_id,
                    )
                    await message.ack()
                    continue

                t0 = time.perf_counter()
                retry_info = f" (重试第 {retry_count} 次)" if retry_count > 0 else ""
                logger.info(
                    "%s 收到订单 %s%s，开始处理...",
                    _colorize(full_tag, color),
                    order_id,
                    retry_info,
                )

                try:
                    if failure_rate > 0 and random.random() < failure_rate:
                        raise ProcessingError(f"模拟处理失败 (失败率 {int(failure_rate * 100)}%)")

                    await asyncio.sleep(simulate_seconds)
                    await process_fn(body)

                    elapsed = time.perf_counter() - t0
                    logger.info(
                        "%s 订单 %s 处理完成 ✓ (%.2fs)",
                        _colorize(tag, color),
                        order_id,
                        elapsed,
                    )
                    await message.ack()

                except ProcessingError as e:
                    # 向后兼容：ProcessingError 视为 TRANSIENT（模拟的临时失败）
                    elapsed = time.perf_counter() - t0
                    logger.warning(
                        "%s 订单 %s 处理失败: %s (%.2fs)",
                        _colorize(tag, COLOR_RED),
                        order_id,
                        e,
                        elapsed,
                    )
                    if idempotent:
                        await store.unmark(order_id)  # 释放占位，让重试能再进
                    await _handle_failure(message, e, ErrorClass.TRANSIENT)

                except Exception as e:
                    # 未知异常按 error_policy 分类（默认 PERMANENT → 直接 DLQ）
                    logger.exception(
                        "%s 订单 %s 未知异常: %s",
                        _colorize(tag, COLOR_RED),
                        order_id,
                        e,
                    )
                    if idempotent:
                        await store.unmark(order_id)
                    await _handle_failure(message, e, classify(e, error_policy))


def setup_logging() -> None:
    """配置日志格式，所有服务共用"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d %(message)s",
        datefmt="%H:%M:%S",
        force=True,
    )
