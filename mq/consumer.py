"""通用消费者框架：手动 ACK、重试计数、幂等去重、随机失败模拟"""

import asyncio
import json
import logging
import random
import time
from typing import Awaitable, Callable

import aio_pika

from config import settings

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
) -> None:
    """生产级消费者框架。

    Args:
        queue_name: 监听的队列名
        tag: 日志标签
        color: ANSI 颜色代码
        process_fn: 异步业务处理函数
        simulate_seconds: 模拟处理耗时
        max_retries: 最大重试次数（超过后进入 DLX）
        failure_rate: 随机失败概率 (0.0~1.0)
        idempotent: 是否开启幂等去重
        worker_id: Worker 实例标识（Competing Consumers）
    """
    if max_retries is None:
        max_retries = settings.max_retries

    processed_ids: set[str] = set()
    wid = f"-W{worker_id}" if worker_id else ""
    full_tag = f"{tag}{wid}"

    logger.info(
        "%s 正在连接 RabbitMQ，监听 [%s]%s",
        _colorize(full_tag, color), queue_name, wid,
    )

    connection = await aio_pika.connect_robust(settings.amqp_url)
    async with connection:
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=1)
        queue = await channel.get_queue(queue_name)

        info_parts = [f"耗时 {simulate_seconds}s"]
        if failure_rate > 0:
            info_parts.append(f"失败率 {int(failure_rate * 100)}%")
        if idempotent:
            info_parts.append("幂等已开启")
        logger.info(
            "%s 已就绪%s，等待消息... (%s)",
            _colorize(full_tag, color), wid, ", ".join(info_parts),
        )

        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                body = json.loads(message.body.decode())
                order_id = body.get("order_id", "unknown")
                retry_count = get_retry_count(message)

                # 幂等去重
                if idempotent and order_id in processed_ids:
                    logger.info(
                        "%s 订单 %s 已处理过，跳过（幂等）",
                        _colorize(tag, color), order_id,
                    )
                    await message.ack()
                    continue

                t0 = time.perf_counter()
                retry_info = f" (重试第 {retry_count} 次)" if retry_count > 0 else ""
                logger.info(
                    "%s 收到订单 %s%s，开始处理...",
                    _colorize(full_tag, color), order_id, retry_info,
                )

                try:
                    if failure_rate > 0 and random.random() < failure_rate:
                        raise ProcessingError(
                            f"模拟处理失败 (失败率 {int(failure_rate * 100)}%)"
                        )

                    await asyncio.sleep(simulate_seconds)
                    await process_fn(body)

                    elapsed = time.perf_counter() - t0
                    logger.info(
                        "%s 订单 %s 处理完成 ✓ (%.2fs)",
                        _colorize(tag, color), order_id, elapsed,
                    )
                    if idempotent:
                        processed_ids.add(order_id)
                    await message.ack()

                except ProcessingError as e:
                    elapsed = time.perf_counter() - t0
                    logger.warning(
                        "%s 订单 %s 处理失败: %s (%.2fs)",
                        _colorize(tag, COLOR_RED), order_id, e, elapsed,
                    )

                    if retry_count < max_retries:
                        logger.info(
                            "%s → 重试 %d/%d，等待后重新投递...",
                            _colorize(tag, COLOR_YELLOW),
                            retry_count + 1, max_retries,
                        )
                        await message.reject(requeue=False)
                    else:
                        logger.error(
                            "%s ✗ 超过最大重试次数 (%d)，转入死信队列",
                            _colorize(tag, COLOR_RED), max_retries,
                        )
                        await message.reject(requeue=False)

                except Exception as e:
                    logger.exception(
                        "%s 订单 %s 未知异常: %s",
                        _colorize(tag, COLOR_RED), order_id, e,
                    )
                    await message.reject(requeue=False)


def setup_logging() -> None:
    """配置日志格式，所有服务共用"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d %(message)s",
        datefmt="%H:%M:%S",
        force=True,
    )
