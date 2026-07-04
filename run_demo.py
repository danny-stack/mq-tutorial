"""一键演示：6 个生产级场景

运行：python run_demo.py
"""

import asyncio
import json
import logging
import subprocess
import sys
import time

import aio_pika

from config import settings
from mq.consumer import setup_logging
from topology import EXCHANGE_MAP, EXCHANGES, QUEUE_MAP, QUEUES

s = settings
logger = logging.getLogger("mq-tutorial")


def run_bg(cmd: list[str]) -> subprocess.Popen:
    return subprocess.Popen(cmd, stdout=sys.stdout, stderr=sys.stderr)


def banner(num: int, title: str) -> None:
    print(f"\n{'=' * 65}")
    print(f"  场景 {num}: {title}")
    print(f"{'=' * 65}\n", flush=True)


def section(text: str) -> None:
    print(f"  {text}", flush=True)


def wait(seconds: float, desc: str = "") -> None:
    if desc:
        print(f"  ⏳ 等待 {seconds}s ({desc})...", flush=True)
    time.sleep(seconds)


def cleanup_queues() -> None:
    """清空所有队列，避免场景间消息干扰"""

    async def _clean() -> None:
        conn = await aio_pika.connect_robust(s.amqp_url)
        async with conn:
            ch = await conn.channel()
            for q in QUEUE_MAP.values():
                try:
                    queue = await ch.get_queue(q.name)
                    await queue.purge()
                except Exception:
                    pass

    asyncio.run(_clean())


def check_topology() -> list[str]:
    """被动检查（passive declare）拓扑是否已在 broker 上预配。

    只验证存在性、**不创建任何资源**——生产实践要求拓扑由运维预配，
    应用代码只 get 不 declare。返回缺失项列表（空 = 全部就绪）。
    """

    async def _check() -> list[str]:
        missing: list[str] = []
        conn = await aio_pika.connect_robust(s.amqp_url)
        async with conn:
            # 每个资源独立 channel：passive declare 命中 404 会关掉当前 channel
            for ex in EXCHANGES:
                ch = await conn.channel()
                try:
                    await ch.declare_exchange(ex.name, aio_pika.ExchangeType(ex.type), passive=True)
                except Exception:
                    missing.append(f"exchange:{ex.name}")
                finally:
                    await ch.close()
            for q in QUEUES:
                ch = await conn.channel()
                try:
                    await ch.declare_queue(q.name, passive=True)
                except Exception:
                    missing.append(f"queue:{q.name}")
                finally:
                    await ch.close()
        return missing

    return asyncio.run(_check())


def main() -> None:
    setup_logging()

    print(f"\n{'=' * 65}")
    print("  跨境电商异步消息枢纽 — 6 个生产级场景演示")
    print(f"{'=' * 65}")

    # ── 启动 RabbitMQ ──────────────────────────────────────────
    print("\n[准备] 检查 RabbitMQ...")
    result = subprocess.run(
        ["docker", "compose", "ps", "-q"],
        capture_output=True,
        text=True,
    )
    if not result.stdout.strip():
        print("  正在启动 RabbitMQ...")
        subprocess.run(["docker", "compose", "up", "-d"], check=True)
        time.sleep(8)
    else:
        print("  RabbitMQ 已运行")

    # ── 校验拓扑（不创建） ───────────────────────────────────────
    # 生产实践：拓扑是基础设施，应由运维预配（面板 / definitions.json / IaC），
    # 应用代码只 get_exchange/get_queue，不 declare。这里仅 passive 检查存在性，
    # 缺失时打印指引并退出，由人去配。
    print("  校验 RabbitMQ 拓扑（passive，不创建资源）...")
    missing = check_topology()
    if missing:
        print(f"\n  ✗ 拓扑未就绪，缺失 {len(missing)} 项：{', '.join(missing)}")
        print("\n  请先创建拓扑（三选一），完成后重跑：python run_demo.py")
        print("    1) make setup          开发一键（等同 setup_exchanges.py）")
        print("    2) 面板导入 definitions.json")
        print("       http://localhost:15672 → Overview → Import definitions")
        print("    3) 面板手动：照 README「拓扑清单」在 Exchanges/Queues/Bindings 逐个建")
        sys.exit(1)
    print("  拓扑就绪 ✓\n")

    # ── 启动 alert_service ─────────────────────────────────────
    alert_proc = run_bg([sys.executable, "services/alert_service.py"])
    time.sleep(2)

    # 从 topology 获取名称
    ex_fulfillment = EXCHANGE_MAP["order.fulfillment"].name
    ex_compliance = EXCHANGE_MAP["order.compliance"].name
    q_nlp = QUEUE_MAP["nlp_queue"].name

    # ════════════════════════════════════════════════════════════
    # 场景 1: 手动 ACK
    # ════════════════════════════════════════════════════════════
    banner(1, "手动 ACK — 消费者崩溃不丢消息")
    section("问题：消费者处理到一半崩溃，消息是否丢失？")
    section("方案：手动 ACK，处理完才确认。未 ACK 的消息重入队列。\n")
    cleanup_queues()

    async def scenario1() -> None:
        conn = await aio_pika.connect_robust(s.amqp_url)
        async with conn:
            ch = await conn.channel()
            ex = await ch.get_exchange(ex_compliance)
            body = json.dumps(
                {
                    "order_id": "S1-CRASH-001",
                    "customer": "测试用户",
                    "items": [
                        {
                            "name": "崩溃测试商品",
                            "description": "测试",
                            "quantity": 1,
                            "price": "1.00",
                        }
                    ],
                    "has_text": True,
                    "has_image": False,
                }
            ).encode()
            await ex.publish(
                aio_pika.Message(body=body, delivery_mode=aio_pika.DeliveryMode.PERSISTENT),
                routing_key=f"{s.routing_text_prefix}.S1-CRASH-001",
            )
        section("已发送订单 S1-CRASH-001 到 NLP 队列")

        section("启动模拟消费者（收到消息后不 ACK，直接断开）...")
        crash_code = (
            "import asyncio, json, aio_pika\n"
            "async def main():\n"
            "    conn = await aio_pika.connect_robust('amqp://guest:guest@localhost/')\n"
            "    ch = await conn.channel()\n"
            "    await ch.set_qos(prefetch_count=1)\n"
            f"    q = await ch.get_queue('{q_nlp}')\n"
            "    async with q.iterator() as it:\n"
            "        async for msg in it:\n"
            "            body = json.loads(msg.body.decode())\n"
            '            print(f\'  [模拟崩溃] 收到订单 {body["order_id"]}，'
            "处理到一半崩溃！不发送 ACK！')\n"
            "            await conn.close()\n"
            "            return\n"
            "asyncio.run(main())\n"
        )
        crash_proc = subprocess.Popen(
            [sys.executable, "-c", crash_code],
            stdout=sys.stdout,
            stderr=sys.stderr,
        )
        crash_proc.wait()

        section("消费者已崩溃！消息未被 ACK。")
        wait(2, "等待 RabbitMQ 检测到连接断开，消息重入队列")

        section("启动正常的 NLP 消费者...")
        nlp_proc = run_bg([sys.executable, "services/nlp_service.py"])
        wait(3, "等待 NLP 消费者处理重入队列的消息")
        nlp_proc.terminate()

    asyncio.run(scenario1())
    section("结果：消息没有丢失！崩溃后消息重入队列，被新消费者正常处理。✓")

    # ════════════════════════════════════════════════════════════
    # 场景 2: Dead Letter Queue — 延迟重试 + 重试上限
    # ════════════════════════════════════════════════════════════
    banner(2, "Dead Letter Queue — 延迟重试 + 重试上限保护")
    section("问题：消费者反复失败，无限重试拖垮系统。")
    section("方案：失败先延迟重试（retry.exchange + TTL 回流），超过 max_retries 后进 DLQ。\n")
    cleanup_queues()

    section("启动 CV 消费者（100% 失败率 + 延迟重试）...")
    cv_proc = run_bg([sys.executable, "services/cv_service.py", "1.0"])
    wait(2, "等待 CV 消费者就绪")

    section("发送 1 条图片消息到 CV 队列（retry_count=0）...")

    async def scenario2() -> None:
        conn = await aio_pika.connect_robust(s.amqp_url)
        async with conn:
            ch = await conn.channel()
            ex = await ch.get_exchange(ex_compliance)
            body = json.dumps(
                {
                    "order_id": "S2-FAIL-001",
                    "customer": "失败测试",
                    "items": [
                        {
                            "name": "无法处理商品",
                            "description": "注定失败",
                            "quantity": 1,
                            "price": "1.00",
                        }
                    ],
                    "has_text": False,
                    "has_image": True,
                }
            ).encode()
            await ex.publish(
                aio_pika.Message(
                    body=body,
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                    headers={"x-retry-count": 0},
                ),
                routing_key=f"{s.routing_image_prefix}.S2-FAIL-001",
            )

    asyncio.run(scenario2())

    section("观察延迟重试链路：失败 → retry.exchange → 回流 → 再失败 → ... → DLQ")
    wait(
        s.max_retries * s.retry_ttl_ms / 1000 + 6,
        f"等待 {s.max_retries} 次延迟重试（每次 {s.retry_ttl_ms}ms）后进 DLQ",
    )
    cv_proc.terminate()

    wait(2, "等待 alert_service 消费死信")
    section("结果：消息经延迟重试 max_retries 次仍失败，最终进入死信队列。✓")

    # ════════════════════════════════════════════════════════════
    # 场景 3: Competing Consumers
    # ════════════════════════════════════════════════════════════
    banner(3, "Competing Consumers — 多实例负载均衡")
    section("问题：单个库存消费者处理不过来，队列积压。")
    section("方案：启动多个 Worker 实例，RabbitMQ 自动负载均衡。\n")
    cleanup_queues()

    section("启动 2 个库存 Worker 实例...")
    inv1 = run_bg([sys.executable, "services/inventory_service.py", "1"])
    inv2 = run_bg([sys.executable, "services/inventory_service.py", "2"])
    wait(2, "等待 Worker 就绪")

    section("发送 10 笔订单到库存队列...")

    async def scenario3() -> None:
        conn = await aio_pika.connect_robust(s.amqp_url)
        async with conn:
            ch = await conn.channel()
            ex = await ch.get_exchange(ex_fulfillment)
            for i in range(10):
                body = json.dumps(
                    {
                        "order_id": f"S3-LOAD-{i + 1:03d}",
                        "customer": f"客户{i + 1}",
                        "items": [{"name": f"商品{i + 1}", "quantity": 1, "price": "10.00"}],
                    }
                ).encode()
                await ex.publish(
                    aio_pika.Message(body=body, delivery_mode=aio_pika.DeliveryMode.PERSISTENT),
                    routing_key="",
                )

    asyncio.run(scenario3())

    wait(8, "等待两个 Worker 分摊处理 10 条消息")
    inv1.terminate()
    inv2.terminate()
    section("结果：10 条消息被 2 个 Worker 分摊处理，每个约处理 5 条。✓")

    # ════════════════════════════════════════════════════════════
    # 场景 4: Priority Queue
    # ════════════════════════════════════════════════════════════
    banner(4, "Priority Queue — VIP 订单优先处理")
    section("问题：VIP 客户的订单排在普通订单后面，体验差。")
    section("方案：队列启用 x-max-priority，高优先级消息插队处理。\n")
    cleanup_queues()

    section("先发送 5 条普通订单 (priority=1)，让它们在队列中排队...")

    async def scenario4_send() -> None:
        conn = await aio_pika.connect_robust(s.amqp_url)
        async with conn:
            ch = await conn.channel()
            ex = await ch.get_exchange(ex_fulfillment)
            for i in range(5):
                body = json.dumps(
                    {
                        "order_id": f"S4-NORMAL-{i + 1:03d}",
                        "customer": f"普通客户{i + 1}",
                        "items": [{"name": f"普通商品{i + 1}", "quantity": 1, "price": "10.00"}],
                    }
                ).encode()
                await ex.publish(
                    aio_pika.Message(
                        body=body,
                        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                        priority=1,
                    ),
                    routing_key="",
                )
            vip_body = json.dumps(
                {
                    "order_id": "S4-VIP-001",
                    "customer": "VIP 张三",
                    "items": [{"name": "VIP 限量商品", "quantity": 1, "price": "9999.00"}],
                }
            ).encode()
            await ex.publish(
                aio_pika.Message(
                    body=vip_body,
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                    priority=s.max_priority,
                ),
                routing_key="",
            )
        section("再发送 1 条 VIP 订单 (priority=10)")

    asyncio.run(scenario4_send())

    section("现在启动库存消费者...")
    inv_proc = run_bg([sys.executable, "services/inventory_service.py"])
    wait(6, "等待消费者按优先级处理")
    inv_proc.terminate()
    section("结果：VIP 订单(priority=10) 虽然最后发送，但最先被处理！✓")

    # ════════════════════════════════════════════════════════════
    # 场景 5: 幂等消费
    # ════════════════════════════════════════════════════════════
    banner(5, "幂等消费 — 重复消息不重复处理")
    section("问题：网络抖动导致消息重复投递，库存被扣两次。")
    section("方案：消费者用 order_id 去重，重复消息直接 ACK 跳过。\n")
    cleanup_queues()

    section("启动库存消费者（幂等模式）...")
    inv_proc = run_bg([sys.executable, "services/inventory_service.py"])
    wait(2, "等待就绪")

    section("发送同一订单 S5-DUP-001 两次...")

    async def scenario5() -> None:
        conn = await aio_pika.connect_robust(s.amqp_url)
        async with conn:
            ch = await conn.channel()
            ex = await ch.get_exchange(ex_fulfillment)
            body = json.dumps(
                {
                    "order_id": "S5-DUP-001",
                    "customer": "重复测试",
                    "items": [{"name": "去重测试商品", "quantity": 2, "price": "100.00"}],
                }
            ).encode()
            for i in range(2):
                await ex.publish(
                    aio_pika.Message(body=body, delivery_mode=aio_pika.DeliveryMode.PERSISTENT),
                    routing_key="",
                )
                section(f"  第 {i + 1} 次发送 S5-DUP-001")

    asyncio.run(scenario5())

    wait(3, "等待消费者处理")
    inv_proc.terminate()
    section("结果：第 1 次正常处理并扣库存，第 2 次识别为重复，直接跳过。✓")

    # ════════════════════════════════════════════════════════════
    # 场景 6: Message TTL
    # ════════════════════════════════════════════════════════════
    banner(6, "Message TTL — 超时自动过期")
    section("问题：海关服务宕机，消息在队列中无限等待。")
    section("方案：队列设置 x-message-ttl，超时消息自动进入 DLX。\n")
    cleanup_queues()

    section("不启动海关消费者（模拟服务宕机）...")
    section("发送订单到海关队列（TTL=8s）...")

    async def scenario6() -> None:
        conn = await aio_pika.connect_robust(s.amqp_url)
        async with conn:
            ch = await conn.channel()
            ex = await ch.get_exchange(ex_fulfillment)
            body = json.dumps(
                {
                    "order_id": "S6-TTL-001",
                    "customer": "超时测试",
                    "items": [{"name": "过期测试商品", "quantity": 1, "price": "1.00"}],
                }
            ).encode()
            await ex.publish(
                aio_pika.Message(body=body, delivery_mode=aio_pika.DeliveryMode.PERSISTENT),
                routing_key="",
            )

    asyncio.run(scenario6())

    section("订单 S6-TTL-001 已发送到 customs_queue (TTL=8s)")
    section("海关服务未启动，消息在队列中等待...")
    wait(10, "等待消息 TTL 过期（8s），自动进入 DLX")
    wait(2, "等待 alert_service 消费死信")
    section("结果：消息在 8s 后自动过期，进入死信队列，alert_service 发出告警。✓")

    # ── 清理 ────────────────────────────────────────────────────
    print(f"\n{'=' * 65}")
    print("  全部 6 个场景演示完成")
    print(f"{'=' * 65}\n")
    print("  总结：")
    print("    场景 1: 手动 ACK     → 崩溃不丢消息，重入队列重新消费")
    print("    场景 2: DLX 死信      → 重试上限保护，失败消息集中告警")
    print("    场景 3: Competing     → 多 Worker 分摊压力，水平扩展")
    print("    场景 4: Priority      → VIP 订单插队，高优先级先处理")
    print("    场景 5: 幂等消费      → 重复消息去重，防止重复扣库存")
    print("    场景 6: Message TTL   → 超时自动过期，避免消息无限堆积")
    print()
    print("  管理面板: http://localhost:15672 (guest/guest)")
    print()

    alert_proc.terminate()


if __name__ == "__main__":
    main()
