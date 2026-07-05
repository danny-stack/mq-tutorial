"""通知服务 — Direct 消费者（routing key: order.paid）

启动：python services/notification_service.py
"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from mq.consumer import COLOR_MAGENTA, run_consumer, setup_logging
from topology import QUEUE_MAP


async def process(body: dict) -> None:
    order_id = body.get("order_id", "unknown")
    customer = body.get("customer", "匿名用户")
    print(f"    发送支付成功通知 | 订单 {order_id} | 用户 {customer}", flush=True)


async def main() -> None:
    setup_logging()
    await run_consumer(
        queue_name=QUEUE_MAP["notification_queue"].name,
        tag="通知",
        color=COLOR_MAGENTA,
        process_fn=process,
        simulate_seconds=0.3,
        idempotent=True,
    )


if __name__ == "__main__":
    asyncio.run(main())
