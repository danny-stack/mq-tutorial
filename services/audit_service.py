"""审计服务 — Direct 消费者（routing key: order.paid）

启动：python services/audit_service.py
"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from mq.consumer import COLOR_CYAN, run_consumer, setup_logging
from topology import QUEUE_MAP


async def process(body: dict) -> None:
    order_id = body.get("order_id", "unknown")
    total = body.get("total", "0.00")
    print(f"    记录审计日志 | 订单 {order_id} | 金额 {total}", flush=True)


async def main() -> None:
    setup_logging()
    await run_consumer(
        queue_name=QUEUE_MAP["audit_queue"].name,
        tag="审计",
        color=COLOR_CYAN,
        process_fn=process,
        simulate_seconds=0.3,
        idempotent=True,
    )


if __name__ == "__main__":
    asyncio.run(main())
