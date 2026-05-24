"""海关服务 — Fanout 消费者（TTL 演示）

启动：python services/customs_service.py
"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import settings
from mq.consumer import COLOR_YELLOW, run_consumer, setup_logging
from topology import QUEUE_MAP


async def process(body: dict) -> None:
    customer = body.get("customer", "未知")
    items = body.get("items", [])
    item_names = ", ".join(item.get("name", "") for item in items)
    print(f"    申报人: {customer}, 商品: {item_names}", flush=True)


async def main() -> None:
    setup_logging()
    await run_consumer(
        queue_name=QUEUE_MAP["customs_queue"].name,
        tag="海关",
        color=COLOR_YELLOW,
        process_fn=process,
        simulate_seconds=settings.simulate_customs,
        idempotent=True,
    )


if __name__ == "__main__":
    asyncio.run(main())
