"""库存服务 — Fanout 消费者

启动方式：python inventory_service.py
"""

import asyncio

from config import QUEUE_INVENTORY, SIMULATE_INVENTORY
from consumers import COLOR_GREEN, log, run_consumer


def process(body: dict):
    items = body.get("items", [])
    total = sum(item.get("quantity", 0) for item in items)
    log("库存", COLOR_GREEN, f"  已扣减 {total} 件商品库存")


async def main():
    await run_consumer(
        queue_name=QUEUE_INVENTORY,
        tag="库存",
        color=COLOR_GREEN,
        simulate_seconds=SIMULATE_INVENTORY,
        process_fn=process,
    )


if __name__ == "__main__":
    asyncio.run(main())
