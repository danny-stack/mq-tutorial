"""库存服务 — Fanout 消费者（Priority + Competing Consumers）

启动：python inventory_service.py [worker_id]
"""

import asyncio
import sys

from config import settings
from consumers import COLOR_GREEN, run_consumer, setup_logging


async def process(body: dict) -> None:
    items = body.get("items", [])
    total = sum(item.get("quantity", 0) for item in items)
    print(f"    已扣减 {total} 件商品库存", flush=True)


async def main() -> None:
    setup_logging()
    worker_id = sys.argv[1] if len(sys.argv) > 1 else ""
    await run_consumer(
        queue_name=settings.queue_inventory,
        tag="库存",
        color=COLOR_GREEN,
        process_fn=process,
        simulate_seconds=settings.simulate_inventory,
        idempotent=True,
        worker_id=worker_id,
    )


if __name__ == "__main__":
    asyncio.run(main())
