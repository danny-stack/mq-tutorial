"""海关服务 — Fanout 消费者

启动方式：python customs_service.py
"""

import asyncio

from config import QUEUE_CUSTOMS, SIMULATE_CUSTOMS
from consumers import COLOR_YELLOW, log, run_consumer


def process(body: dict):
    customer = body.get("customer", "未知")
    items = body.get("items", [])
    item_names = ", ".join(item.get("name", "") for item in items)
    log("海关", COLOR_YELLOW, f"  申报人: {customer}, 商品: {item_names}")


async def main():
    await run_consumer(
        queue_name=QUEUE_CUSTOMS,
        tag="海关",
        color=COLOR_YELLOW,
        simulate_seconds=SIMULATE_CUSTOMS,
        process_fn=process,
    )


if __name__ == "__main__":
    asyncio.run(main())
