"""NLP 合规审查服务 — Topic 消费者 (routing key: compliance.text.*)

启动：python nlp_service.py
"""

import asyncio

from config import settings
from consumers import COLOR_BLUE, run_consumer, setup_logging


async def process(body: dict) -> None:
    items = body.get("items", [])
    texts = [
        f"{item.get('name')}: {item.get('description', '')[:30]}..."
        for item in items
        if item.get("description")
    ]
    print(f"    文本合规检查: {len(texts)} 条描述 — 全部通过", flush=True)


async def main() -> None:
    setup_logging()
    await run_consumer(
        queue_name=settings.queue_nlp,
        tag="NLP",
        color=COLOR_BLUE,
        process_fn=process,
        simulate_seconds=settings.simulate_nlp,
        idempotent=True,
    )


if __name__ == "__main__":
    asyncio.run(main())
