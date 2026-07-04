"""NLP 合规审查服务 — Topic 消费者 (routing key: compliance.text.*)

启动：python services/nlp_service.py
"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import settings
from mq.consumer import COLOR_BLUE, run_consumer, setup_logging
from topology import QUEUE_MAP


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
        queue_name=QUEUE_MAP["nlp_queue"].name,
        tag="NLP",
        color=COLOR_BLUE,
        process_fn=process,
        simulate_seconds=settings.simulate_nlp,
        idempotent=True,
        retry_exchange_name="retry.exchange",
    )


if __name__ == "__main__":
    asyncio.run(main())
