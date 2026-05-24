"""NLP 合规审查服务 — Topic 消费者 (routing key: compliance.text.*)

启动方式：python nlp_service.py
"""

import asyncio

from config import QUEUE_NLP, SIMULATE_NLP
from consumers import COLOR_BLUE, log, run_consumer


def process(body: dict):
    items = body.get("items", [])
    texts = [
        f"{item.get('name')}: {item.get('description', '')[:30]}..."
        for item in items
        if item.get("description")
    ]
    log("NLP", COLOR_BLUE, f"  文本合规检查: {len(texts)} 条描述 — 全部通过")


async def main():
    await run_consumer(
        queue_name=QUEUE_NLP,
        tag="NLP",
        color=COLOR_BLUE,
        simulate_seconds=SIMULATE_NLP,
        process_fn=process,
    )


if __name__ == "__main__":
    asyncio.run(main())
