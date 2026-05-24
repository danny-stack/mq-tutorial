"""CV 合规审查服务 — Topic 消费者 (routing key: compliance.image.*)

启动：python cv_service.py
"""

import asyncio

from config import settings
from consumers import COLOR_MAGENTA, run_consumer, setup_logging
from topology import QUEUE_MAP


async def process(body: dict) -> None:
    items = body.get("items", [])
    images = [item.get("image_url") for item in items if item.get("image_url")]
    print(f"    图片合规检查: {len(images)} 张 — 全部通过", flush=True)


async def main() -> None:
    setup_logging()
    await run_consumer(
        queue_name=QUEUE_MAP["cv_queue"].name,
        tag="CV",
        color=COLOR_MAGENTA,
        process_fn=process,
        simulate_seconds=settings.simulate_cv,
        idempotent=True,
    )


if __name__ == "__main__":
    asyncio.run(main())
