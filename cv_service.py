"""CV 合规审查服务 — Topic 消费者 (routing key: compliance.image.*)

启动方式：python cv_service.py
"""

import asyncio

from config import QUEUE_CV, SIMULATE_CV
from consumers import COLOR_MAGENTA, log, run_consumer


def process(body: dict):
    items = body.get("items", [])
    images = [item.get("image_url") for item in items if item.get("image_url")]
    log("CV", COLOR_MAGENTA, f"  图片合规检查: {len(images)} 张 — 全部通过")


async def main():
    await run_consumer(
        queue_name=QUEUE_CV,
        tag="CV",
        color=COLOR_MAGENTA,
        simulate_seconds=SIMULATE_CV,
        process_fn=process,
    )


if __name__ == "__main__":
    asyncio.run(main())
