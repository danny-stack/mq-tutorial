"""CV 合规审查服务 — Topic 消费者 (routing key: compliance.image.*)

启动：
  python services/cv_service.py        # 正常消费
  python services/cv_service.py 1.0    # 100% 失败，演示延迟重试 → DLQ
"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import settings
from mq.consumer import COLOR_MAGENTA, run_consumer, setup_logging
from topology import QUEUE_MAP


async def process(body: dict) -> None:
    items = body.get("items", [])
    images = [item.get("image_url") for item in items if item.get("image_url")]
    print(f"    图片合规检查: {len(images)} 张 — 全部通过", flush=True)


async def main() -> None:
    setup_logging()
    # 命令行注入失败率（演示延迟重试）：python services/cv_service.py 1.0
    failure_rate = float(sys.argv[1]) if len(sys.argv) > 1 else 0.0
    await run_consumer(
        queue_name=QUEUE_MAP["cv_queue"].name,
        tag="CV",
        color=COLOR_MAGENTA,
        process_fn=process,
        simulate_seconds=settings.simulate_cv,
        failure_rate=failure_rate,
        idempotent=True,
        retry_exchange_name="retry.exchange",
    )


if __name__ == "__main__":
    asyncio.run(main())
