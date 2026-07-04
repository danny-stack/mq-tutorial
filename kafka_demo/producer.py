"""生产者：模拟上游 DSS/MySQL 往 competitors.stg 推竞品数据

跑：python -m kafka_demo.producer [条数]
默认发 5 条。消息体是 JSON，key 用 asin（同 asin 落同一分区，保证顺序，
upsert 去重需要这个语义）。
"""

from __future__ import annotations

import asyncio
import json
import random
import sys

from aiokafka import AIOKafkaProducer

from kafka_demo.common import BOOTSTRAP_SERVERS, TOPIC, logger, setup_logging


def make_competitor(idx: int) -> dict:
    """造一条合理的竞品数据（competitors_stg 的关键字段，非全 33 列）。"""
    asin = f"B0{random.randint(10**7, 10**8 - 1)}"  # 10 字符，形如 Amazon ASIN
    return {
        "asin": asin,
        "parent_asin": asin,
        "category_node_id": "PPMN20250930000001",  # 同步过滤键，必填
        "category_tag": random.choice(["electronics", "home", "toys"]),
        "channel": "amazon",
        "category_tree": {"l1": "Electronics", "l2": "Audio"},  # stg 是 TEXT，存 JSON 文本
        "features": ["wireless", "bluetooth 5.3", "noise cancelling"],
        "brand": "Acme",
        "manufacturer": "Acme Corp",
        "title": f"Demo Product {idx}",
        "price": f"{round(random.uniform(9.9, 199.9), 2):.2f}",
        "asin_cost": f"{round(random.uniform(3, 80), 2):.2f}",
        "package_weight": f"{round(random.uniform(0.1, 5.0), 2)}",
        "package_length": f"{round(random.uniform(5, 50), 1)}",
        "package_width": f"{round(random.uniform(5, 50), 1)}",
        "package_height": f"{round(random.uniform(1, 20), 1)}",
        "node_id": random.randint(1, 99999),
        "star_rating": f"{round(random.uniform(3.5, 5.0), 1)}",
        "review_count": random.randint(0, 5000),
        "has_reviews": True,
        # 上游来源时间戳（正式表去重排序用，必须真实）
        "source_created_at": "2026-07-04T10:00:00Z",
        "source_updated_at": "2026-07-04T10:00:00Z",
    }


async def main(n: int) -> None:
    producer = AIOKafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    )
    await producer.start()
    try:
        for i in range(n):
            row = make_competitor(i + 1)
            # key=asin：同 asin 消息落同一分区，保证分区级顺序
            await producer.send_and_wait(TOPIC, value=row, key=row["asin"].encode("utf-8"))
            logger.info("↑ 发送 [%d/%d] asin=%s → %s", i + 1, n, row["asin"], TOPIC)
            await asyncio.sleep(0.2)
        logger.info("全部 %d 条已发送，topic=%s", n, TOPIC)
    finally:
        await producer.stop()


if __name__ == "__main__":
    setup_logging()
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 5
    asyncio.run(main(n))
