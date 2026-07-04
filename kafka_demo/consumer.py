"""消费者：从 competitors.stg 消费 → 组装 competitors_stg 行 → 模拟 INSERT

跑：
  python -m kafka_demo.consumer              # 持续消费
  python -m kafka_demo.consumer --max 3      # 消费 3 条后停（验证用）

设计要点（和生产消费者一致）：
  - enable_auto_commit=False：处理完一条才手动 commit，挂掉时不丢偏移量语义
  - auto_offset_reset=earliest：新消费组从头消费（学习时 producer 先发、consumer 后起也能消费到）
  - to_stg_row：把消息补齐成 33 列的完整 stg 行（审计列、JSON 序列化）
"""

from __future__ import annotations

import argparse
import asyncio
import json
from datetime import UTC, datetime

from aiokafka import AIOKafkaConsumer

from kafka_demo.common import (
    BOOTSTRAP_SERVERS,
    CONSUMER_GROUP,
    STG_COLUMNS,
    TOPIC,
    logger,
    setup_logging,
)


def to_stg_row(msg_value: dict) -> dict:
    """把消息组装成 competitors_stg 的完整行（补审计列、规整 JSON 列）。"""
    # 只取 STG_COLUMNS 定义的列，丢弃消息里多余的字段
    row = {col: msg_value.get(col) for col in STG_COLUMNS}
    # 必填审计列（正式表 NOT NULL）：消费入库时补上
    if not row.get("created_by"):
        row["created_by"] = "kafka-consumer"
    if not row.get("created_at"):
        row["created_at"] = datetime.now(UTC).isoformat()
    # category_tree / features：stg 是 TEXT 列，dict/list 要序列化成 JSON 文本
    for col in ("category_tree", "features"):
        v = row.get(col)
        if isinstance(v, dict | list):
            row[col] = json.dumps(v, ensure_ascii=False)
    return row


def _sql_value(v: object) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, int | float):
        return str(v)
    return "'" + str(v).replace("'", "''") + "'"


def render_insert(row: dict) -> str:
    """渲染一条模拟 INSERT（不真执行，只打印证明 33 列组装正确）。"""
    cols = ", ".join(row.keys())
    vals = ", ".join(_sql_value(v) for v in row.values())
    return f"INSERT INTO competitors_stg ({cols})\n  VALUES ({vals});"


async def main(max_messages: int | None = None) -> None:
    consumer = AIOKafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    )
    await consumer.start()
    logger.info("消费者就绪，监听 %s（group=%s）...", TOPIC, CONSUMER_GROUP)
    count = 0
    try:
        async for msg in consumer:
            row = to_stg_row(msg.value)
            logger.info(
                "↓ 收到 asin=%s (partition=%d offset=%d)，组装 stg 行 %d 列",
                row["asin"],
                msg.partition,
                msg.offset,
                len(row),
            )
            print(render_insert(row), flush=True)
            await consumer.commit()  # 处理完才提交偏移量
            count += 1
            if max_messages and count >= max_messages:
                logger.info("已达 --max %d，停止消费", max_messages)
                break
    finally:
        await consumer.stop()
        logger.info("消费者已停止，共处理 %d 条", count)


if __name__ == "__main__":
    setup_logging()
    parser = argparse.ArgumentParser(description="competitors.stg 消费者（模拟落 stg）")
    parser.add_argument("--max", type=int, default=None, help="消费 N 条后停（默认持续消费）")
    args = parser.parse_args()
    asyncio.run(main(args.max))
