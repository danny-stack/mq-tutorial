"""Kafka 学习 demo 共享配置 + 数据工厂

模拟 ai-x-backbone 的 competitors 上游同步：
  原本有外部 DSS/MySQL ETL 往 PostgreSQL 的 competitors_stg 表推数据，
  未来要改成自己从 Kafka 消费来填 stg。本 demo 用本地 Kafka 跑通
  生产者(模拟上游) → topic → 消费者(组装 stg 行) 的闭环，不接真库。
"""

from __future__ import annotations

import logging

# 本地 Kafka（docker-compose.kafka.yml 起的，宿主机走 HOST listener 29092）
BOOTSTRAP_SERVERS = "localhost:29092"
# topic 命名对齐团队约定（参照 pp.maps）：<来源系统>.<实体>，全小写点分隔。
# 注意别和 PG 缓冲表 competitors_stg（下划线）搞混：topic 是数据流，表是落库终点。
TOPIC = "dss.competitors"  # 模拟上游 DSS 系统推竞品数据的同步流
CONSUMER_GROUP = "competitors-stg-loader"

# competitors_stg 的 33 列（与 ai-x-backbone processing.py 的 STG_COLUMNS 对齐）
STG_COLUMNS = [
    "asin",
    "parent_asin",
    "category_tag",
    "channel",
    "category_tree",
    "manufacturer",
    "brand",
    "model",
    "color",
    "size",
    "title",
    "features",
    "description",
    "package_height",
    "package_length",
    "package_width",
    "package_weight",
    "pick_and_pack_fee",
    "price",
    "price_date",
    "star_rating",
    "has_reviews",
    "review_count",
    "created_by",
    "created_at",
    "sales_rank_drops30",
    "images_csv",
    "source_created_at",
    "source_updated_at",
    "cost_status",
    "asin_cost",
    "node_id",
    "category_node_id",
]

logger = logging.getLogger("kafka-demo")


def setup_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
    # 压掉 aiokafka 的 group coordinator / rebalance 等 info 噪音；想看就调成 INFO
    logging.getLogger("aiokafka").setLevel(logging.WARNING)
