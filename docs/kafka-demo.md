# Kafka Demo：competitors 上游同步

模拟 ai-x-backbone 的 competitors 数据链路改造：

- **现状**：外部 DSS/MySQL ETL 直接往 PostgreSQL 的 `competitors_stg` 缓冲表推数据
- **目标**：上游改成写 Kafka，我们这边起消费者消费，再组装/写入 `competitors_stg`

本 demo 用本地 Kafka 跑通「生产者(模拟上游) → topic → 消费者(组装 stg 行)」的闭环，
**不接真库**——consumer 只打印组装好的 `INSERT` 语句，验证 33 列组装逻辑正确。

## 命名约定（重要，别搞混）

| 名字 | 是什么 | 说明 |
|---|---|---|
| `dss.competitors` | **Kafka topic** | 上游同步流。命名对齐团队约定（参照 `pp.maps`）：`<来源系统>.<实体>`，全小写、点分隔 |
| `competitors_stg` | **PostgreSQL 表** |  competitors 缓冲表（stg = staging），33 列，是数据流的**落库终点** |

topic 和表是两个东西：topic 是传输中的数据流，表是消费后的归宿。
命名上 topic 用点、表用下划线，看到 `competitors.stg` 这种点号写法一律是 topic。

## 组件

`docker-compose.kafka.yml`（独立于 RabbitMQ 教程的 `docker-compose.yml`）：

- **kafka**：apache/kafka，KRaft 模式（无 ZooKeeper），单容器 broker+controller
  - 容器间 listener：`kafka:9092`（PLAINTEXT）
  - 宿主机 listener：`localhost:29092`（HOST）——**本地 Python 连这个**
  - heap 限到 512M，可和 RabbitMQ 共存
- **kafka-ui**：http://localhost:8080 ，看 topic / 消息 / 消费组 lag

## 启动

```bash
# 1. 起 Kafka（一次即可）
docker compose -f docker-compose.kafka.yml up -d

# 2. 装依赖（aiokafka 在 kafka extra 里）
conda activate async-mq          # 本项目依赖装在这个 conda 环境
pip install -e ".[kafka]"

# 3. 终端 A：起消费者（我们自己这边）
python -m kafka_demo.consumer            # 持续消费
python -m kafka_demo.consumer --max 5    # 或消费 5 条后自动停

# 4. 终端 B：起生产者（模拟上游，发完即退出）
python -m kafka_demo.producer 5          # 发 5 条
```

停止：`docker compose -f docker-compose.kafka.yml down`（加 `-v` 清数据）。

## 消息语义

- **topic**：`dss.competitors`（首次发送时 broker 自动创建，单分区）
- **key = asin**：同 asin 的消息落同一分区，保证分区级顺序——stg 按 asin upsert 去重依赖这个语义
- **消费组**：`competitors-stg-loader`
- **手动 commit**：`enable_auto_commit=False`，处理完一条才提交偏移量，消费者挂掉不漏不重
- **`auto_offset_reset=earliest`**：新消费组从头读，先发生产、后启消费也能拿到（学习场景方便）

## 消费侧技术细节

### 连接与订阅（consumer.py）

- `bootstrap_servers="localhost:29092"` 只是初始入口，连上后 client 通过 metadata 协议拿到全集群 broker 列表
- `group_id="competitors-stg-loader"`：组内多实例自动瓜分分区；组与组之间各自维护 offset，互不影响
- `value_deserializer` 只反序列化 value（bytes → dict）；key 拿到的是原始 bytes，消费端不读——生产端 key=asin 只用于分区路由

### Offset 语义：at-least-once

`auto_offset_reset="earliest"` 只在**消费组首次启动、无已提交 offset** 时生效；一旦提交过，
重启后从上次提交点继续，该参数不再起作用。

`enable_auto_commit=False` + 每条处理后 `await consumer.commit()`，即：

```
收到消息 → 处理（组装行/打印/未来的写库） → 提交 offset
```

- 处理到一半挂 → offset 未提交 → 重启后**重新投递**，不丢
- 处理完但提交前挂 → 重启后**重复投递** → 下游必须幂等（对 stg 表即按 asin upsert，而非裸 INSERT）

注意：`commit()` 提交的是**当前消费位置**（所有分区的 position 水位线），不是逐条确认。
不能跳过某条坏消息单独确认它后面的消息——这点和 RabbitMQ 的手动 ACK 语义不同。

### 分区与顺序

- 当前 topic 自动创建为**单分区**，全局有序
- 生产若多分区：key=asin 哈希到同一分区 → **分区级有序**；跨分区不保证顺序，但 upsert 只需同 key 有序
- 并行度上限 = 分区数：单分区时消费组里多于 1 个实例没有用

### 消息组装 `to_stg_row()`

纯函数、无副作用，三步：

1. **白名单裁剪**：只取 `STG_COLUMNS` 的 33 列，消息里多带的字段直接丢弃——上游加字段不会搞崩下游
2. **补审计列**：`created_by`/`created_at` 缺省补上（正式表 NOT NULL，入库责任在消费侧）
3. **JSON 规整**：`category_tree`/`features` 在消息里是 dict/list，stg 是 TEXT 列，序列化成 JSON 字符串

缺省列填 `None`（SQL NULL）。将来真写库时这部分不用改。

### 关停与异常

- `try/finally` 里 `consumer.stop()`：主动退出时先离组、释放分区；否则 broker 要等
  `session.timeout.ms`（默认 45s）才发现消费者死了、触发 rebalance
- 目前**没有**错误处理：消息体非法 JSON 会让消费者整个崩掉，重启后在同一条上再崩（poison message）。
  生产可参考 `mq/error_policy.py` 的思路：TRANSIENT 重试 / PERMANENT 发到 DLQ topic
  （如 `dss.competitors.dlq`），记下 offset 后继续

### 和 RabbitMQ 教程的对照

| | RabbitMQ | Kafka |
|---|---|---|
| 确认粒度 | 逐条 ACK | offset 水位线，提交即之前全收 |
| 重投 | reject/nack 重入队 | 不提交 offset，重启后重读 |
| 消息存留 | ACK 后删除 | 提交后也保留（按 retention），别的组还能读 |
| 坏消息 | DLX 死信队列 | 需自建 DLQ topic |

### 演进方向：真写 PG

关键改动两个：

1. consumer 把 `print(render_insert(row))` 换成 asyncpg 的
   `INSERT ... ON CONFLICT (asin) DO UPDATE`——upsert 天然幂等，兜住 at-least-once 的重复投递
2. 加 poison message 的 DLQ 处理

## 代码结构

```
kafka_demo/
├── common.py     # BOOTSTRAP_SERVERS / TOPIC / 消费组 / STG_COLUMNS(33列)
├── producer.py   # 造竞品数据发 Kafka，发完即结束
└── consumer.py   # 消费 → to_stg_row() 组装 33 列 → 打印 INSERT（不落库）
```

`STG_COLUMNS` 与 ai-x-backbone `processing.py` 的 stg 表结构对齐；
`to_stg_row()` 负责补审计列（`created_by`/`created_at`）、把 `category_tree`/`features`
序列化成 JSON 文本（stg 里这两列是 TEXT）。

## 排查

```bash
# 列 topic
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

# 看消费组 lag
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group competitors-stg-loader

# 从头 dump 消息
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic dss.competitors --from-beginning
```
