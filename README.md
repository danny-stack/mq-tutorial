# 跨境电商异步消息枢纽 — RabbitMQ Tutorial

> 用 **Fanout 广播** + **Topic 精准路由**，彻底解决海关接口慢、AI 推理耗时长导致电商系统卡死的问题。

## 场景

跨境电商用户支付订单后，系统需要同时触发：

| 下游服务 | 处理内容 | 耗时 |
|---------|---------|------|
| 库存服务 | 扣减库存 | ~0.5s |
| 海关服务 | 跨境申报 | ~3s |
| NLP 服务 | 商品描述合规审查 | ~2s |
| CV 服务 | 商品图片合规审查 | ~4s |

**问题**：如果同步调用，用户要等 `0.5 + 3 + 2 + 4 = 9.5s` 才能看到"支付成功"。

**解决**：通过 RabbitMQ 异步解耦，支付接口 <100ms 返回，4 个服务并行处理，总耗时 = 最慢的那个（4s）。

## 架构

```
                    ┌─ inventory_queue ──── [Worker 1] ──┐  ← Competing Consumers
order.fulfillment ──┤                              [Worker 2] ──┤  (多实例负载均衡)
   [Fanout]         └─ customs_queue (TTL=8s) ──── [Worker]
                                           过期 → DLX

                    ┌─ nlp_queue ───────── [Worker]
order.compliance ───┤
   [Topic]          └─ cv_queue ────────── [Worker]
                        失败 → retry → 重试3次 → DLX

order.status ───────┬─ notification_queue ─ [通知服务]    ← Direct 精确路由
   [Direct]         └─ audit_queue ──────── [审计服务]    ← key=order.paid

dead_letter_queue ────── [告警服务]  ← 所有失败/过期的消息最终汇聚于此
```

## 快速开始

### 前置条件

- Docker + Docker Compose
- Python 3.10+

### 1. 安装依赖

```bash
make install  # 或 pip install -e ".[dev]"
```

### 2. 启动 RabbitMQ

```bash
docker compose up -d
```

### 3. 创建拓扑（三选一，必做）

> **生产实践**：拓扑（Exchange/Queue/Binding）是基础设施，应由运维预配，**应用代码只 `get` 不 `declare`**——生产环境应用进程通常没有 declare 权限。`run_demo.py` 启动时只做被动校验，**不会自动创建**。

- **① 开发一键**（本地/教学，等同 `setup_exchanges.py`）：
  ```bash
  make setup
  ```
- **② 面板导入**（推荐，最省事）：浏览器打开 http://localhost:15672（guest/guest）→ 下方 `Import definitions` → 选项目根的 [`definitions.json`](./definitions.json) → 导入，全部资源一键就绪。
- **③ 面板手动**：照下方「[拓扑清单](#拓扑清单手动配置参考)」在 `Exchanges` / `Queues` / `Bindings` 页签逐个建。

### 4. 一键演示（7 个生产级场景）

```bash
python run_demo.py
```

> 若第 3 步没做，`run_demo.py` 会检测到拓扑缺失，打印缺失项并退出，提示你先配。

### 5. 管理面板

浏览器打开 http://localhost:15672（guest/guest），可以看到 Queues 页面中的队列和消息流量。

---

## 7 个生产级场景详解

### 场景 1: 手动 ACK — 消费者崩溃不丢消息

**问题**：消费者处理到一半崩溃，消息是否丢失？

**方案**：手动 ACK（Manual Acknowledgement）。消费者处理完业务逻辑后才发送 ACK 确认。如果消费者在 ACK 之前崩溃（断开连接），RabbitMQ 检测到连接断开后会把消息重新放回队列，由其他消费者重新消费。

```
消费者收到消息 → 处理中... → 💥崩溃（未ACK）
    → RabbitMQ 检测到连接断开
    → 消息重入队列
    → 新消费者收到同一条消息，重新处理
```

### 场景 2: Dead Letter Queue — 延迟重试 + 重试上限保护

**问题**：消费者反复失败，消息无限重试拖垮系统。

**方案**：失败的消息先**延迟重试**——publish 到 `retry.exchange`（fanout），消息进入 `retry_queue` 时保留原 routing_key，TTL（默认 5s）过期后死信回流到 `order.compliance`（topic），按原 routing_key 精确回到 cv/nlp 队列再次消费。重试计数器（`x-retry-count`）递增，超过 `max_retries`（默认 3）后 `reject(requeue=False)` 进入死信队列，由告警服务处理。

```
失败 → retry_count < 3? → publish 到 retry.exchange（retry_count+1，保留原 rk）
                         → retry_queue 等 TTL → 死信回流 order.compliance → 再次消费
       retry_count >= 3? → reject(requeue=False) → DLX → 告警服务
```

> 注：延迟重试仅对 topic exchange（order.compliance）上的 cv/nlp 队列生效；fanout exchange（order.fulfillment）的 inventory/customs 失败直接进 DLQ。

### 场景 3: Competing Consumers — 多实例负载均衡

**问题**：单个消费者处理不过来，队列积压。

**方案**：启动同一队列的多个消费者实例。RabbitMQ 的 `prefetch_count=1` 确保每条消息只投递给一个消费者，实现自动负载均衡。水平扩展只需多跑几个 Worker 进程。

```
队列中有 10 条消息
    → Worker 1 消费 5 条
    → Worker 2 消费 5 条
    → 总耗时减半
```

### 场景 4: Priority Queue — VIP 订单优先处理

**问题**：VIP 客户的订单排在普通订单后面，体验差。

**方案**：队列声明时设置 `x-max-priority`，发布消息时指定 `priority` 字段。RabbitMQ 优先投递高优先级消息，即使它后进队列。

```
队列中：[普通(p=1), 普通(p=1), 普通(p=1), ..., VIP(p=10)]
消费者取消息时：先取 VIP(p=10)，再按顺序取普通
```

### 场景 5: 幂等消费 — 重复消息不重复处理

**问题**：网络抖动导致消息重复投递，库存被扣两次。

**方案**：消费者维护已处理的 `order_id` 集合（生产环境用 Redis/DB），收到消息后先检查是否已处理。如果已处理，直接 ACK 跳过，不执行业务逻辑。

```
同一 order_id 的消息发送 2 次
    → 第 1 次：正常处理，order_id 加入已处理集合
    → 第 2 次：检测到重复，直接 ACK，不扣库存
```

### 场景 6: Message TTL — 超时自动过期

**问题**：海关服务宕机，消息在队列中无限等待。

**方案**：队列声明时设置 `x-message-ttl`（毫秒）。消息在队列中等待超过 TTL 后，自动被移除并转发到 DLX，由告警服务通知运维。

```
海关队列 TTL=8s
    → 消息入队
    → 8s 内无消费者 → 消息过期
    → 转发到 DLX → 告警服务发出通知
```

### 场景 7: Direct Exchange — 精确路由

**问题**：支付成功后，只想让"通知服务"和"审计服务"收到消息，而不是广播给所有下游。

**方案**：direct exchange + 精确 routing_key。生产者发送 `routing_key=order.paid` 的消息到 `order.status`，只有 binding key 完全匹配的队列（`notification_queue`、`audit_queue`）能收到。

```
支付成功 → order.status(direct) ──┬─ key=order.paid → notification_queue → 发送通知
                                  └─ key=order.paid → audit_queue        → 记录审计日志
```

> 与 fanout 的区别：fanout 是"所有绑定队列都收"；direct 是"只有 binding key 完全匹配的队列才收"。同一个 routing key 可以绑定多个队列，实现"精确的一对多"。

---

## 分步教程

如果想手动观察每一步：

```bash
# 终端 0: 创建拓扑（也可面板导入 definitions.json，见「拓扑清单」）
python setup_exchanges.py   # 或 make setup

# 终端 1-8: 启动消费者
python services/inventory_service.py          # 库存 (Fanout, Priority)
python services/inventory_service.py 2        # 库存 Worker 2 (Competing Consumers)
python services/customs_service.py            # 海关 (Fanout, TTL)
python services/nlp_service.py                # NLP (Topic)
python services/cv_service.py                 # CV (Topic, 可随机失败)
python services/notification_service.py       # 通知 (Direct)
python services/audit_service.py              # 审计 (Direct)
python services/alert_service.py              # 死信告警

# 终端 9: 启动 FastAPI
uvicorn services.payment_service:app --port 8000

# 终端 10: 触发支付
curl -X POST http://localhost:8000/orders/CN-20260524-001/pay | python -m json.tool
```

### API 参数

```bash
# 普通订单
curl -X POST http://localhost:8000/orders/CN-20260524-001/pay

# VIP 订单 (priority=10)
curl -X POST "http://localhost:8000/orders/CN-20260524-001/pay?priority=10"

# 批量发送 10 笔订单
curl -X POST "http://localhost:8000/orders/batch?count=10"
```

---

## 拓扑清单（手动配置参考）

> 用面板「方式 ③」手动配时，照此表逐个建。所有 Exchange/Queue 均为 `durable=true`，vhost `/`。
> 也可直接导入 [`definitions.json`](./definitions.json)（已含下表全部内容，最省事）。

### Exchanges

| name | type | 说明 |
|------|------|------|
| `order.fulfillment` | fanout | 订单履约广播（库存 + 海关） |
| `order.compliance` | topic | 合规审查路由（NLP + CV，按内容类型分发） |
| `order.status` | direct | 订单状态变更（例如 `order.paid` 精确路由到通知/审计） |
| `dead_letter_exchange` | fanout | 死信汇聚 |
| `retry.exchange` | fanout | 重试缓冲（TTL 后死信回流 `order.compliance`） |

### Queues

| name | arguments | 说明 |
|------|-----------|------|
| `inventory_queue` | `x-max-priority=10`, `x-dead-letter-exchange=dead_letter_exchange` | 库存（Priority） |
| `customs_queue` | `x-message-ttl=8000`, `x-dead-letter-exchange=dead_letter_exchange` | 海关（TTL） |
| `nlp_queue` | `x-dead-letter-exchange=dead_letter_exchange` | NLP 文本审查 |
| `cv_queue` | `x-dead-letter-exchange=dead_letter_exchange` | CV 图片审查 |
| `notification_queue` | `x-dead-letter-exchange=dead_letter_exchange` | 支付成功通知（Direct） |
| `audit_queue` | `x-dead-letter-exchange=dead_letter_exchange` | 支付成功审计日志（Direct） |
| `dead_letter_queue` | — | 死信终端（告警服务消费） |
| `retry_queue` | `x-message-ttl=5000`, `x-dead-letter-exchange=order.compliance` | 重试缓冲（**不设** DL routing_key，保留原 rk 回流 cv/nlp） |

### Bindings

| exchange → queue | routing_key |
|------------------|-------------|
| `order.fulfillment` → `inventory_queue` | *(fanout，留空)* |
| `order.fulfillment` → `customs_queue` | *(fanout，留空)* |
| `order.compliance` → `nlp_queue` | `compliance.text.*` |
| `order.compliance` → `cv_queue` | `compliance.image.*` |
| `dead_letter_exchange` → `dead_letter_queue` | *(fanout，留空)* |
| `retry.exchange` → `retry_queue` | *(fanout，留空)* |
| `order.status` → `notification_queue` | `order.paid` |
| `order.status` → `audit_queue` | `order.paid` |

> **生产推荐**：把 `definitions.json` 挂载为 broker 的 [definitions file](https://www.rabbitmq.com/definitions.html)（或用 Terraform `rabbitmq_*` 资源），broker 启动即自带拓扑，完全脱离应用代码。
> 注意：改 `topology.py` 后需同步重新导出/改 `definitions.json`（尤其 TTL、priority 等参数值）。

---

## Exchange 对比

| 特性 | Fanout Exchange | Direct Exchange | Topic Exchange |
|------|----------------|-----------------|----------------|
| 路由方式 | 广播到所有绑定队列 | 精确匹配 routing key | 根据 routing key 通配符匹配 |
| Routing Key | 忽略 | 必须完全一致 | 支持 `*`（一个词）和 `#`（零或多个词） |
| 适用场景 | 同一事件需通知所有下游 | 精确事件类型路由 | 根据内容类型精准分发 |
| 本教程用途 | 库存 + 海关（都要处理每笔订单） | 通知 + 审计（仅处理 `order.paid`） | NLP + CV（按内容类型分别路由） |

## 项目结构

```
├── docker-compose.yml        # RabbitMQ 服务
├── definitions.json          # 拓扑导出（面板 Import definitions 一键导入）
├── pyproject.toml            # 项目元数据与依赖
├── Makefile                  # 常用命令（make run / make setup 等）
├── .env.example              # 环境变量模板
├── config.py                 # 应用配置（pydantic-settings）
├── topology.py               # Exchange/Queue/Binding 声明式定义
├── models.py                 # Pydantic 数据模型
├── setup_exchanges.py        # 根据 topology.py 创建 MQ 资源
├── run_demo.py               # 6 场景一键演示
├── mq/
│   └── consumer.py           # 消费者框架（手动ACK/重试/幂等）
└── services/
    ├── payment_service.py    # FastAPI 发布者（priority + batch + direct 状态事件）
    ├── inventory_service.py  # 库存消费者 (Fanout, Priority)
    ├── customs_service.py    # 海关消费者 (Fanout, TTL)
    ├── nlp_service.py        # NLP 消费者 (Topic)
    ├── cv_service.py         # CV 消费者 (Topic, 随机失败)
    ├── notification_service.py  # 通知消费者 (Direct)
    ├── audit_service.py      # 审计消费者 (Direct)
    └── alert_service.py      # 死信告警服务 (DLX)
```

## 生产环境检查清单

### 生产者可靠性
- **消息持久化**：Exchange 和 Queue `durable=True`，消息 `delivery_mode=PERSISTENT` ✓
- **Publisher Confirms**：robust channel 默认开启，broker 收到并确认才算成功 ✓
- **mandatory + on_return_raises**：消息不可路由时抛 `PublishError` 而非静默丢弃 ✓（见 `mq/publisher.py`）

### 消费者可靠性
- **手动 ACK**：处理完成后才确认，崩溃后消息重入队列 ✓
- **限流**：`prefetch_count=1` 防止消费者被消息淹没 ✓
- **死信队列 (DLX)**：reject/TTL 过期的消息集中处理 ✓
- **延迟重试**：失败先 publish 到 `retry.exchange`，TTL 后死信回流原队列，超过 `max_retries` 才进 DLQ ✓
- **错误分类**：`TRANSIENT`（重试）vs `PERMANENT`（直接 DLQ），未知异常默认 PERMANENT 防 poison message ✓（见 `mq/error_policy.py`）
- **幂等消费**：`IdempotencyStore` 抽象（`InMemoryStore`/`SqliteStore`），失败时 `unmark` 释放占位保证重试可重入 ✓（见 `mq/idempotency.py`）
- **优雅关停**：SIGINT/SIGTERM 后停止接收新消息，在途消息 `nack` 回队列 ✓

### 连接与拓扑
- **连接重连**：`aio_pika.connect_robust()` 自动重连 + 恢复 channel/consumer ✓
- **声明式拓扑**：`topology.py` 单一事实来源；应用代码只 `get` 不 `declare`（生产由面板/`definitions.json`/IaC 预配）✓
- **优先级队列**：`x-max-priority` 支持 VIP 订单插队 ✓
- **消息 TTL**：`x-message-ttl` 防止过时消息无限堆积 ✓

### 可运维
- **健康探针**：`/health`（liveness，探 AMQP 连接）+ `/ready`（readiness，探 Publisher channel）✓

## 清理

```bash
docker compose down      # 停止
docker compose down -v   # 停止并删除数据
```
