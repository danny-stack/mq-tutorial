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

dead_letter_queue ────── [告警服务]  ← 所有失败/过期的消息最终汇聚于此
```

## 快速开始

### 前置条件

- Docker + Docker Compose
- Python 3.10+

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 启动 RabbitMQ

```bash
docker compose up -d
```

### 3. 一键演示（6 个生产级场景）

```bash
python run_demo.py
```

### 4. 管理面板

浏览器打开 http://localhost:15672（guest/guest），可以看到 Queues 页面中的队列和消息流量。

---

## 6 个生产级场景详解

### 场景 1: 手动 ACK — 消费者崩溃不丢消息

**问题**：消费者处理到一半崩溃，消息是否丢失？

**方案**：手动 ACK（Manual Acknowledgement）。消费者处理完业务逻辑后才发送 ACK 确认。如果消费者在 ACK 之前崩溃（断开连接），RabbitMQ 检测到连接断开后会把消息重新放回队列，由其他消费者重新消费。

```
消费者收到消息 → 处理中... → 💥崩溃（未ACK）
    → RabbitMQ 检测到连接断开
    → 消息重入队列
    → 新消费者收到同一条消息，重新处理
```

### 场景 2: Dead Letter Queue — 重试上限保护

**问题**：消费者反复失败，消息无限重试拖垮系统。

**方案**：Dead Letter Exchange (DLX)。每个业务队列绑定一个 DLX，当消息被 reject 或过期时，自动转发到 DLX。配合重试计数器（`x-retry-count`），超过最大重试次数后消息进入死信队列，由专门的告警服务处理。

```
消息处理失败 → retry_count < 3? → reject(requeue=False) → DLX → 告警服务
                                 → 是 → 重新发布（retry_count+1）→ 再次消费
```

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

---

## 分步教程

如果想手动观察每一步：

```bash
# 终端 0: 声明资源
python setup_exchanges.py

# 终端 1-6: 启动消费者
python services/inventory_service.py          # 库存 (Fanout, Priority)
python services/inventory_service.py 2        # 库存 Worker 2 (Competing Consumers)
python services/customs_service.py            # 海关 (Fanout, TTL)
python services/nlp_service.py                # NLP (Topic)
python services/cv_service.py                 # CV (Topic, 可随机失败)
python services/alert_service.py              # 死信告警

# 终端 7: 启动 FastAPI
uvicorn services.payment_service:app --port 8000

# 终端 7: 触发支付
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

## Exchange 对比

| 特性 | Fanout Exchange | Topic Exchange |
|------|----------------|----------------|
| 路由方式 | 广播到所有绑定队列 | 根据 routing key 通配符匹配 |
| Routing Key | 忽略 | 支持 `*`（一个词）和 `#`（零或多个词） |
| 适用场景 | 同一事件需通知所有下游 | 根据内容类型精准分发 |
| 本教程用途 | 库存 + 海关（都要处理每笔订单） | NLP + CV（按内容类型分别路由） |

## 项目结构

```
├── docker-compose.yml        # RabbitMQ 服务
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
    ├── payment_service.py    # FastAPI 发布者（priority + batch）
    ├── inventory_service.py  # 库存消费者 (Fanout, Priority)
    ├── customs_service.py    # 海关消费者 (Fanout, TTL)
    ├── nlp_service.py        # NLP 消费者 (Topic)
    ├── cv_service.py         # CV 消费者 (Topic, 随机失败)
    └── alert_service.py      # 死信告警服务 (DLX)
```

## 生产环境检查清单

- **消息持久化**：Exchange 和 Queue `durable=True`，消息 `delivery_mode=PERSISTENT` ✓
- **手动 ACK**：处理完成后才确认，崩溃后消息重入队列 ✓
- **死信队列 (DLX)**：reject/TTL 过期的消息集中处理 ✓
- **重试上限**：`x-retry-count` 计数，超过后进 DLX ✓
- **幂等消费**：`order_id` 去重，防止重复处理 ✓
- **连接重连**：`aio_pika.connect_robust()` 自动重连 ✓
- **限流**：`prefetch_count=1` 防止消费者被消息淹没 ✓
- **优先级队列**：`x-max-priority` 支持 VIP 订单插队 ✓
- **消息 TTL**：`x-message-ttl` 防止过时消息无限堆积 ✓

## 清理

```bash
docker compose down      # 停止
docker compose down -v   # 停止并删除数据
```
