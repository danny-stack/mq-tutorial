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
  HTTP POST /orders/{id}/pay
           │
     FastAPI (Payment Service)
           │ aio-pika publish
     ┌─────┴──────────────────────────┐
     │                                │
[Fanout: order.fulfillment]    [Topic: order.compliance]
   │            │                 │             │
   ▼            ▼                 ▼             ▼
库存服务    海关服务          NLP 服务      CV 服务
(0.5s)      (3s)             (2s)          (4s)
```

- **Fanout Exchange**：广播给库存 + 海关（无条件分发，绑定即收到）
- **Topic Exchange**：根据 routing key 路由
  - `compliance.text.*` → NLP 服务（文本审查）
  - `compliance.image.*` → CV 服务（图片审查）

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

### 3. 一键演示

```bash
python run_demo.py
```

脚本会自动：启动 RabbitMQ → 声明 Exchange/Queue → 启动所有服务 → 发送 3 笔测试订单 → 展示异步效果。

### 4. 管理面板

浏览器打开 http://localhost:15672（guest/guest），可以看到 Queues 页面中的 4 个队列和消息流量。

## 分步教程

如果想手动观察每一步，开 6 个终端：

```bash
# 终端 0: 声明资源
python setup_exchanges.py

# 终端 1-4: 启动消费者
python inventory_service.py   # Fanout 消费者
python customs_service.py     # Fanout 消费者
python nlp_service.py         # Topic 消费者
python cv_service.py          # Topic 消费者

# 终端 5: 启动 FastAPI
uvicorn payment_service:app --port 8000

# 终端 6: 触发支付
curl -X POST http://localhost:8000/orders/CN-20260524-001/pay | python -m json.tool
```

### 测试不同内容类型的路由

```bash
# 文本+图片 → NLP 和 CV 都收到
curl -X POST http://localhost:8000/orders/CN-20260524-001/pay

# 仅文本 → 只有 NLP 收到
curl -X POST http://localhost:8000/orders/CN-20260524-002/pay

# 仅图片 → 只有 CV 收到
curl -X POST http://localhost:8000/orders/CN-20260524-003/pay
```

## Fanout vs Topic 对比

| 特性 | Fanout Exchange | Topic Exchange |
|------|----------------|----------------|
| 路由方式 | 广播到所有绑定队列 | 根据 routing key 通配符匹配 |
| Routing Key | 忽略 | 支持 `*`（一个词）和 `#`（零或多个词） |
| 适用场景 | 同一事件需通知所有下游 | 根据内容类型精准分发 |
| 本教程用途 | 库存 + 海关（都要处理每笔订单） | NLP + CV（按内容类型分别路由） |

## 项目结构

```
├── docker-compose.yml        # RabbitMQ 服务
├── requirements.txt          # Python 依赖
├── config.py                 # 共享配置
├── models.py                 # Pydantic 数据模型
├── setup_exchanges.py        # 声明 Exchange/Queue/Binding
├── payment_service.py        # FastAPI 发布者
├── inventory_service.py      # 库存消费者 (Fanout)
├── customs_service.py        # 海关消费者 (Fanout)
├── nlp_service.py            # NLP 消费者 (Topic)
├── cv_service.py             # CV 消费者 (Topic)
├── consumers.py              # 通用消费者工具
└── run_demo.py               # 一键演示脚本
```

## 生产环境建议

- **消息持久化**：Exchange 和 Queue 设为 `durable=True`，消息设为 `delivery_mode=PERSISTENT`（本教程已实现）
- **手动 ACK**：处理完成后才确认，防止消息丢失（本教程已实现）
- **死信队列 (DLX)**：处理失败的消息自动进入死信队列，便于排查
- **连接重连**：使用 `aio_pika.connect_robust()` 自动重连（本教程已实现）
- **限流**：`prefetch_count=1` 防止消费者被消息淹没（本教程已实现）
- **监控告警**：通过 RabbitMQ Management API 监控队列积压
- **幂等消费**：消费者应支持重复消息，用 order_id 去重

## 清理

```bash
# 停止所有服务
docker compose down

# 删除 RabbitMQ 数据
docker compose down -v
```
