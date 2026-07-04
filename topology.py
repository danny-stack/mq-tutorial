"""RabbitMQ 拓扑定义 — 所有 Exchange / Queue / Binding 的唯一声明来源

本文件是整个消息系统的"基础设施蓝图"，任何服务不得在代码中动态声明 Exchange 或 Queue。
修改拓扑前请确认：
  1. 理解每条 Binding 的数据流向
  2. 变更队列参数需要先删除旧队列（RabbitMQ 不允许修改已有队列参数）
  3. 通过 `python setup_exchanges.py` 应用变更

拓扑总览：

  order.fulfillment (fanout) ──┬──→ inventory_queue  [Priority=10, DLX]
                               └──→ customs_queue    [TTL=8s, DLX]

  order.compliance (topic) ────┬──→ nlp_queue        [DLX]  ← compliance.text.*
                               └──→ cv_queue         [DLX]  ← compliance.image.*

  dead_letter_exchange (fanout) ──→ dead_letter_queue       ← 所有死信汇聚

  retry.exchange (fanout) ──→ retry_queue [TTL=5s]   ← 重试缓冲（回流 order.compliance）
"""

from __future__ import annotations

from dataclasses import dataclass, field

from config import settings


@dataclass(frozen=True)
class ExchangeDef:
    name: str
    type: str  # fanout | direct | topic | headers
    durable: bool = True
    arguments: dict = field(default_factory=dict)
    desc: str = ""


@dataclass(frozen=True)
class QueueDef:
    name: str
    durable: bool = True
    arguments: dict = field(default_factory=dict)
    desc: str = ""


@dataclass(frozen=True)
class BindingDef:
    queue: str
    exchange: str
    routing_key: str = ""  # fanout 不需要
    desc: str = ""


# ═══════════════════════════════════════════════════════════════
#  Exchange 定义
# ═══════════════════════════════════════════════════════════════

EXCHANGES: list[ExchangeDef] = [
    # --- 业务 Exchange ---
    ExchangeDef(
        name="order.fulfillment",
        type="fanout",
        desc="订单履约广播（库存 + 海关）",
    ),
    ExchangeDef(
        name="order.compliance",
        type="topic",
        desc="合规审查路由（NLP + CV，按内容类型分发）",
    ),
    # --- 死信 Exchange ---
    ExchangeDef(
        name="dead_letter_exchange",
        type="fanout",
        desc="所有死信消息汇聚（rejected / expired / maxlen）",
    ),
    # --- 重试 Exchange ---
    # fanout：消息携带的原 routing_key 会被保留，TTL 过期死信到 order.compliance(topic)
    # 时仍能匹配 compliance.image.* / compliance.text.*，精确回流到 cv/nlp 队列。
    ExchangeDef(
        name="retry.exchange",
        type="fanout",
        desc="重试缓冲（fanout，TTL 后死信回流到 order.compliance，仅服务 topic 队列）",
    ),
]


# ═══════════════════════════════════════════════════════════════
#  Queue 定义
# ═══════════════════════════════════════════════════════════════

QUEUES: list[QueueDef] = [
    # --- 业务队列 ---
    QueueDef(
        name="inventory_queue",
        arguments={
            "x-max-priority": 10,
            "x-dead-letter-exchange": "dead_letter_exchange",
        },
        desc="库存扣减（支持 Priority + Competing Consumers）",
    ),
    QueueDef(
        name="customs_queue",
        arguments={
            "x-message-ttl": settings.customs_ttl_ms,
            "x-dead-letter-exchange": "dead_letter_exchange",
        },
        desc="海关申报（消息 TTL 超时进死信，见 settings.customs_ttl_ms）",
    ),
    QueueDef(
        name="nlp_queue",
        arguments={
            "x-dead-letter-exchange": "dead_letter_exchange",
        },
        desc="NLP 文本合规审查",
    ),
    QueueDef(
        name="cv_queue",
        arguments={
            "x-dead-letter-exchange": "dead_letter_exchange",
        },
        desc="CV 图片合规审查（随机失败 + 重试演示）",
    ),
    # --- 死信队列 ---
    QueueDef(
        name="dead_letter_queue",
        desc="死信消息终端（告警服务消费）",
    ),
    # --- 重试队列 ---
    # 故意不设 x-dead-letter-routing-key：死信时保留消息原 routing_key，
    # 回到 order.compliance(topic) 后仍能匹配 compliance.image.* / compliance.text.*。
    QueueDef(
        name="retry_queue",
        arguments={
            "x-message-ttl": settings.retry_ttl_ms,
            "x-dead-letter-exchange": "order.compliance",
        },
        desc="重试缓冲（TTL 后死信到 order.compliance，保留原 rk 回流到 cv/nlp 队列）",
    ),
]


# ═══════════════════════════════════════════════════════════════
#  Binding 定义
# ═══════════════════════════════════════════════════════════════

BINDINGS: list[BindingDef] = [
    # --- Fanout: order.fulfillment → 库存 + 海关 ---
    BindingDef(
        queue="inventory_queue",
        exchange="order.fulfillment",
        desc="每笔订单广播给库存服务",
    ),
    BindingDef(
        queue="customs_queue",
        exchange="order.fulfillment",
        desc="每笔订单广播给海关服务",
    ),
    # --- Topic: order.compliance → NLP + CV ---
    BindingDef(
        queue="nlp_queue",
        exchange="order.compliance",
        routing_key="compliance.text.*",
        desc="文本类内容路由到 NLP 服务",
    ),
    BindingDef(
        queue="cv_queue",
        exchange="order.compliance",
        routing_key="compliance.image.*",
        desc="图片类内容路由到 CV 服务",
    ),
    # --- DLX: dead_letter_exchange → dead_letter_queue ---
    BindingDef(
        queue="dead_letter_queue",
        exchange="dead_letter_exchange",
        desc="所有死信汇聚到告警队列",
    ),
    # --- Retry: retry.exchange → retry_queue（fanout，无需 routing_key）---
    BindingDef(
        queue="retry_queue",
        exchange="retry.exchange",
        desc="重试缓冲入口（fanout 广播到 retry_queue，rk 由 publish 时携带并在死信回流时复用）",
    ),
]


# ═══════════════════════════════════════════════════════════════
#  索引（按 name 快速查找）
# ═══════════════════════════════════════════════════════════════

EXCHANGE_MAP: dict[str, ExchangeDef] = {e.name: e for e in EXCHANGES}
QUEUE_MAP: dict[str, QueueDef] = {q.name: q for q in QUEUES}


def print_topology() -> None:
    """打印完整拓扑关系（供 review）"""
    print("Exchange 定义:")
    print("-" * 70)
    for ex in EXCHANGES:
        print(f"  {ex.name:<28} type={ex.type:<8} durable={ex.durable}")
        if ex.desc:
            print(f"  {'':28} # {ex.desc}")

    print("\nQueue 定义:")
    print("-" * 70)
    for q in QUEUES:
        extras = []
        for k, v in q.arguments.items():
            label = k.replace("x-", "").replace("-", " ")
            extras.append(f"{label}={v}")
        args_str = f"  ({', '.join(extras)})" if extras else ""
        print(f"  {q.name:<28} durable={q.durable}{args_str}")
        if q.desc:
            print(f"  {'':28} # {q.desc}")

    print("\nBinding 关系:")
    print("-" * 70)
    for b in BINDINGS:
        rk = f"  key={b.routing_key}" if b.routing_key else ""
        print(f"  {b.exchange:<28} → {b.queue:<22}{rk}")
        if b.desc:
            print(f"  {'':28} # {b.desc}")


if __name__ == "__main__":
    print("=" * 70)
    print("  RabbitMQ 拓扑定义总览")
    print("=" * 70 + "\n")
    print_topology()
