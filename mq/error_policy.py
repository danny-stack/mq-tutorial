"""错误分类策略 — 决定消费者失败后是延迟重试还是直接进 DLQ

工业实践：未知异常默认 PERMANENT（直接进 DLQ），避免 poison message 无限重试、
把真正的 bug 藏起来。只有明确判定为 TRANSIENT 的异常（网络抖动、限流、超时）
才走延迟重试。

用法：
    # 业务代码主动声明错误类型
    from mq.error_policy import TransientError, PermanentError

    async def process(body):
        if not body.get("order_id"):
            raise PermanentError("order_id 缺失")     # 直接进 DLQ
        try:
            await call_downstream(body)
        except httpx.HTTPStatusError as e:
            if e.response.status_code >= 500:
                raise TransientError(...) from e      # 延迟重试
            raise

    # 框架侧自动分类
    cls = classify(exc)   # → ErrorClass.TRANSIENT / PERMANENT
"""

from __future__ import annotations

import asyncio
from enum import StrEnum

import aio_pika


class ErrorClass(StrEnum):
    """错误分类，决定消息的去向。"""

    TRANSIENT = "transient"  # 临时性故障（网络/限流/超时）→ 延迟重试
    PERMANENT = "permanent"  # 永久性故障（数据错/业务规则违反）→ 直接 DLQ


class TransientError(Exception):
    """业务处理的临时性故障 — 框架会延迟重试。

    例：下游 HTTP 5xx/429、数据库瞬时不可用、外部服务超时。
    """


class PermanentError(Exception):
    """业务处理的永久性故障 — 框架直接送 DLQ，不重试。

    例：消息体格式错、必填字段缺失、订单不存在等业务规则违反。
    重试多少次结果都一样，不如让人看见（进 DLQ + 告警）。
    """


# 默认异常 → 分类映射。未列出的异常（含 KeyError/ValueError/TypeError）
# 一律按 PERMANENT 处理 —— 宁可让消息可见地失败，也不靠无界重试掩盖 bug。
DEFAULT_POLICY: dict[type[BaseException], ErrorClass] = {
    TransientError: ErrorClass.TRANSIENT,
    PermanentError: ErrorClass.PERMANENT,
    asyncio.TimeoutError: ErrorClass.TRANSIENT,
    aio_pika.exceptions.AMQPConnectionError: ErrorClass.TRANSIENT,
    aio_pika.exceptions.AMQPChannelError: ErrorClass.TRANSIENT,
}


def classify(
    exc: BaseException,
    policy: dict[type[BaseException], ErrorClass] | None = None,
) -> ErrorClass:
    """对异常分类。未知异常默认 PERMANENT（保守，防 poison message）。"""
    p = policy if policy is not None else DEFAULT_POLICY
    for exc_type, cls in p.items():
        if isinstance(exc, exc_type):
            return cls
    return ErrorClass.PERMANENT
