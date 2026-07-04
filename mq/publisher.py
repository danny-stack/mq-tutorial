"""生产者封装 — 单 channel + publisher confirm + mandatory 不可路由抛错

关键点：aio-pika 默认 on_return_raises=False，mandatory=True 的消息不可路由时
*不会抛异常*（publish 正常返回，消息却丢了）。必须显式
``channel(publisher_confirms=True, on_return_raises=True)``，不可路由才会抛
``PublishError``（``DeliveryError`` 子类），生产者才能感知失败。

三者结合的可靠性语义：
  - publisher_confirms：broker 收到并持久化（针对 PERSISTENT 消息）后才算成功
  - mandatory=True：消息必须路由到至少一个队列，否则视为失败
  - on_return_raises=True：把 broker 的 basic.return 转成异常而非静默成功
"""

from __future__ import annotations

import logging

import aio_pika
from aio_pika.exceptions import ChannelClosed, DeliveryError

logger = logging.getLogger("mq-tutorial")


class PublishError(Exception):
    """发布失败：消息不可路由（mandatory）或被 broker nack。"""

    def __init__(self, reason: str, *, routing_key: str = "", exchange: str = "") -> None:
        super().__init__(reason)
        self.routing_key = routing_key
        self.exchange = exchange


class Publisher:
    """单 channel 发布器。

    使用 robust 连接：断线自动重连并恢复 channel、publisher confirms。
    """

    def __init__(self, connection: aio_pika.RobustConnection) -> None:
        self._connection = connection
        self._channel: aio_pika.RobustChannel | None = None

    async def connect(self) -> None:
        # 核心修正：on_return_raises=True 才能让 mandatory 不可路由抛 PublishError。
        self._channel = await self._connection.channel(
            publisher_confirms=True,
            on_return_raises=True,
        )
        logger.info(
            "Publisher 就绪：publisher_confirms=%s, on_return_raises=True",
            self._channel.publisher_confirms,
        )

    async def publish(
        self,
        exchange_name: str,
        routing_key: str,
        message: aio_pika.Message,
        *,
        mandatory: bool = True,
        timeout: float | None = None,
    ) -> None:
        """发布消息。mandatory=True（默认）时不可路由会抛 PublishError。"""
        if self._channel is None:
            raise RuntimeError("Publisher 未 connect()，先 await publisher.connect()")
        try:
            exchange = await self._channel.get_exchange(exchange_name)
            await exchange.publish(
                message, routing_key=routing_key, mandatory=mandatory, timeout=timeout
            )
        except DeliveryError as e:
            # PublishError 是 DeliveryError 子类，不可路由 / broker nack 都走这里
            raise PublishError(
                f"消息不可路由或被 broker nack: {e}",
                routing_key=routing_key,
                exchange=exchange_name,
            ) from e
        except ChannelClosed as e:
            raise PublishError(
                f"channel 已关闭: {e}",
                routing_key=routing_key,
                exchange=exchange_name,
            ) from e

    @property
    def is_ready(self) -> bool:
        """channel 是否就绪（供 /ready 探针用）。"""
        return self._channel is not None and not self._channel.is_closed

    async def close(self) -> None:
        if self._channel is not None and not self._channel.is_closed:
            await self._channel.close()
