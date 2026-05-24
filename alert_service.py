"""告警服务 — Dead Letter Queue 消费者

接收所有被 reject / TTL 过期 / 重试上限耗尽的消息，打印告警。
启动：python alert_service.py
"""

import asyncio
import json

import aio_pika

from config import settings
from consumers import COLOR_RED, COLOR_CYAN, _colorize, setup_logging
from topology import QUEUE_MAP

REASON_MAP = {
    "rejected": "消费者 reject（处理失败/重试上限）",
    "expired": "消息 TTL 过期（队列中等待超时）",
    "maxlen": "队列已满（超出最大长度）",
}


async def main() -> None:
    setup_logging()
    import logging
    logger = logging.getLogger("mq-tutorial")

    logger.info("%s 正在连接 RabbitMQ，监听死信队列...", _colorize("告警", COLOR_RED))
    connection = await aio_pika.connect_robust(settings.amqp_url)
    async with connection:
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=1)
        queue = await channel.get_queue(QUEUE_MAP["dead_letter_queue"].name)

        logger.info(
            "%s 已就绪，监听 [%s]（所有死信消息汇聚于此）",
            _colorize("告警", COLOR_RED), QUEUE_MAP["dead_letter_queue"].name,
        )

        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    body = json.loads(message.body.decode())
                    order_id = body.get("order_id", "unknown")

                    reason = "unknown"
                    original_queue = "unknown"
                    if message.headers and "x-death" in message.headers:
                        deaths = message.headers["x-death"]
                        if deaths:
                            reason = deaths[0].get("reason", "unknown")
                            original_queue = deaths[0].get("queue", "unknown")

                    reason_desc = REASON_MAP.get(reason, reason)
                    logger.warning(
                        "%s 死信消息 | 订单 %s | 来源: %s | 原因: %s",
                        _colorize("告警", COLOR_RED),
                        order_id, original_queue, reason_desc,
                    )
                    logger.info(
                        "%s → 模拟发送告警通知（钉钉/邮件/SMS）",
                        _colorize("告警", COLOR_CYAN),
                    )


if __name__ == "__main__":
    asyncio.run(main())
