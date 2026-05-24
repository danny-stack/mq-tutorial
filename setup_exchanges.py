"""声明 RabbitMQ Exchange、Queue、Binding 关系

运行方式：python setup_exchanges.py
"""

import asyncio
import sys

import aio_pika

from config import (
    AMQP_URL,
    BINDING_IMAGE,
    BINDING_TEXT,
    ORDER_COMPLIANCE_EXCHANGE,
    ORDER_FULFILLMENT_EXCHANGE,
    QUEUE_CV,
    QUEUE_CUSTOMS,
    QUEUE_INVENTORY,
    QUEUE_NLP,
)


async def setup():
    connection = await aio_pika.connect_robust(AMQP_URL)
    async with connection:
        channel = await connection.channel()

        # ── 声明 Exchange ──────────────────────────────────
        await channel.declare_exchange(
            ORDER_FULFILLMENT_EXCHANGE,
            aio_pika.ExchangeType.FANOUT,
            durable=True,
        )
        print(f"  Exchange [{ORDER_FULFILLMENT_EXCHANGE}] (fanout) 已声明")

        await channel.declare_exchange(
            ORDER_COMPLIANCE_EXCHANGE,
            aio_pika.ExchangeType.TOPIC,
            durable=True,
        )
        print(f"  Exchange [{ORDER_COMPLIANCE_EXCHANGE}] (topic)  已声明")

        # ── 声明 Queue ─────────────────────────────────────
        queues = {
            QUEUE_INVENTORY: "库存队列",
            QUEUE_CUSTOMS: "海关队列",
            QUEUE_NLP: "NLP 合规队列",
            QUEUE_CV: "CV 合规队列",
        }
        for queue_name, desc in queues.items():
            await channel.declare_queue(queue_name, durable=True)
            print(f"  Queue [{queue_name}] ({desc}) 已声明")

        # ── Binding ────────────────────────────────────────
        # Fanout: 绑定库存和海关队列
        fanout_ex = await channel.get_exchange(ORDER_FULFILLMENT_EXCHANGE)
        await (await channel.get_queue(QUEUE_INVENTORY)).bind(fanout_ex)
        await (await channel.get_queue(QUEUE_CUSTOMS)).bind(fanout_ex)
        print(f"  {QUEUE_INVENTORY}, {QUEUE_CUSTOMS} → {ORDER_FULFILLMENT_EXCHANGE} (fanout)")

        # Topic: 绑定 NLP 和 CV 队列
        topic_ex = await channel.get_exchange(ORDER_COMPLIANCE_EXCHANGE)
        await (await channel.get_queue(QUEUE_NLP)).bind(topic_ex, routing_key=BINDING_TEXT)
        await (await channel.get_queue(QUEUE_CV)).bind(topic_ex, routing_key=BINDING_IMAGE)
        print(f"  {QUEUE_NLP}  → {ORDER_COMPLIANCE_EXCHANGE} (binding: {BINDING_TEXT})")
        print(f"  {QUEUE_CV}  → {ORDER_COMPLIANCE_EXCHANGE} (binding: {BINDING_IMAGE})")

        print("\n所有 Exchange / Queue / Binding 已就绪")


if __name__ == "__main__":
    print("正在声明 RabbitMQ 资源...")
    try:
        asyncio.run(setup())
    except Exception as e:
        print(f"\n连接失败: {e}")
        print("请确认 RabbitMQ 已启动: docker-compose up -d")
        sys.exit(1)
