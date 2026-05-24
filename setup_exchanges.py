"""声明 RabbitMQ Exchange、Queue、Binding 关系

包含：业务 Exchange、Dead Letter Exchange、Retry Exchange、带参数的业务队列。
运行方式：python setup_exchanges.py
"""

import asyncio
import sys

import aio_pika

from config import settings


async def setup() -> None:
    connection = await aio_pika.connect_robust(settings.amqp_url)
    async with connection:
        channel = await connection.channel()

        s = settings

        # ── 0. 清理旧队列（参数变更时必须先删除重建）──────────
        for qn in [
            s.queue_inventory, s.queue_customs, s.queue_nlp, s.queue_cv,
            s.retry_queue, s.dlx_queue,
        ]:
            try:
                await channel.queue_delete(qn)
            except Exception:
                pass

        # ── 1. Dead Letter Exchange (fanout) ─────────────────
        await channel.declare_exchange(
            s.dlx_name, aio_pika.ExchangeType.FANOUT, durable=True
        )
        await channel.declare_queue(s.dlx_queue, durable=True)
        dlx_ex = await channel.get_exchange(s.dlx_name)
        await (await channel.get_queue(s.dlx_queue)).bind(dlx_ex)
        print(f"  [DLX] {s.dlx_name} (fanout) → {s.dlx_queue}")

        # ── 2. Retry Exchange (direct) ───────────────────────
        await channel.declare_exchange(
            s.retry_exchange, aio_pika.ExchangeType.DIRECT, durable=True
        )
        await channel.declare_queue(
            s.retry_queue,
            durable=True,
            arguments={
                "x-message-ttl": s.retry_ttl_ms,
                "x-dead-letter-exchange": s.order_compliance_exchange,
            },
        )
        await (await channel.get_queue(s.retry_queue)).bind(
            await channel.get_exchange(s.retry_exchange),
            routing_key=s.queue_cv,
        )
        print(f"  [Retry] {s.retry_exchange} (direct) → {s.retry_queue} (TTL={s.retry_ttl_ms}ms)")

        # ── 3. 业务 Exchange ─────────────────────────────────
        await channel.declare_exchange(
            s.order_fulfillment_exchange, aio_pika.ExchangeType.FANOUT, durable=True
        )
        print(f"  Exchange [{s.order_fulfillment_exchange}] (fanout)")

        await channel.declare_exchange(
            s.order_compliance_exchange, aio_pika.ExchangeType.TOPIC, durable=True
        )
        print(f"  Exchange [{s.order_compliance_exchange}] (topic)")

        # ── 4. 业务 Queue（带 DLX / TTL / Priority 参数）─────
        queues = {
            s.queue_inventory: ("库存队列", s.queue_args_inventory),
            s.queue_customs: ("海关队列", s.queue_args_customs),
            s.queue_nlp: ("NLP 合规队列", s.queue_args_nlp),
            s.queue_cv: ("CV 合规队列", s.queue_args_cv),
        }
        for queue_name, (desc, args) in queues.items():
            await channel.declare_queue(queue_name, durable=True, arguments=args)
            extras = []
            if "x-max-priority" in args:
                extras.append(f"priority={args['x-max-priority']}")
            if "x-message-ttl" in args:
                extras.append(f"TTL={args['x-message-ttl']}ms")
            extras_str = f" ({', '.join(extras)})" if extras else ""
            print(f"  Queue [{queue_name}] ({desc}){extras_str} → DLX={s.dlx_name}")

        # ── 5. Binding ───────────────────────────────────────
        fanout_ex = await channel.get_exchange(s.order_fulfillment_exchange)
        await (await channel.get_queue(s.queue_inventory)).bind(fanout_ex)
        await (await channel.get_queue(s.queue_customs)).bind(fanout_ex)
        print(f"  {s.queue_inventory}, {s.queue_customs} → {s.order_fulfillment_exchange} (fanout)")

        topic_ex = await channel.get_exchange(s.order_compliance_exchange)
        await (await channel.get_queue(s.queue_nlp)).bind(topic_ex, routing_key=s.binding_text)
        await (await channel.get_queue(s.queue_cv)).bind(topic_ex, routing_key=s.binding_image)
        print(f"  {s.queue_nlp}  → {s.order_compliance_exchange} (binding: {s.binding_text})")
        print(f"  {s.queue_cv}  → {s.order_compliance_exchange} (binding: {s.binding_image})")

        print("\n所有资源已就绪 ✓")


if __name__ == "__main__":
    print("正在声明 RabbitMQ 资源...")
    try:
        asyncio.run(setup())
    except Exception as e:
        print(f"\n连接失败: {e}")
        print("请确认 RabbitMQ 已启动: docker compose up -d")
        sys.exit(1)
