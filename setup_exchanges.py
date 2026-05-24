"""根据 topology.py 的声明式定义，在 RabbitMQ 中创建所有资源

运行方式：python setup_exchanges.py
"""

import asyncio
import sys

import aio_pika

from config import settings
from topology import BINDINGS, EXCHANGE_MAP, EXCHANGES, QUEUE_MAP, QUEUES


async def setup() -> None:
    connection = await aio_pika.connect_robust(settings.amqp_url)
    async with connection:
        channel = await connection.channel()

        # 0. 清理旧队列（参数变更时必须先删除重建）
        for q in QUEUES:
            try:
                await channel.queue_delete(q.name)
            except Exception:
                pass

        # 1. 声明所有 Exchange
        for ex in EXCHANGES:
            await channel.declare_exchange(
                ex.name,
                aio_pika.ExchangeType(ex.type),
                durable=ex.durable,
                arguments=ex.arguments or None,
            )
            print(f"  Exchange [{ex.name}] ({ex.type})")
        print()

        # 2. 声明所有 Queue
        for q in QUEUES:
            await channel.declare_queue(
                q.name,
                durable=q.durable,
                arguments=q.arguments or None,
            )
            extras = []
            for k, v in q.arguments.items():
                if k == "x-max-priority":
                    extras.append(f"priority={v}")
                elif k == "x-message-ttl":
                    extras.append(f"TTL={v}ms")
                elif k == "x-dead-letter-exchange":
                    extras.append(f"DLX={v}")
            desc = f"  # {q.desc}" if q.desc else ""
            print(f"  Queue   [{q.name}] ({', '.join(extras)}){desc}")
        print()

        # 3. 建立所有 Binding
        for b in BINDINGS:
            ex = await channel.get_exchange(b.exchange)
            q = await channel.get_queue(b.queue)
            await q.bind(ex, routing_key=b.routing_key)
            rk = f"  key={b.routing_key}" if b.routing_key else ""
            desc = f"  # {b.desc}" if b.desc else ""
            print(f"  Binding [{b.exchange}] → [{b.queue}]{rk}{desc}")

        print("\n所有资源已就绪 ✓")


if __name__ == "__main__":
    print("正在声明 RabbitMQ 资源...\n")
    try:
        asyncio.run(setup())
    except Exception as e:
        print(f"\n连接失败: {e}")
        print("请确认 RabbitMQ 已启动: docker compose up -d")
        sys.exit(1)
