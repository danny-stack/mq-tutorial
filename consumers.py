"""通用消费者工具：连接、日志格式、消息处理框架"""

import asyncio
import json
import sys
import time
from datetime import datetime

import aio_pika


def log(tag: str, color_code: str, msg: str):
    ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    print(f"\033[{color_code}m{ts} [{tag}]\033[0m {msg}", flush=True)


COLOR_GREEN = "32"
COLOR_YELLOW = "33"
COLOR_BLUE = "34"
COLOR_MAGENTA = "35"


async def run_consumer(
    queue_name: str,
    tag: str,
    color: str,
    simulate_seconds: float,
    process_fn,
):
    log(tag, color, f"正在连接 RabbitMQ，监听 [{queue_name}]...")
    connection = await aio_pika.connect_robust("amqp://guest:guest@localhost/")
    async with connection:
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=1)
        queue = await channel.get_queue(queue_name)

        log(tag, color, f"已就绪，等待消息... (模拟耗时 {simulate_seconds}s)")

        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    body = json.loads(message.body.decode())
                    t0 = time.perf_counter()
                    log(tag, color, f"收到订单 {body.get('order_id')}，开始处理...")
                    await asyncio.sleep(simulate_seconds)
                    process_fn(body)
                    elapsed = time.perf_counter() - t0
                    log(
                        tag,
                        color,
                        f"订单 {body.get('order_id')} 处理完成 ✓ ({elapsed:.2f}s)",
                    )
