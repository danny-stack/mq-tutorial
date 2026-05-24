"""Payment Service — FastAPI 发布者（生产级）

支持：priority 优先级、message_id 幂等、batch 批量发送。
启动方式：uvicorn payment_service:app --reload
"""

import time
from contextlib import asynccontextmanager
from typing import Optional

import aio_pika
from fastapi import FastAPI, HTTPException, Query

from config import settings
from models import SAMPLE_ORDERS, Order, PaymentResult
from topology import EXCHANGE_MAP

s = settings


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.amqp_connection = await aio_pika.connect_robust(s.amqp_url)
    app.state.amqp_channel = await app.state.amqp_connection.channel()
    print("[Payment Service] 已连接 RabbitMQ")
    yield
    await app.state.amqp_connection.close()
    print("[Payment Service] 已断开 RabbitMQ")


app = FastAPI(title="跨境电商支付服务（生产级）", lifespan=lifespan)


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/orders/{order_id}/pay", response_model=PaymentResult)
async def pay_order(
    order_id: str,
    priority: Optional[int] = Query(default=None, ge=0, le=s.max_priority),
):
    order = next((o for o in SAMPLE_ORDERS if o.order_id == order_id), None)
    if order is None:
        order = Order(
            order_id=order_id,
            items=SAMPLE_ORDERS[0].items,
            customer="匿名用户",
            has_text=True,
            has_image=True,
        )
    order.compute_total()

    channel: aio_pika.Channel = app.state.amqp_channel
    published_to: list[str] = []
    t0 = time.perf_counter()

    try:
        msg_kwargs: dict = {
            "delivery_mode": aio_pika.DeliveryMode.PERSISTENT,
            "message_id": order_id,
            "headers": {"x-retry-count": 0},
        }
        if priority is not None:
            msg_kwargs["priority"] = priority

        # Fanout: order.fulfillment
        fanout_ex = await channel.get_exchange(EXCHANGE_MAP["order.fulfillment"].name)
        await fanout_ex.publish(
            aio_pika.Message(body=order.model_dump_json().encode(), **msg_kwargs),
            routing_key="",
        )
        published_to.extend(["inventory_queue", "customs_queue"])

        # Topic: order.compliance
        topic_ex = await channel.get_exchange(EXCHANGE_MAP["order.compliance"].name)
        body = order.model_dump_json().encode()

        if order.has_text:
            rk = f"{s.routing_text_prefix}.{order_id}"
            await topic_ex.publish(
                aio_pika.Message(body=body, **msg_kwargs),
                routing_key=rk,
            )
            published_to.append(f"nlp_queue (key={rk})")

        if order.has_image:
            rk = f"{s.routing_image_prefix}.{order_id}"
            await topic_ex.publish(
                aio_pika.Message(body=body, **msg_kwargs),
                routing_key=rk,
            )
            published_to.append(f"cv_queue (key={rk})")

    except Exception as e:
        raise HTTPException(status_code=503, detail=f"消息发布失败: {e}")

    elapsed_ms = (time.perf_counter() - t0) * 1000
    return PaymentResult(
        order_id=order_id,
        published_to=published_to,
        elapsed_ms=round(elapsed_ms, 2),
    )


@app.post("/orders/batch", response_model=list[PaymentResult])
async def batch_pay(count: int = Query(default=10, ge=1, le=50)):
    """批量发送订单（Competing Consumers / Priority 演示）"""
    results = []
    for i in range(count):
        order_id = f"BATCH-{i + 1:03d}"
        resp = await pay_order(
            order_id,
            priority=i % 5 + 1 if i < count - 1 else s.max_priority,
        )
        results.append(resp)
    return results


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=s.api_port)
