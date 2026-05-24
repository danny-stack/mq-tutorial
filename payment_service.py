"""Payment Service — FastAPI 发布者

启动方式：uvicorn payment_service:app --reload
测试：curl -X POST http://localhost:8000/orders/CN-20260524-001/pay
"""

import time
from contextlib import asynccontextmanager

import aio_pika
from fastapi import FastAPI, HTTPException

from config import (
    AMQP_URL,
    ORDER_COMPLIANCE_EXCHANGE,
    ORDER_FULFILLMENT_EXCHANGE,
    ROUTING_IMAGE_PREFIX,
    ROUTING_TEXT_PREFIX,
)
from models import SAMPLE_ORDERS, Order, PaymentResult


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.amqp_connection = await aio_pika.connect_robust(AMQP_URL)
    app.state.amqp_channel = await app.state.amqp_connection.channel()
    print("[Payment Service] 已连接 RabbitMQ")
    yield
    await app.state.amqp_connection.close()
    print("[Payment Service] 已断开 RabbitMQ")


app = FastAPI(title="跨境电商支付服务", lifespan=lifespan)


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/orders/{order_id}/pay", response_model=PaymentResult)
async def pay_order(order_id: str):
    # 查找订单，找不到则创建一个带默认值的订单
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
    message_body = order.model_dump_json().encode()

    published_to: list[str] = []
    t0 = time.perf_counter()

    try:
        # ── Fanout: 一条消息广播到库存 + 海关 ───────────
        fanout_ex = await channel.get_exchange(ORDER_FULFILLMENT_EXCHANGE)
        await fanout_ex.publish(
            aio_pika.Message(
                body=message_body,
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            ),
            routing_key="",
        )
        published_to.extend(["inventory_queue", "customs_queue"])

        # ── Topic: 根据内容类型精准路由 ────────────────────
        topic_ex = await channel.get_exchange(ORDER_COMPLIANCE_EXCHANGE)

        if order.has_text:
            rk = f"{ROUTING_TEXT_PREFIX}.{order_id}"
            await topic_ex.publish(
                aio_pika.Message(
                    body=message_body,
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                ),
                routing_key=rk,
            )
            published_to.append(f"nlp_queue (key={rk})")

        if order.has_image:
            rk = f"{ROUTING_IMAGE_PREFIX}.{order_id}"
            await topic_ex.publish(
                aio_pika.Message(
                    body=message_body,
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                ),
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


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
