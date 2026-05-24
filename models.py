"""数据模型：订单、商品、支付结果"""

from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional

from pydantic import BaseModel, Field


class OrderItem(BaseModel):
    name: str
    description: str = ""
    image_url: Optional[str] = None
    quantity: int = 1
    price: Decimal = Field(default=Decimal("0"), ge=0)


class Order(BaseModel):
    order_id: str
    items: list[OrderItem]
    customer: str
    has_text: bool = True
    has_image: bool = True
    total_amount: Decimal = Field(default=Decimal("0"), ge=0)
    timestamp: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    def compute_total(self) -> Decimal:
        self.total_amount = sum(
            item.price * item.quantity for item in self.items
        )
        return self.total_amount


class PaymentResult(BaseModel):
    order_id: str
    status: str = "accepted"
    message: str = "支付已接受，下游服务异步处理中"
    published_to: list[str] = Field(default_factory=list)
    elapsed_ms: float = 0


# ── 测试用订单工厂 ──────────────────────────────────────────────

SAMPLE_ORDERS: list[Order] = [
    Order(
        order_id="CN-20260524-001",
        items=[
            OrderItem(
                name="无线蓝牙耳机",
                description="高保真降噪蓝牙耳机，适用于通勤和运动场景",
                image_url="https://example.com/images/headphones.jpg",
                quantity=2,
                price=Decimal("299.00"),
            ),
            OrderItem(
                name="USB-C 快充线",
                description="100W 快充数据线 1.5 米",
                quantity=3,
                price=Decimal("29.90"),
            ),
        ],
        customer="张三",
        has_text=True,
        has_image=True,
    ),
    Order(
        order_id="CN-20260524-002",
        items=[
            OrderItem(
                name="Python 编程入门",
                description="零基础学 Python 第三版",
                quantity=1,
                price=Decimal("79.00"),
            ),
        ],
        customer="李四",
        has_text=True,
        has_image=False,
    ),
    Order(
        order_id="CN-20260524-003",
        items=[
            OrderItem(
                name="艺术画框",
                description="",
                image_url="https://example.com/images/frame.png",
                quantity=1,
                price=Decimal("450.00"),
            ),
        ],
        customer="王五",
        has_text=False,
        has_image=True,
    ),
]
