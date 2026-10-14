import uuid
from decimal import Decimal
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models import Currency, Payment
from app.schemas.payment import PaymentCreate
from app.services.payments import PaymentService


def payment_payload(**overrides: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "amount": "1500.50",
        "currency": "RUB",
        "description": "Order #42",
        "metadata": {"order_id": 42, "customer": {"email": "user@example.com"}},
        "webhook_url": "https://merchant.example.com/webhooks/payments",
    }
    payload.update(overrides)
    return payload


async def create_payment(
    session_factory: async_sessionmaker[AsyncSession], **overrides: Any
) -> Payment:
    data = PaymentCreate(
        **{
            "amount": Decimal("100.00"),
            "currency": Currency.USD,
            "webhook_url": "http://merchant.test/hook",
            **overrides,
        }
    )
    async with session_factory() as session:
        result = await PaymentService(session).create(data, idempotency_key=str(uuid.uuid4()))
    return result.payment
