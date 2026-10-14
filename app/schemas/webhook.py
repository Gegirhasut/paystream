import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any

from pydantic import BaseModel

from app.models import Currency, PaymentStatus


class PaymentWebhook(BaseModel):
    event: str = "payment.processed"
    payment_id: uuid.UUID
    status: PaymentStatus
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any]
    created_at: datetime
    processed_at: datetime | None
