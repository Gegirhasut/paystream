import uuid
from decimal import Decimal

from pydantic import BaseModel

from app.models import Currency


class PaymentCreatedEvent(BaseModel):
    """Message for `payments.new`; the consumer re-reads the payment from the DB."""

    event_id: uuid.UUID
    payment_id: uuid.UUID
    amount: Decimal
    currency: Currency
