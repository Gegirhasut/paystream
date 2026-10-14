import asyncio

from app.models import Payment, PaymentStatus
from app.services.webhook import WebhookDeliveryError


class FakeGateway:
    def __init__(
        self,
        status: PaymentStatus = PaymentStatus.SUCCEEDED,
        delay: float = 0,
        error: Exception | None = None,
    ) -> None:
        self.status = status
        self.delay = delay
        self.error = error
        self.charged: list[Payment] = []

    async def charge(self, payment: Payment) -> PaymentStatus:
        self.charged.append(payment)
        await asyncio.sleep(self.delay)
        if self.error is not None:
            raise self.error
        return self.status


class FakeWebhookSender:
    def __init__(self, failures: int = 0) -> None:
        self.failures = failures
        self.attempts: list[Payment] = []
        self.delivered: list[Payment] = []

    async def send(self, payment: Payment) -> None:
        self.attempts.append(payment)
        if len(self.attempts) <= self.failures:
            raise WebhookDeliveryError("endpoint is down")
        self.delivered.append(payment)
