import asyncio
import logging
import random
from typing import Protocol

from app.models import Payment, PaymentStatus

logger = logging.getLogger(__name__)

FINAL_STATUSES = frozenset({PaymentStatus.SUCCEEDED, PaymentStatus.FAILED})


class GatewayError(Exception):
    pass


class PaymentGateway(Protocol):
    async def charge(self, payment: Payment) -> PaymentStatus:
        """Charge the payment and return its final status.

        Must be idempotent by `payment.id` (sent to the acquirer as the
        idempotency key): a charge is retried when its outcome is unknown, e.g.
        after a timeout or when the result could not be saved.
        """
        ...


class EmulatedPaymentGateway:
    """Stand-in for an external acquirer: random latency, outcome by payment id.

    A real acquirer keeps the outcome under the idempotency key, so a repeated
    charge returns the original result. Here the outcome is derived from the
    payment id instead of being stored: the same payment always gets the same
    result, also after a consumer restart. Payment ids are random UUIDs, so the
    success rate still holds across payments.
    """

    def __init__(
        self,
        min_delay: float,
        max_delay: float,
        success_rate: float,
        rng: random.Random | None = None,
    ) -> None:
        if min_delay > max_delay:
            raise ValueError("min_delay must not exceed max_delay")
        self._min_delay = min_delay
        self._max_delay = max_delay
        self._success_rate = success_rate
        self._rng = rng or random.Random()

    async def charge(self, payment: Payment) -> PaymentStatus:
        await asyncio.sleep(self._rng.uniform(self._min_delay, self._max_delay))
        succeeded = random.Random(payment.id.int).random() < self._success_rate
        status = PaymentStatus.SUCCEEDED if succeeded else PaymentStatus.FAILED
        logger.info("Gateway processed payment %s: %s", payment.id, status)
        return status
