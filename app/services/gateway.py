import asyncio
import logging
import random
import uuid
from collections import OrderedDict
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
    """Stand-in for an external acquirer: random latency and outcome.

    Like a real acquirer it remembers outcomes by payment id, so a repeated
    charge returns the original result instead of charging again.
    """

    MAX_REMEMBERED = 100_000

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
        self._outcomes: OrderedDict[uuid.UUID, PaymentStatus] = OrderedDict()

    async def charge(self, payment: Payment) -> PaymentStatus:
        known = self._outcomes.get(payment.id)
        if known is not None:
            logger.info("Gateway already processed payment %s: %s", payment.id, known)
            return known

        await asyncio.sleep(self._rng.uniform(self._min_delay, self._max_delay))
        succeeded = self._rng.random() < self._success_rate
        status = PaymentStatus.SUCCEEDED if succeeded else PaymentStatus.FAILED
        self._outcomes[payment.id] = status
        if len(self._outcomes) > self.MAX_REMEMBERED:
            self._outcomes.popitem(last=False)
        logger.info("Gateway processed payment %s: %s", payment.id, status)
        return status
