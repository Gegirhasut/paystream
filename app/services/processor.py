import asyncio
import logging
import uuid
from collections.abc import Callable
from datetime import UTC, datetime

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models import Payment, PaymentStatus
from app.repositories.payments import PaymentRepository
from app.services.exceptions import PaymentNotFoundError
from app.services.gateway import FINAL_STATUSES, GatewayError, PaymentGateway
from app.services.webhook import WebhookSender

logger = logging.getLogger(__name__)


def utcnow() -> datetime:
    return datetime.now(UTC)


class PaymentProcessor:
    """Charges a payment and notifies the merchant.

    Idempotent: a finalized payment is never charged again and a delivered
    webhook is never resent, so redelivered or retried messages are safe.
    """

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        gateway: PaymentGateway,
        webhook_sender: WebhookSender,
        gateway_timeout: float = 30.0,
        clock: Callable[[], datetime] = utcnow,
    ) -> None:
        self._session_factory = session_factory
        self._gateway = gateway
        self._webhook_sender = webhook_sender
        self._gateway_timeout = gateway_timeout
        self._clock = clock

    async def process(self, payment_id: uuid.UUID) -> Payment:
        await self._charge_once(payment_id)
        return await self._notify_once(payment_id)

    async def _charge_once(self, payment_id: uuid.UUID) -> None:
        # The row lock serializes duplicate deliveries of the same event: the second
        # one waits, then sees a final status and does not call the gateway again.
        # If the outcome is not saved (timeout, crash, failed commit), the payment
        # stays pending and the retry relies on the gateway being idempotent.
        async with self._session_factory.begin() as session:
            payment = await PaymentRepository(session).get_for_update(payment_id)
            if payment is None:
                raise PaymentNotFoundError(payment_id)
            if payment.status != PaymentStatus.PENDING:
                logger.info("Payment %s already %s, skipping charge", payment_id, payment.status)
                return

            async with asyncio.timeout(self._gateway_timeout):
                status = await self._gateway.charge(payment)
            if status not in FINAL_STATUSES:
                raise GatewayError(f"Gateway returned a non-final status: {status}")
            payment.status = status
            payment.processed_at = self._clock()

    async def mark_dead_lettered(self, payment_id: uuid.UUID) -> None:
        """Records that all attempts failed, so recovery does not retry the webhook."""
        async with self._session_factory.begin() as session:
            await PaymentRepository(session).mark_dead_lettered(payment_id, self._clock())

    async def _notify_once(self, payment_id: uuid.UUID) -> Payment:
        # A separate transaction, so a failed webhook never rolls back the charge
        # result. The same lock keeps a duplicate delivery from resending it.
        async with self._session_factory.begin() as session:
            payment = await PaymentRepository(session).get_for_update(payment_id)
            if payment is None:
                raise PaymentNotFoundError(payment_id)
            if payment.webhook_delivered_at is not None:
                logger.info("Webhook for payment %s already delivered", payment_id)
                return payment

            await self._webhook_sender.send(payment)
            payment.webhook_delivered_at = self._clock()
        return payment
