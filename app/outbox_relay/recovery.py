import logging
from collections.abc import Callable
from datetime import UTC, datetime, timedelta

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.repositories.outbox import OutboxRepository
from app.repositories.payments import PaymentRepository
from app.services.payments import payment_created_event

logger = logging.getLogger(__name__)


class StalledPaymentRecovery:
    """Dispatches again payments whose message was lost after it left the outbox.

    Once the relay has handed an event to RabbitMQ, nothing else would ever touch
    the payment if the message disappeared: the broker restarted without its data,
    a queue was deleted or purged, or a delayed retry was dead-lettered while the
    main queue did not exist (dead-lettering of classic queues is at-most-once).
    The payment would stay `pending`, or charged without a webhook, forever.

    A payment that made no progress for `stalled_after` and has no event in flight
    gets a new outbox event. That is safe: the consumer is idempotent and the
    gateway is called with the payment id as the idempotency key. A pending
    payment that exhausted its attempts (e.g. the gateway was down) is retried
    this way too, so its unknown outcome gets resolved; a final payment whose
    webhook failed all attempts is not (it stays in the DLQ, as the spec says).
    """

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        stalled_after: timedelta,
        max_attempts: int,
        batch_size: int = 100,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._session_factory = session_factory
        self._stalled_after = stalled_after
        self._max_attempts = max_attempts
        self._batch_size = batch_size
        self._clock = clock

    async def recover(self) -> int:
        cutoff = self._clock() - self._stalled_after
        recovered = 0
        while True:
            async with self._session_factory.begin() as session:
                payments = await PaymentRepository(session).lock_stalled(
                    cutoff, self._max_attempts, self._batch_size
                )
                outbox = OutboxRepository(session)
                for payment in payments:
                    payment.recovery_attempts += 1
                    outbox.add(payment_created_event(payment))
                    last = payment.recovery_attempts >= self._max_attempts
                    logger.log(
                        logging.ERROR if last else logging.WARNING,
                        "Payment %s (%s) made no progress for %s, dispatching it again "
                        "(recovery attempt %s/%s)%s",
                        payment.id,
                        payment.status,
                        self._stalled_after,
                        payment.recovery_attempts,
                        self._max_attempts,
                        ", the last one: reconcile manually if it fails" if last else "",
                    )
            recovered += len(payments)
            if len(payments) < self._batch_size:
                return recovered
