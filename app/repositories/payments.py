import uuid
from collections.abc import Sequence
from datetime import datetime

from sqlalchemy import or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import OutboxEvent, Payment, PaymentStatus


class PaymentRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get(self, payment_id: uuid.UUID) -> Payment | None:
        return await self._session.get(Payment, payment_id)

    async def get_for_update(self, payment_id: uuid.UUID) -> Payment | None:
        return await self._session.get(Payment, payment_id, with_for_update=True)

    async def get_by_idempotency_key(self, key: str) -> Payment | None:
        result = await self._session.execute(select(Payment).where(Payment.idempotency_key == key))
        return result.scalar_one_or_none()

    def add(self, payment: Payment) -> None:
        self._session.add(payment)

    async def mark_dead_lettered(self, payment_id: uuid.UUID, when: datetime) -> None:
        await self._session.execute(
            update(Payment).where(Payment.id == payment_id).values(dead_lettered_at=when)
        )

    async def lock_stalled(
        self, cutoff: datetime, max_recovery_attempts: int, limit: int
    ) -> Sequence[Payment]:
        """Unfinished payments with no event published since `cutoff` nor waiting in the outbox.

        Unfinished: still pending, or final but the webhook was neither delivered
        nor given up on (a webhook that failed all attempts stays in the DLQ, as
        the spec requires). Payments being processed right now are row-locked by
        the consumer and skipped.
        """
        event_in_flight = (
            select(OutboxEvent.id)
            .where(
                OutboxEvent.aggregate_id == Payment.id,
                or_(OutboxEvent.published_at.is_(None), OutboxEvent.published_at >= cutoff),
            )
            .exists()
        )
        result = await self._session.scalars(
            select(Payment)
            .where(
                Payment.webhook_delivered_at.is_(None),
                or_(Payment.status == PaymentStatus.PENDING, Payment.dead_lettered_at.is_(None)),
                Payment.created_at < cutoff,
                Payment.recovery_attempts < max_recovery_attempts,
                ~event_in_flight,
            )
            .order_by(Payment.created_at)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        return result.all()
