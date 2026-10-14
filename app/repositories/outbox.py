from collections.abc import Sequence
from datetime import datetime

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import OutboxEvent


class OutboxRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    def add(self, event: OutboxEvent) -> None:
        self._session.add(event)

    async def lock_unpublished(self, limit: int) -> Sequence[OutboxEvent]:
        """SKIP LOCKED lets several relays run in parallel without double publishing."""
        result = await self._session.scalars(
            select(OutboxEvent)
            .where(OutboxEvent.published_at.is_(None))
            .order_by(OutboxEvent.created_at)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        return result.all()

    async def delete_published_before(self, cutoff: datetime, limit: int) -> int:
        ids = (
            select(OutboxEvent.id)
            .where(OutboxEvent.published_at < cutoff)
            .limit(limit)
            .with_for_update(skip_locked=True)
            .scalar_subquery()
        )
        result = await self._session.execute(
            delete(OutboxEvent).where(OutboxEvent.id.in_(ids)),
            execution_options={"synchronize_session": False},
        )
        return result.rowcount
