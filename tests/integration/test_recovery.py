import asyncio
import uuid
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import delete, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models import OutboxEvent, Payment, PaymentStatus
from app.outbox_relay.recovery import StalledPaymentRecovery
from tests.factories import create_payment

STALLED_AFTER = timedelta(minutes=10)
LONG_AGO = timedelta(hours=1)


def recovery(session_factory, max_attempts: int = 5, batch_size: int = 100):
    return StalledPaymentRecovery(
        session_factory, STALLED_AFTER, max_attempts=max_attempts, batch_size=batch_size
    )


async def stalled_payment(session_factory: async_sessionmaker[AsyncSession], **fields) -> Payment:
    """A payment created an hour ago whose event was published an hour ago."""
    payment = await create_payment(session_factory)
    async with session_factory.begin() as session:
        await session.execute(
            update(Payment)
            .where(Payment.id == payment.id)
            .values(created_at=func.now() - LONG_AGO, **fields)
        )
        await session.execute(
            update(OutboxEvent)
            .where(OutboxEvent.aggregate_id == payment.id)
            .values(published_at=func.now() - LONG_AGO)
        )
    return payment


async def events_for(session_factory, payment: Payment) -> list[OutboxEvent]:
    async with session_factory() as session:
        return list(
            await session.scalars(
                select(OutboxEvent)
                .where(OutboxEvent.aggregate_id == payment.id)
                .order_by(OutboxEvent.created_at)
            )
        )


async def reload(session_factory, payment: Payment) -> Payment:
    async with session_factory() as session:
        return await session.get(Payment, payment.id)


async def test_lost_pending_payment_is_dispatched_again(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    payment = await stalled_payment(session_factory)

    assert await recovery(session_factory).recover() == 1

    old, new = await events_for(session_factory, payment)
    assert new.published_at is None
    assert new.id != old.id
    assert new.routing_key == old.routing_key
    assert new.payload["payment_id"] == str(payment.id)
    assert (await reload(session_factory, payment)).recovery_attempts == 1

    # The new event is in flight now: no second dispatch.
    assert await recovery(session_factory).recover() == 0


@pytest.mark.parametrize(
    ("fields", "recovered"),
    [
        pytest.param({}, True, id="pending"),
        pytest.param({"dead_lettered_at": datetime.now(UTC)}, True, id="pending-dead-lettered"),
        pytest.param(
            {"status": PaymentStatus.SUCCEEDED, "processed_at": datetime.now(UTC)},
            True,
            id="charged-webhook-lost",
        ),
        pytest.param(
            {
                "status": PaymentStatus.FAILED,
                "processed_at": datetime.now(UTC),
                "dead_lettered_at": datetime.now(UTC),
            },
            False,
            id="webhook-failed-all-attempts",
        ),
        pytest.param(
            {
                "status": PaymentStatus.SUCCEEDED,
                "processed_at": datetime.now(UTC),
                "webhook_delivered_at": datetime.now(UTC),
            },
            False,
            id="finished",
        ),
        pytest.param({"recovery_attempts": 5}, False, id="out-of-recovery-attempts"),
    ],
)
async def test_which_payments_are_recovered(
    session_factory: async_sessionmaker[AsyncSession], fields: dict, recovered: bool
) -> None:
    await stalled_payment(session_factory, **fields)

    assert await recovery(session_factory).recover() == int(recovered)


async def test_payment_with_event_in_flight_is_left_alone(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    fresh = await create_payment(session_factory)  # created just now
    unpublished = await stalled_payment(session_factory)
    recently_published = await stalled_payment(session_factory)
    async with session_factory.begin() as session:
        await session.execute(
            update(OutboxEvent)
            .where(OutboxEvent.aggregate_id == unpublished.id)
            .values(published_at=None)
        )
        await session.execute(
            update(OutboxEvent)
            .where(OutboxEvent.aggregate_id == recently_published.id)
            .values(published_at=func.now() - timedelta(minutes=1))
        )

    assert await recovery(session_factory).recover() == 0
    for payment in (fresh, unpublished, recently_published):
        assert len(await events_for(session_factory, payment)) == 1


async def test_purged_outbox_history_still_counts_as_stalled(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    payment = await stalled_payment(session_factory)
    async with session_factory.begin() as session:
        await session.execute(delete(OutboxEvent).where(OutboxEvent.aggregate_id == payment.id))

    assert await recovery(session_factory).recover() == 1


async def test_payment_being_processed_is_skipped(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    payment = await stalled_payment(session_factory)
    locked = asyncio.Event()
    release = asyncio.Event()

    async def consumer_holding_the_lock() -> None:
        async with session_factory.begin() as session:
            await session.get(Payment, payment.id, with_for_update=True)
            locked.set()
            await release.wait()

    task = asyncio.create_task(consumer_holding_the_lock())
    await locked.wait()
    try:
        assert await recovery(session_factory).recover() == 0
    finally:
        release.set()
        await task
    assert await recovery(session_factory).recover() == 1


async def test_recovers_in_batches_until_done(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    payments = [await stalled_payment(session_factory) for _ in range(5)]

    assert await recovery(session_factory, batch_size=2).recover() == 5
    for payment in payments:
        assert len(await events_for(session_factory, payment)) == 2


async def test_gives_up_after_max_attempts(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    payment = await stalled_payment(session_factory)
    for attempt in range(1, 3):
        assert await recovery(session_factory, max_attempts=2).recover() == 1
        # The dispatched event got published long ago and was lost again.
        async with session_factory.begin() as session:
            await session.execute(
                update(OutboxEvent)
                .where(OutboxEvent.aggregate_id == payment.id)
                .values(published_at=func.now() - LONG_AGO)
            )
        assert (await reload(session_factory, payment)).recovery_attempts == attempt

    assert await recovery(session_factory, max_attempts=2).recover() == 0


async def test_unknown_payment_id_is_not_an_issue(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    # Outbox rows of other aggregates do not affect the check.
    await stalled_payment(session_factory)
    async with session_factory.begin() as session:
        session.add(
            OutboxEvent(
                id=uuid.uuid4(),
                aggregate_type="other",
                aggregate_id=uuid.uuid4(),
                event_type="x",
                routing_key="x",
                payload={},
            )
        )
    assert await recovery(session_factory).recover() == 1
