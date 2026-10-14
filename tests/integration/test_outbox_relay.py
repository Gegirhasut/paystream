import asyncio
from datetime import timedelta

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models import OutboxEvent
from app.outbox_relay.relay import MAX_BACKOFF, BatchResult, OutboxRelay
from tests.factories import create_payment


class FakePublisher:
    def __init__(self, fail_on: int | None = None, delay: float = 0) -> None:
        self.fail_on = fail_on
        self.delay = delay
        self.published: list[OutboxEvent] = []
        self.calls = 0

    async def publish(self, event: OutboxEvent) -> None:
        self.calls += 1
        await asyncio.sleep(self.delay)
        if self.fail_on is not None and self.calls == self.fail_on:
            raise ConnectionError("broker unavailable")
        self.published.append(event)


async def outbox(session_factory: async_sessionmaker[AsyncSession]) -> list[OutboxEvent]:
    async with session_factory() as session:
        return list(await session.scalars(select(OutboxEvent).order_by(OutboxEvent.created_at)))


def relay(session_factory, publisher, batch_size: int = 10) -> OutboxRelay:
    return OutboxRelay(session_factory, publisher, batch_size=batch_size, poll_interval=0.01)


async def test_publishes_pending_events_once(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    for _ in range(3):
        await create_payment(session_factory)
    publisher = FakePublisher()

    assert await relay(session_factory, publisher).publish_batch() == BatchResult(3)
    assert await relay(session_factory, publisher).publish_batch() == BatchResult(0)

    assert len(publisher.published) == 3
    rows = await outbox(session_factory)
    assert all(row.published_at is not None and row.attempts == 1 for row in rows)


async def test_failed_publish_keeps_event_for_next_run(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    for _ in range(3):
        await create_payment(session_factory)

    result = await relay(session_factory, FakePublisher(fail_on=2)).publish_batch()

    assert result == BatchResult(published=1, failed=True)
    rows = await outbox(session_factory)
    assert [row.published_at is not None for row in rows] == [True, False, False]
    assert rows[1].attempts == 1
    assert "broker unavailable" in rows[1].last_error

    assert await relay(session_factory, FakePublisher()).publish_batch() == BatchResult(2)
    rows = await outbox(session_factory)
    assert all(row.published_at is not None for row in rows)
    assert rows[1].attempts == 2 and rows[1].last_error is None


async def test_parallel_relays_do_not_publish_twice(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    for _ in range(6):
        await create_payment(session_factory)
    first, second = FakePublisher(delay=0.05), FakePublisher(delay=0.05)

    await asyncio.gather(
        relay(session_factory, first, batch_size=3).publish_batch(),
        relay(session_factory, second, batch_size=3).publish_batch(),
    )

    ids = [e.id for e in first.published + second.published]
    assert len(ids) == len(set(ids)) == 6


async def test_run_loop_stops_on_event(session_factory: async_sessionmaker[AsyncSession]) -> None:
    await create_payment(session_factory)
    publisher = FakePublisher()
    stop = asyncio.Event()

    task = asyncio.create_task(relay(session_factory, publisher).run(stop))
    for _ in range(100):
        if publisher.published:
            break
        await asyncio.sleep(0.01)
    stop.set()
    await asyncio.wait_for(task, timeout=1)

    assert len(publisher.published) == 1


async def test_long_batch_is_cut_short_but_always_progresses(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    for _ in range(3):
        await create_payment(session_factory)
    publisher = FakePublisher(delay=0.05)
    outbox_relay = OutboxRelay(
        session_factory, publisher, batch_size=10, poll_interval=0.01, max_batch_duration=0
    )

    assert await outbox_relay.publish_batch() == BatchResult(published=1, truncated=True)
    rows = await outbox(session_factory)
    assert sum(row.published_at is not None for row in rows) == 1
    assert sum(row.attempts for row in rows) == 1


async def test_run_loop_runs_recovery(session_factory: async_sessionmaker[AsyncSession]) -> None:
    class Recovery:
        runs = 0

        async def recover(self) -> int:
            Recovery.runs += 1
            return 0

    stop = asyncio.Event()
    outbox_relay = OutboxRelay(
        session_factory,
        FakePublisher(),
        batch_size=10,
        poll_interval=0.01,
        recovery=Recovery(),  # type: ignore[arg-type]
        recovery_interval=3600,
    )
    task = asyncio.create_task(outbox_relay.run(stop))
    await asyncio.sleep(0.1)
    stop.set()
    await asyncio.wait_for(task, timeout=1)

    assert Recovery.runs == 1  # right away, then once per interval


def test_backoff_grows_exponentially_and_is_capped() -> None:
    outbox_relay = OutboxRelay(None, FakePublisher(), batch_size=10, poll_interval=0.5)  # type: ignore[arg-type]

    assert [outbox_relay.backoff(n) for n in (1, 2, 3)] == [1.0, 2.0, 4.0]
    assert outbox_relay.backoff(100) == MAX_BACKOFF


async def test_purges_only_old_published_events(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    for _ in range(5):
        await create_payment(session_factory)
    await relay(session_factory, FakePublisher(fail_on=4)).publish_batch()  # 3 published

    async with session_factory.begin() as session:
        await session.execute(
            update(OutboxEvent)
            .where(OutboxEvent.id.in_([row.id for row in (await outbox(session_factory))[:2]]))
            .values(published_at=func.now() - timedelta(days=2))
        )

    purger = OutboxRelay(
        session_factory,
        FakePublisher(),
        batch_size=1,  # forces several delete batches
        poll_interval=0.01,
        retention=timedelta(days=1),
    )
    assert await purger.purge_published() == 2

    rows = await outbox(session_factory)
    assert len(rows) == 3
    assert sum(row.published_at is None for row in rows) == 2
