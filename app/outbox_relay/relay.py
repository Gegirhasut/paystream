import asyncio
import contextlib
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol

from faststream.rabbit import RabbitBroker
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.logging_setup import describe_error
from app.messaging.topology import ATTEMPT_HEADER, Topology
from app.models import OutboxEvent
from app.outbox_relay.recovery import StalledPaymentRecovery
from app.repositories.outbox import OutboxRepository

logger = logging.getLogger(__name__)

MAX_ERROR_LENGTH = 1000
MAX_BACKOFF = 30.0


@dataclass(frozen=True, slots=True)
class BatchResult:
    published: int
    failed: bool = False
    # Stopped early to keep the transaction short; more rows are probably waiting.
    truncated: bool = False


class EventPublisher(Protocol):
    async def publish(self, event: OutboxEvent) -> None: ...


class RabbitEventPublisher:
    """Publishes outbox events and keeps the RabbitMQ topology in place.

    Why the topology is declared here and not once at startup: if RabbitMQ is
    restarted without its data (new volume, failover to an empty node), the
    exchange and queues are gone. Every publish then fails as unroutable, the
    relay retries forever and payments stay `pending` until someone restarts
    the relay. So the topology is declared before the first publish and again
    after any failed one; declaring is idempotent and costs a few round trips
    only after an error.
    """

    def __init__(self, broker: RabbitBroker, topology: Topology, timeout: float = 10.0) -> None:
        self._broker = broker
        self._topology = topology
        self._timeout = timeout
        self._declared = False

    async def publish(self, event: OutboxEvent) -> None:
        # Returns after the publisher confirm; `mandatory` turns an unroutable
        # message into an error. The timeout stops a hung connection from holding
        # the outbox row locks.
        try:
            async with asyncio.timeout(self._timeout):
                if not self._declared:
                    # First publish, or the previous one failed (see the class docstring).
                    await self._topology.declare(self._broker)
                    self._declared = True
                await self._broker.publish(
                    event.payload,
                    exchange=self._topology.exchange,
                    routing_key=event.routing_key,
                    message_id=str(event.id),
                    message_type=event.event_type,
                    persist=True,
                    mandatory=True,
                    headers={ATTEMPT_HEADER: 1},
                )
        except BaseException:
            # Cancellation and timeouts included: declare again on the next publish.
            self._declared = False
            raise


class OutboxRelay:
    """Moves committed outbox rows to the broker (at-least-once).

    A row is marked published only after the broker confirmed it; a crash in
    between means a duplicate message, which the idempotent consumer tolerates.
    Published rows are purged after the retention period.
    """

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        publisher: EventPublisher,
        batch_size: int,
        poll_interval: float,
        retention: timedelta = timedelta(hours=24),
        cleanup_interval: float = 300.0,
        max_batch_duration: float = 60.0,
        recovery: StalledPaymentRecovery | None = None,
        recovery_interval: float = 60.0,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._session_factory = session_factory
        self._publisher = publisher
        self._batch_size = batch_size
        self._poll_interval = poll_interval
        self._retention = retention
        self._cleanup_interval = cleanup_interval
        # The batch transaction idles while events are published; PostgreSQL ends
        # one that idles too long (DB_IDLE_IN_TRANSACTION_TIMEOUT), and then the
        # whole batch would be published again. So no new publish starts after this.
        self._max_batch_duration = max_batch_duration
        self._recovery = recovery
        self._recovery_interval = recovery_interval
        self._clock = clock

    def backoff(self, consecutive_failures: int) -> float:
        """Exponential backoff while the broker or the database is unavailable."""
        return min(self._poll_interval * 2**consecutive_failures, MAX_BACKOFF)

    async def publish_batch(self) -> BatchResult:
        published = 0
        failed = truncated = False
        loop = asyncio.get_running_loop()
        async with self._session_factory.begin() as session:
            events = await OutboxRepository(session).lock_unpublished(self._batch_size)
            deadline = loop.time() + self._max_batch_duration
            for event in events:
                if published and loop.time() > deadline:  # at least one per batch
                    truncated = True
                    break
                event.attempts += 1
                try:
                    await self._publisher.publish(event)
                except Exception as exc:
                    event.last_error = describe_error(exc)[:MAX_ERROR_LENGTH]
                    logger.warning(
                        "Failed to publish outbox event %s: %s", event.id, event.last_error
                    )
                    # Keep the order and back off: the broker is most likely down.
                    failed = True
                    break
                event.published_at = self._clock()
                event.last_error = None
                published += 1
        if published:
            logger.info("Published %s outbox event(s)", published)
        return BatchResult(published=published, failed=failed, truncated=truncated)

    async def purge_published(self) -> int:
        cutoff = self._clock() - self._retention
        purged = 0
        while True:
            async with self._session_factory.begin() as session:
                deleted = await OutboxRepository(session).delete_published_before(
                    cutoff, self._batch_size
                )
            purged += deleted
            if deleted < self._batch_size:
                break
        if purged:
            logger.info("Purged %s published outbox event(s)", purged)
        return purged

    @staticmethod
    async def _periodically(
        job: Callable[[], Awaitable[int]], interval: float, last_run: float, name: str
    ) -> float:
        now = asyncio.get_running_loop().time()
        if now - last_run < interval:
            return last_run
        try:
            await job()
        except Exception:
            logger.exception("%s failed", name)
        return now

    async def run(self, stop: asyncio.Event) -> None:
        logger.info("Outbox relay started")
        consecutive_failures = 0
        last_purge = last_recovery = float("-inf")
        while not stop.is_set():
            last_purge = await self._periodically(
                self.purge_published, self._cleanup_interval, last_purge, "Outbox cleanup"
            )
            if self._recovery is not None:
                last_recovery = await self._periodically(
                    self._recovery.recover, self._recovery_interval, last_recovery, "Recovery"
                )
            try:
                result = await self.publish_batch()
            except Exception:
                logger.exception("Outbox relay iteration failed")
                result = BatchResult(published=0, failed=True)

            consecutive_failures = consecutive_failures + 1 if result.failed else 0
            if consecutive_failures:
                delay = self.backoff(consecutive_failures)
            elif result.published < self._batch_size and not result.truncated:
                delay = self._poll_interval
            else:
                continue  # a full or cut short batch: more rows are probably waiting
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=delay)
        logger.info("Outbox relay stopped")
