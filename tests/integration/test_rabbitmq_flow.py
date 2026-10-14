"""End-to-end flow against a real RabbitMQ: outbox relay -> exchange -> consumer
-> retry delay queues -> DLQ. Runs in a throwaway vhost so it never interferes
with a consumer that may be running on the same broker."""

import asyncio
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from urllib.parse import urlsplit, urlunsplit

import httpx
import pytest
from faststream.rabbit import RabbitBroker
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import Settings
from app.consumer.failures import FailureHandler
from app.consumer.handlers import register_handlers
from app.consumer.watchdog import SubscriptionWatchdog
from app.messaging.connection import create_broker
from app.messaging.topology import ATTEMPT_HEADER, LAST_ERROR_HEADER, Topology, build_topology
from app.models import OutboxEvent, Payment, PaymentStatus
from app.outbox_relay.relay import OutboxRelay, RabbitEventPublisher
from app.services.processor import PaymentProcessor
from tests.factories import create_payment
from tests.fakes import FakeGateway, FakeWebhookSender

pytestmark = pytest.mark.integration

MAX_ATTEMPTS = 3
BASE_DELAY = 0.2


@pytest.fixture
async def vhost_url(settings: Settings) -> AsyncIterator[str]:
    parts = urlsplit(settings.rabbitmq_url)
    vhost = f"test-{uuid.uuid4().hex[:8]}"
    api = f"http://{parts.hostname}:15672/api"
    auth = (parts.username or "guest", parts.password or "guest")
    async with httpx.AsyncClient(auth=auth, timeout=5) as http:
        try:
            response = await http.put(f"{api}/vhosts/{vhost}")
        except httpx.HTTPError as exc:
            pytest.skip(f"RabbitMQ management API is not reachable: {exc}")
        response.raise_for_status()
        user = auth[0]
        permissions = {"configure": ".*", "write": ".*", "read": ".*"}
        (await http.put(f"{api}/permissions/{vhost}/{user}", json=permissions)).raise_for_status()
        yield urlunsplit((parts.scheme, parts.netloc, f"/{vhost}", "", ""))
        await http.delete(f"{api}/vhosts/{vhost}")


class Pipeline:
    def __init__(self, broker: RabbitBroker, topology: Topology, relay: OutboxRelay) -> None:
        self.broker = broker
        self.topology = topology
        self.relay = relay

    async def dlq_messages(self) -> list:
        queue = await self.broker.declare_queue(self.topology.dlq)
        messages = []
        while (message := await queue.get(no_ack=True, fail=False)) is not None:
            messages.append(message)
        return messages


@pytest.fixture
async def pipeline(
    vhost_url: str, session_factory: async_sessionmaker[AsyncSession]
) -> AsyncIterator[Callable[[FakeWebhookSender], Awaitable[Pipeline]]]:
    brokers: list[RabbitBroker] = []

    async def start(webhooks: FakeWebhookSender, gateway: FakeGateway | None = None) -> Pipeline:
        topology = build_topology(MAX_ATTEMPTS, BASE_DELAY)
        broker = create_broker(vhost_url, logger=None)
        brokers.append(broker)
        processor = PaymentProcessor(session_factory, gateway or FakeGateway(), webhooks)
        failures = FailureHandler(broker, topology, MAX_ATTEMPTS)
        register_handlers(broker, topology, processor, failures, prefetch_count=10)

        await broker.connect()
        await topology.declare(broker)
        await broker.start()
        relay = OutboxRelay(
            session_factory,
            RabbitEventPublisher(broker, topology),
            batch_size=10,
            poll_interval=0.05,
        )
        return Pipeline(broker, topology, relay)

    yield start
    for broker in brokers:
        await broker.stop()


async def wait_for(predicate: Callable[[], Awaitable[bool]], seconds: float = 10) -> None:
    deadline = asyncio.get_running_loop().time() + seconds
    while not await predicate():
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("condition was not met in time")
        await asyncio.sleep(0.05)


async def test_payment_is_processed_end_to_end(
    pipeline, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    webhooks = FakeWebhookSender()
    flow = await pipeline(webhooks)
    payment = await create_payment(session_factory)

    assert (await flow.relay.publish_batch()).published == 1

    async def delivered() -> bool:
        async with session_factory() as session:
            stored = await session.get(Payment, payment.id)
        return stored.webhook_delivered_at is not None

    await wait_for(delivered)
    assert [p.id for p in webhooks.delivered] == [payment.id]
    assert webhooks.delivered[0].status == PaymentStatus.SUCCEEDED


async def test_duplicate_event_is_processed_once(
    pipeline, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    """At-least-once publishing must not turn into double charging."""
    gateway, webhooks = FakeGateway(delay=0.2), FakeWebhookSender()
    flow = await pipeline(webhooks, gateway)
    payment = await create_payment(session_factory)

    await flow.relay.publish_batch()
    # Simulate a relay crash after publishing but before commit: same row again.
    async with session_factory.begin() as session:
        await session.execute(update(OutboxEvent).values(published_at=None))
    await flow.relay.publish_batch()

    async def delivered() -> bool:
        async with session_factory() as session:
            stored = await session.get(Payment, payment.id)
        return stored.webhook_delivered_at is not None

    await wait_for(delivered)
    await asyncio.sleep(0.5)  # give the duplicate time to be consumed
    assert len(gateway.charged) == 1
    assert len(webhooks.delivered) == 1


async def test_webhook_is_retried_until_it_succeeds(
    pipeline, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    webhooks = FakeWebhookSender(failures=MAX_ATTEMPTS - 1)
    flow = await pipeline(webhooks)
    await create_payment(session_factory)

    await flow.relay.publish_batch()

    async def delivered() -> bool:
        return bool(webhooks.delivered)

    await wait_for(delivered)
    assert len(webhooks.attempts) == MAX_ATTEMPTS
    assert await flow.dlq_messages() == []


async def test_message_goes_to_dlq_after_all_attempts(
    pipeline, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    webhooks = FakeWebhookSender(failures=100)
    flow = await pipeline(webhooks)
    payment = await create_payment(session_factory)

    await flow.relay.publish_batch()

    dead: list = []

    async def dead_lettered() -> bool:
        dead.extend(await flow.dlq_messages())
        return bool(dead)

    await wait_for(dead_lettered)
    [message] = dead
    assert message.headers[ATTEMPT_HEADER] == MAX_ATTEMPTS
    assert "endpoint is down" in message.headers[LAST_ERROR_HEADER]
    assert str(payment.id) in message.body.decode()
    assert len(webhooks.attempts) == MAX_ATTEMPTS

    async with session_factory() as session:
        stored = await session.get(Payment, payment.id)
    # The charge itself happened once; only the notification failed.
    assert stored.status == PaymentStatus.SUCCEEDED
    assert stored.webhook_delivered_at is None


async def test_unparseable_message_is_dead_lettered(pipeline) -> None:
    flow = await pipeline(FakeWebhookSender())

    await flow.broker.publish(
        b"not json", exchange=flow.topology.exchange, routing_key="payments.new"
    )

    dead: list = []

    async def dead_lettered() -> bool:
        dead.extend(await flow.dlq_messages())
        return bool(dead)

    await wait_for(dead_lettered)
    assert dead[0].body == b"not json"


async def test_unroutable_event_is_not_lost(
    vhost_url: str, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    """RabbitMQ lost its queue: the publish must fail (not be silently dropped),
    the event stays in the outbox and goes out once the topology is declared again."""
    topology = build_topology(MAX_ATTEMPTS, BASE_DELAY)
    broker = create_broker(vhost_url, logger=None)
    await broker.connect()
    try:
        relay = OutboxRelay(
            session_factory, RabbitEventPublisher(broker, topology), batch_size=10, poll_interval=0
        )
        await create_payment(session_factory)
        assert (await relay.publish_batch()).published == 1

        queue = await broker.declare_queue(topology.queue)
        await queue.delete(if_unused=False, if_empty=False)
        await create_payment(session_factory)

        result = await relay.publish_batch()
        assert result.failed
        assert result.published == 0
        async with session_factory() as session:
            [pending] = (
                await session.scalars(select(OutboxEvent).where(OutboxEvent.published_at.is_(None)))
            ).all()
        assert "NO_ROUTE" in pending.last_error

        assert (await relay.publish_batch()).published == 1
        queue = await broker.declare_queue(topology.queue)
        message = await queue.get(no_ack=True, fail=False)
        assert message is not None
        assert message.message_id == str(pending.id)
    finally:
        await broker.stop()


async def test_watchdog_notices_a_subscription_dropped_by_the_broker(
    pipeline, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    """Deleting the queue cancels the subscription silently; the watchdog sees it
    and brings the queue back (the restart itself is up to Docker / Kubernetes)."""
    flow = await pipeline(FakeWebhookSender())
    stopped: list[bool] = []
    watchdog = SubscriptionWatchdog(
        flow.broker, flow.topology, on_lost=lambda: stopped.append(True)
    )

    assert await watchdog.check(0) == 0  # subscribed

    queue = await flow.broker.declare_queue(flow.topology.queue)
    await queue.delete(if_unused=False, if_empty=False)

    misses = await watchdog.check(0)
    await watchdog.check(misses)
    assert watchdog.lost
    assert stopped == [True]
    # The topology is back, so the relay can publish and a fresh consumer can subscribe.
    queue = await flow.broker.declare_queue(flow.topology.queue)
    assert (await queue.declare()).consumer_count == 0
