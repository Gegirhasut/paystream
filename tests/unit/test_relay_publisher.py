import asyncio
import uuid
from typing import Any

import pytest

from app.messaging.topology import build_topology
from app.models import OutboxEvent
from app.outbox_relay.relay import RabbitEventPublisher


class FakeBroker:
    def __init__(self) -> None:
        self.fail_next = False
        self.hang = False
        self.published: list[dict[str, Any]] = []

    async def publish(self, message: Any, **kwargs: Any) -> None:
        if self.hang:
            await asyncio.sleep(10)
        if self.fail_next:
            self.fail_next = False
            raise ConnectionError("channel closed")
        self.published.append({"message": message, **kwargs})


class FakeTopology:
    def __init__(self) -> None:
        self.exchange = build_topology(3, 2).exchange
        self.declarations = 0

    async def declare(self, broker: FakeBroker) -> None:
        self.declarations += 1


@pytest.fixture
def broker() -> FakeBroker:
    return FakeBroker()


def event() -> OutboxEvent:
    return OutboxEvent(
        id=uuid.uuid4(),
        aggregate_type="payment",
        aggregate_id=uuid.uuid4(),
        event_type="payment.created",
        routing_key="payments.new",
        payload={"payment_id": "x"},
    )


async def test_publishes_persistent_mandatory_message(broker: FakeBroker) -> None:
    topology = FakeTopology()
    outbox_event = event()

    await RabbitEventPublisher(broker, topology).publish(outbox_event)

    [sent] = broker.published
    assert sent["exchange"] is topology.exchange
    assert sent["routing_key"] == "payments.new"
    assert sent["message_id"] == str(outbox_event.id)
    assert sent["persist"] is True
    assert sent["mandatory"] is True
    assert sent["headers"] == {"x-attempt": 1}


async def test_declares_topology_once_and_again_after_a_failure(broker: FakeBroker) -> None:
    topology = FakeTopology()
    publisher = RabbitEventPublisher(broker, topology)

    await publisher.publish(event())
    await publisher.publish(event())
    assert topology.declarations == 1

    broker.fail_next = True
    with pytest.raises(ConnectionError):
        await publisher.publish(event())

    await publisher.publish(event())
    assert topology.declarations == 2
    assert len(broker.published) == 3


async def test_publish_times_out(broker: FakeBroker) -> None:
    broker.hang = True
    publisher = RabbitEventPublisher(broker, FakeTopology(), timeout=0.05)

    with pytest.raises(TimeoutError):
        await publisher.publish(event())
