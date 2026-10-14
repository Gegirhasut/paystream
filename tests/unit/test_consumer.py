import uuid
from decimal import Decimal
from typing import Any

import pytest
from faststream.rabbit import RabbitBroker, TestRabbitBroker

from app.consumer.failures import FailureAction, FailureHandler, current_attempt
from app.consumer.handlers import register_handlers
from app.messaging.topology import (
    ATTEMPT_HEADER,
    LAST_ERROR_HEADER,
    PAYMENTS_DLQ_NAME,
    PAYMENTS_NEW_ROUTING_KEY,
    build_topology,
)
from app.models import Currency
from app.schemas.events import PaymentCreatedEvent
from app.services.exceptions import PaymentNotFoundError
from app.services.webhook import WebhookDeliveryError

TOPOLOGY = build_topology(max_attempts=3, base_delay=2)


def make_event() -> PaymentCreatedEvent:
    return PaymentCreatedEvent(
        event_id=uuid.uuid4(),
        payment_id=uuid.uuid4(),
        amount=Decimal("1.00"),
        currency=Currency.RUB,
    )


class RecordingBroker:
    def __init__(self) -> None:
        self.published: list[dict[str, Any]] = []

    async def publish(self, message: Any, **kwargs: Any) -> None:
        self.published.append({"message": message, **kwargs})


@pytest.mark.parametrize(
    ("headers", "expected"),
    [({}, 1), ({ATTEMPT_HEADER: 2}, 2), ({ATTEMPT_HEADER: "3"}, 3), ({ATTEMPT_HEADER: "x"}, 1)],
)
def test_current_attempt(headers: dict, expected: int) -> None:
    assert current_attempt(headers) == expected


class TestFailureHandler:
    async def test_retries_with_growing_delay(self) -> None:
        broker = RecordingBroker()
        handler = FailureHandler(broker, TOPOLOGY, max_attempts=3)  # type: ignore[arg-type]
        event = make_event()

        assert await handler.handle(event, 1, WebhookDeliveryError("down")) is FailureAction.RETRY
        assert await handler.handle(event, 2, WebhookDeliveryError("down")) is FailureAction.RETRY

        first, second = broker.published
        assert first["queue"].name == "payments.new.retry.2000ms"
        assert first["headers"][ATTEMPT_HEADER] == 2
        assert "down" in first["headers"][LAST_ERROR_HEADER]
        assert second["queue"].name == "payments.new.retry.4000ms"
        assert second["headers"][ATTEMPT_HEADER] == 3
        assert all(p["persist"] and p["mandatory"] for p in broker.published)

    async def test_dead_letters_after_last_attempt(self) -> None:
        broker = RecordingBroker()
        handler = FailureHandler(broker, TOPOLOGY, max_attempts=3)  # type: ignore[arg-type]

        action = await handler.handle(make_event(), 3, WebhookDeliveryError("down"))

        assert action is FailureAction.DEAD_LETTER
        [published] = broker.published
        assert published["exchange"] is TOPOLOGY.dlx
        assert published["routing_key"] == PAYMENTS_DLQ_NAME
        assert published["headers"][ATTEMPT_HEADER] == 3
        assert published["mandatory"] is True

    async def test_dead_letter_is_recorded_before_publishing(self) -> None:
        broker = RecordingBroker()
        marked: list[uuid.UUID] = []

        async def mark(payment_id: uuid.UUID) -> None:
            assert broker.published == []
            marked.append(payment_id)

        handler = FailureHandler(broker, TOPOLOGY, max_attempts=3, on_dead_letter=mark)  # type: ignore[arg-type]
        event = make_event()

        await handler.handle(event, 3, WebhookDeliveryError("down"))

        assert marked == [event.payment_id]
        assert len(broker.published) == 1

    async def test_failed_dead_letter_mark_still_publishes_to_dlq(self) -> None:
        broker = RecordingBroker()

        async def mark(payment_id: uuid.UUID) -> None:
            raise ConnectionError("database is down")

        handler = FailureHandler(broker, TOPOLOGY, max_attempts=3, on_dead_letter=mark)  # type: ignore[arg-type]

        assert await handler.handle(make_event(), 3, TimeoutError()) is FailureAction.DEAD_LETTER
        [published] = broker.published
        assert published["headers"][LAST_ERROR_HEADER] == "TimeoutError"

    async def test_non_retryable_goes_straight_to_dlq(self) -> None:
        broker = RecordingBroker()
        handler = FailureHandler(broker, TOPOLOGY, max_attempts=3)  # type: ignore[arg-type]

        action = await handler.handle(
            make_event(), 1, PaymentNotFoundError("gone"), retryable=False
        )

        assert action is FailureAction.DEAD_LETTER
        assert broker.published[0]["routing_key"] == PAYMENTS_DLQ_NAME


class FakeProcessor:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.processed: list[uuid.UUID] = []

    async def process(self, payment_id: uuid.UUID) -> None:
        self.processed.append(payment_id)
        if self.error:
            raise self.error


class SpyFailures:
    def __init__(self) -> None:
        self.calls: list[tuple] = []

    async def handle(self, event, attempt, error, *, retryable=True, message_id=None):
        self.calls.append((event, attempt, type(error), retryable))


def make_broker(processor: FakeProcessor, failures: SpyFailures) -> RabbitBroker:
    broker = RabbitBroker()
    register_handlers(broker, TOPOLOGY, processor, failures, prefetch_count=1)  # type: ignore[arg-type]
    return broker


async def publish(broker: RabbitBroker, event: PaymentCreatedEvent, **kwargs: Any) -> None:
    await broker.publish(
        event, exchange=TOPOLOGY.exchange, routing_key=PAYMENTS_NEW_ROUTING_KEY, **kwargs
    )


class TestHandler:
    async def test_happy_path(self) -> None:
        processor, failures = FakeProcessor(), SpyFailures()
        broker = make_broker(processor, failures)
        event = make_event()

        async with TestRabbitBroker(broker):
            await publish(broker, event)

        assert processor.processed == [event.payment_id]
        assert failures.calls == []

    async def test_error_is_routed_to_failure_handler_with_attempt(self) -> None:
        processor, failures = FakeProcessor(WebhookDeliveryError("down")), SpyFailures()
        broker = make_broker(processor, failures)
        event = make_event()

        async with TestRabbitBroker(broker):
            await publish(broker, event, headers={ATTEMPT_HEADER: 2})

        assert failures.calls == [(event, 2, WebhookDeliveryError, True)]

    async def test_missing_payment_is_not_retried(self) -> None:
        processor, failures = FakeProcessor(PaymentNotFoundError("gone")), SpyFailures()
        broker = make_broker(processor, failures)

        async with TestRabbitBroker(broker):
            await publish(broker, make_event())

        assert failures.calls[0][2:] == (PaymentNotFoundError, False)

    async def test_invalid_message_is_rejected(self) -> None:
        processor, failures = FakeProcessor(), SpyFailures()
        broker = make_broker(processor, failures)

        async with TestRabbitBroker(broker):
            with pytest.raises(Exception):  # noqa: B017 - validation error surfaces in tests
                await broker.publish(
                    {"garbage": True},
                    exchange=TOPOLOGY.exchange,
                    routing_key=PAYMENTS_NEW_ROUTING_KEY,
                )

        assert processor.processed == []
