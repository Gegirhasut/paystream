import asyncio
import uuid

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models import Payment, PaymentStatus
from app.services.exceptions import PaymentNotFoundError
from app.services.gateway import GatewayError
from app.services.processor import PaymentProcessor
from app.services.webhook import WebhookDeliveryError
from tests.factories import create_payment
from tests.fakes import FakeGateway, FakeWebhookSender


async def reload(session_factory: async_sessionmaker[AsyncSession], payment: Payment) -> Payment:
    async with session_factory() as session:
        return await session.get(Payment, payment.id)


@pytest.mark.parametrize("outcome", [PaymentStatus.SUCCEEDED, PaymentStatus.FAILED])
async def test_processes_payment_and_sends_webhook(
    session_factory: async_sessionmaker[AsyncSession], outcome: PaymentStatus
) -> None:
    payment = await create_payment(session_factory)
    gateway, webhooks = FakeGateway(outcome), FakeWebhookSender()

    await PaymentProcessor(session_factory, gateway, webhooks).process(payment.id)

    stored = await reload(session_factory, payment)
    assert stored.status == outcome
    assert stored.processed_at is not None
    assert stored.webhook_delivered_at is not None
    assert len(gateway.charged) == 1
    assert [p.status for p in webhooks.delivered] == [outcome]


async def test_retry_after_webhook_failure_does_not_charge_again(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    payment = await create_payment(session_factory)
    gateway, webhooks = FakeGateway(), FakeWebhookSender(failures=1)
    processor = PaymentProcessor(session_factory, gateway, webhooks)

    with pytest.raises(WebhookDeliveryError):
        await processor.process(payment.id)
    stored = await reload(session_factory, payment)
    assert stored.status == PaymentStatus.SUCCEEDED
    assert stored.webhook_delivered_at is None

    await processor.process(payment.id)

    assert len(gateway.charged) == 1
    assert len(webhooks.attempts) == 2
    assert (await reload(session_factory, payment)).webhook_delivered_at is not None


async def test_redelivered_message_is_a_no_op(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    payment = await create_payment(session_factory)
    gateway, webhooks = FakeGateway(), FakeWebhookSender()
    processor = PaymentProcessor(session_factory, gateway, webhooks)

    await processor.process(payment.id)
    await processor.process(payment.id)

    assert len(gateway.charged) == 1
    assert len(webhooks.delivered) == 1


async def test_concurrent_deliveries_charge_only_once(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    payment = await create_payment(session_factory)
    gateway = FakeGateway(delay=0.3)  # wide window for the race
    webhooks = FakeWebhookSender()
    processor = PaymentProcessor(session_factory, gateway, webhooks)

    results = await asyncio.gather(*(processor.process(payment.id) for _ in range(3)))

    assert len(gateway.charged) == 1
    assert len(webhooks.attempts) == 1
    assert {p.status for p in results} == {PaymentStatus.SUCCEEDED}
    assert (await reload(session_factory, payment)).status == PaymentStatus.SUCCEEDED


async def test_gateway_error_keeps_payment_pending(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    payment = await create_payment(session_factory)
    webhooks = FakeWebhookSender()
    broken = PaymentProcessor(
        session_factory, FakeGateway(error=TimeoutError("acquirer timeout")), webhooks
    )

    with pytest.raises(TimeoutError):
        await broken.process(payment.id)

    stored = await reload(session_factory, payment)
    assert stored.status == PaymentStatus.PENDING
    assert stored.processed_at is None
    assert webhooks.attempts == []

    # The retry goes through normally.
    await PaymentProcessor(session_factory, FakeGateway(), webhooks).process(payment.id)
    assert (await reload(session_factory, payment)).status == PaymentStatus.SUCCEEDED
    assert len(webhooks.delivered) == 1


async def test_gateway_timeout_keeps_payment_pending(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    payment = await create_payment(session_factory)
    webhooks = FakeWebhookSender()
    processor = PaymentProcessor(
        session_factory, FakeGateway(delay=5), webhooks, gateway_timeout=0.1
    )

    with pytest.raises(TimeoutError):
        await processor.process(payment.id)

    stored = await reload(session_factory, payment)
    assert stored.status == PaymentStatus.PENDING
    assert webhooks.attempts == []


async def test_non_final_gateway_status_is_an_error(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    payment = await create_payment(session_factory)
    processor = PaymentProcessor(
        session_factory, FakeGateway(PaymentStatus.PENDING), FakeWebhookSender()
    )

    with pytest.raises(GatewayError):
        await processor.process(payment.id)

    stored = await reload(session_factory, payment)
    assert stored.status == PaymentStatus.PENDING
    assert stored.processed_at is None


async def test_unknown_payment(session_factory: async_sessionmaker[AsyncSession]) -> None:
    processor = PaymentProcessor(session_factory, FakeGateway(), FakeWebhookSender())
    with pytest.raises(PaymentNotFoundError):
        await processor.process(uuid.uuid4())
