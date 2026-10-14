import asyncio
import json
import uuid
from datetime import UTC, datetime
from decimal import Decimal

import httpx
import pytest

from app.models import Currency, Payment, PaymentStatus
from app.services.webhook import HttpWebhookSender, WebhookDeliveryError


def make_payment() -> Payment:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    return Payment(
        id=uuid.uuid4(),
        amount=Decimal("10.00"),
        currency=Currency.EUR,
        description="test",
        metadata_={"a": 1},
        status=PaymentStatus.SUCCEEDED,
        webhook_url="http://merchant.test/hook",
        created_at=now,
        processed_at=now,
    )


def sender(handler) -> HttpWebhookSender:
    return HttpWebhookSender(httpx.AsyncClient(transport=httpx.MockTransport(handler)))


async def test_posts_payment_result() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(204)

    payment = make_payment()
    await sender(handler).send(payment)

    [request] = requests
    assert request.method == "POST"
    assert str(request.url) == "http://merchant.test/hook"
    assert request.headers["X-Payment-Id"] == str(payment.id)
    assert json.loads(request.content) == {
        "event": "payment.processed",
        "payment_id": str(payment.id),
        "status": "succeeded",
        "amount": "10.00",
        "currency": "EUR",
        "description": "test",
        "metadata": {"a": 1},
        "created_at": "2026-01-01T00:00:00Z",
        "processed_at": "2026-01-01T00:00:00Z",
    }


@pytest.mark.parametrize("status_code", [302, 400, 404, 500, 503])
async def test_error_status_raises(status_code: int) -> None:
    with pytest.raises(WebhookDeliveryError, match=str(status_code)):
        await sender(lambda _: httpx.Response(status_code)).send(make_payment())


@pytest.mark.parametrize("error", [httpx.ConnectError, httpx.ReadTimeout])
async def test_network_error_raises(error: type[httpx.TransportError]) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise error("boom", request=request)

    with pytest.raises(WebhookDeliveryError, match=error.__name__):
        await sender(handler).send(make_payment())


async def test_whole_attempt_has_a_deadline() -> None:
    # Per-operation httpx timeouts do not bound an endpoint that answers slowly.
    async def handler(request: httpx.Request) -> httpx.Response:
        await asyncio.sleep(10)
        return httpx.Response(200)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    with pytest.raises(WebhookDeliveryError, match="No response within"):
        await HttpWebhookSender(client, timeout=0.05).send(make_payment())
