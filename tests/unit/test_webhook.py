import asyncio
import hashlib
import hmac
import json
import uuid
from collections.abc import Callable
from datetime import UTC, datetime
from decimal import Decimal

import httpx
import pytest

from app.models import Currency, Payment, PaymentStatus
from app.services.egress import EgressGuard
from app.services.webhook import (
    HttpWebhookSender,
    WebhookBlockedError,
    WebhookDeliveryError,
    create_webhook_client,
)

SECRET = "test-secret"
NOW = 1_800_000_000
DNS = {
    "merchant.test": ["93.184.216.34"],
    "v6.merchant.test": ["2001:4860:4860::8888"],
    "rebind.test": ["93.184.216.34", "127.0.0.1"],  # one internal answer is enough to refuse
}


def make_payment(webhook_url: str = "http://merchant.test/hook") -> Payment:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    return Payment(
        id=uuid.uuid4(),
        amount=Decimal("10.00"),
        currency=Currency.EUR,
        description="test",
        metadata_={"a": 1},
        status=PaymentStatus.SUCCEEDED,
        webhook_url=webhook_url,
        created_at=now,
        processed_at=now,
    )


async def fake_dns(host: str, port: int) -> list[str]:
    if host not in DNS:
        raise OSError(f"Name or service not known: {host}")
    return DNS[host]


def sender(handler, allowed_internal_hosts=(), timeout: float = 5.0) -> HttpWebhookSender:
    return HttpWebhookSender(
        httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        secret=SECRET,
        guard=EgressGuard(allowed_internal_hosts, resolver=fake_dns),
        timeout=timeout,
        clock=lambda: NOW,
    )


def recording() -> tuple[list[httpx.Request], Callable[[httpx.Request], httpx.Response]]:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(204)

    return requests, handler


async def test_posts_signed_payment_result() -> None:
    requests, handler = recording()
    payment = make_payment()

    await sender(handler).send(payment)

    [request] = requests
    assert request.method == "POST"
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
    # What a merchant does: HMAC over "<timestamp>.<raw body>" with the shared secret.
    assert request.headers["X-Webhook-Timestamp"] == str(NOW)
    expected = hmac.new(SECRET.encode(), f"{NOW}.".encode() + request.content, hashlib.sha256)
    assert request.headers["X-Webhook-Signature"] == f"sha256={expected.hexdigest()}"


async def test_request_goes_to_the_checked_address() -> None:
    requests, handler = recording()

    await sender(handler).send(make_payment("https://merchant.test:8443/hook?x=1"))

    [request] = requests
    assert str(request.url) == "https://93.184.216.34:8443/hook?x=1"
    assert request.headers["Host"] == "merchant.test:8443"
    assert request.extensions["sni_hostname"] == "merchant.test"


async def test_ipv6_address_is_pinned() -> None:
    requests, handler = recording()

    await sender(handler).send(make_payment("http://v6.merchant.test/hook"))

    assert requests[0].url.host == "2001:4860:4860::8888"
    assert requests[0].headers["Host"] == "v6.merchant.test"


@pytest.mark.parametrize(
    "url", ["http://10.0.0.5/hook", "http://169.254.169.254/latest/meta-data/"]
)
async def test_internal_destination_is_blocked_without_a_request(url: str) -> None:
    requests, handler = recording()

    with pytest.raises(WebhookBlockedError):
        await sender(handler).send(make_payment(url))
    assert requests == []


async def test_host_resolving_to_an_internal_address_is_blocked() -> None:
    requests, handler = recording()

    with pytest.raises(WebhookBlockedError):
        await sender(handler).send(make_payment("http://rebind.test/hook"))
    assert requests == []


async def test_allowed_internal_host_is_reached_by_name() -> None:
    requests, handler = recording()

    await sender(handler, ["webhook-receiver"]).send(
        make_payment("http://webhook-receiver:8080/webhooks")
    )

    assert str(requests[0].url) == "http://webhook-receiver:8080/webhooks"


async def test_dns_failure_is_retryable() -> None:
    requests, handler = recording()

    with pytest.raises(WebhookDeliveryError) as raised:
        await sender(handler).send(make_payment("http://unknown.test/hook"))
    assert not isinstance(raised.value, WebhookBlockedError)
    assert requests == []


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

    with pytest.raises(WebhookDeliveryError, match="No response within"):
        await sender(handler, timeout=0.05).send(make_payment())


async def test_client_ignores_the_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HTTP_PROXY", "http://proxy.internal:3128")
    monkeypatch.setenv("HTTPS_PROXY", "http://proxy.internal:3128")

    default = httpx.AsyncClient()  # trust_env=True: the proxy from the environment is used
    client = create_webhook_client(timeout=5)
    try:
        assert any(transport is not None for transport in default._mounts.values())
        assert client.trust_env is False
        assert not any(transport is not None for transport in client._mounts.values())
    finally:
        await default.aclose()
        await client.aclose()
