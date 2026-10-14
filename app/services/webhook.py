import asyncio
import hashlib
import hmac
import json
import logging
import time
from collections.abc import Callable
from typing import Protocol

import httpx

from app.logging_setup import describe_error
from app.models import Payment
from app.schemas.webhook import PaymentWebhook
from app.services.egress import BlockedDestinationError, EgressGuard

logger = logging.getLogger(__name__)

SIGNATURE_HEADER = "X-Webhook-Signature"
TIMESTAMP_HEADER = "X-Webhook-Timestamp"


class WebhookDeliveryError(Exception):
    pass


class WebhookBlockedError(WebhookDeliveryError):
    """The URL points into an internal network: retrying will not help."""


class WebhookSender(Protocol):
    async def send(self, payment: Payment) -> None: ...


def build_webhook(payment: Payment) -> PaymentWebhook:
    return PaymentWebhook(
        payment_id=payment.id,
        status=payment.status,
        amount=payment.amount,
        currency=payment.currency,
        description=payment.description,
        metadata=payment.metadata_,
        created_at=payment.created_at,
        processed_at=payment.processed_at,
    )


def create_webhook_client(timeout: float) -> httpx.AsyncClient:
    # trust_env=False: the client must not pick up HTTP(S)_PROXY (traffic to client
    # chosen URLs would take a route nobody chose for it), ~/.netrc credentials
    # (they would be sent to whatever host the client named) or SSL_CERT_* from the
    # environment. Where webhooks go is decided here, not by the deployment env.
    return httpx.AsyncClient(timeout=timeout, trust_env=False)


def sign(secret: str, timestamp: str, body: bytes) -> str:
    """HMAC-SHA256 over "<timestamp>.<body>"; the timestamp lets the merchant reject replays."""
    digest = hmac.new(secret.encode(), timestamp.encode() + b"." + body, hashlib.sha256)
    return f"sha256={digest.hexdigest()}"


class HttpWebhookSender:
    """Makes a single delivery attempt; retries are driven by the broker.

    Every request is signed (see `sign`) and goes only to a public address
    (see app/services/egress.py).
    """

    def __init__(
        self,
        client: httpx.AsyncClient,
        secret: str,
        guard: EgressGuard,
        timeout: float = 5.0,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._client = client
        self._secret = secret
        self._guard = guard
        self._timeout = timeout
        self._clock = clock

    async def send(self, payment: Payment) -> None:
        body = json.dumps(
            build_webhook(payment).model_dump(mode="json"), ensure_ascii=False
        ).encode()
        timestamp = str(int(self._clock()))
        headers = {
            "Content-Type": "application/json",
            "X-Payment-Id": str(payment.id),
            TIMESTAMP_HEADER: timestamp,
            SIGNATURE_HEADER: sign(self._secret, timestamp, body),
        }
        url = httpx.URL(payment.webhook_url)
        # httpx timeouts apply to each network operation, so an endpoint that
        # trickles bytes could hold the call (and the payment row lock) forever:
        # the whole attempt, DNS included, gets one deadline. Only the status
        # matters, so the response body is never read.
        try:
            async with asyncio.timeout(self._timeout):
                host = url.raw_host.decode("ascii")
                address = await self._guard.resolve(host, url.port or _default_port(url))
                extensions = {}
                if address is not None:
                    # Connect to the checked address itself. The real name still goes
                    # into Host and, for HTTPS, into SNI, so the certificate is
                    # verified against it.
                    headers["Host"] = url.netloc.decode("ascii")
                    if url.scheme == "https":
                        extensions["sni_hostname"] = host
                    url = url.copy_with(host=address)
                async with self._client.stream(
                    "POST", url, content=body, headers=headers, extensions=extensions
                ) as response:
                    status_code = response.status_code
        except BlockedDestinationError as exc:
            raise WebhookBlockedError(str(exc)) from exc
        except TimeoutError as exc:
            raise WebhookDeliveryError(f"No response within {self._timeout}s") from exc
        except (httpx.HTTPError, OSError) as exc:  # OSError: DNS resolution failed
            raise WebhookDeliveryError(describe_error(exc)) from exc

        if not 200 <= status_code < 300:
            raise WebhookDeliveryError(f"Webhook endpoint responded with {status_code}")
        logger.info("Webhook for payment %s delivered to %s", payment.id, payment.webhook_url)


def _default_port(url: httpx.URL) -> int:
    return 443 if url.scheme == "https" else 80
