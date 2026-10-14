import asyncio
import logging
from typing import Protocol

import httpx

from app.logging_setup import describe_error
from app.models import Payment
from app.schemas.webhook import PaymentWebhook

logger = logging.getLogger(__name__)


class WebhookDeliveryError(Exception):
    pass


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


class HttpWebhookSender:
    """Makes a single delivery attempt; retries are driven by the broker."""

    def __init__(self, client: httpx.AsyncClient, timeout: float = 5.0) -> None:
        self._client = client
        self._timeout = timeout

    async def send(self, payment: Payment) -> None:
        body = build_webhook(payment).model_dump(mode="json")
        # httpx timeouts apply to each network operation, so an endpoint that
        # trickles bytes could hold the call (and the payment row lock) forever:
        # the whole attempt gets one deadline. Only the status matters, so the
        # response body is never read.
        try:
            async with (
                asyncio.timeout(self._timeout),
                self._client.stream(
                    "POST",
                    payment.webhook_url,
                    json=body,
                    headers={"X-Payment-Id": str(payment.id)},
                ) as response,
            ):
                status_code = response.status_code
        except TimeoutError as exc:
            raise WebhookDeliveryError(f"No response within {self._timeout}s") from exc
        except httpx.HTTPError as exc:
            raise WebhookDeliveryError(describe_error(exc)) from exc

        if not 200 <= status_code < 300:
            raise WebhookDeliveryError(f"Webhook endpoint responded with {status_code}")
        logger.info("Webhook for payment %s delivered to %s", payment.id, payment.webhook_url)
