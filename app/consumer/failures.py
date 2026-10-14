import asyncio
import enum
import logging
import uuid
from collections.abc import Awaitable, Callable, Mapping
from typing import Any

from faststream.rabbit import RabbitBroker

from app.logging_setup import describe_error
from app.messaging.topology import (
    ATTEMPT_HEADER,
    LAST_ERROR_HEADER,
    PAYMENTS_DLQ_NAME,
    Topology,
)
from app.schemas.events import PaymentCreatedEvent

logger = logging.getLogger(__name__)

MAX_ERROR_LENGTH = 1000
# A blocked connection (RabbitMQ memory or disk alarm) or an unreachable database
# would hold the handler for long; the side steps of a failure get a short limit.
PUBLISH_TIMEOUT = 10.0


class FailureAction(enum.StrEnum):
    RETRY = "retry"
    DEAD_LETTER = "dead_letter"


def current_attempt(headers: Mapping[str, Any]) -> int:
    try:
        return max(int(headers.get(ATTEMPT_HEADER, 1)), 1)
    except (TypeError, ValueError):
        return 1


def describe(error: BaseException) -> str:
    return describe_error(error)[:MAX_ERROR_LENGTH]


class FailureHandler:
    """Routes a failed message either to the next delay queue or to the DLQ.

    Publishes are `mandatory`: if the target queue is missing (e.g. RabbitMQ lost
    its data) the publish fails instead of silently dropping the message, the
    original is rejected and lands in the DLQ rather than vanishing.

    Before dead-lettering, `on_dead_letter` records it in the database, so that
    recovery leaves a webhook that failed all attempts in the DLQ. If that write
    fails too (often the very reason all attempts failed is the database being
    down), the payment simply stays eligible for recovery.
    """

    def __init__(
        self,
        broker: RabbitBroker,
        topology: Topology,
        max_attempts: int,
        on_dead_letter: Callable[[uuid.UUID], Awaitable[None]] | None = None,
    ) -> None:
        self._broker = broker
        self._topology = topology
        self._max_attempts = max_attempts
        self._on_dead_letter = on_dead_letter

    async def handle(
        self,
        event: PaymentCreatedEvent,
        attempt: int,
        error: BaseException,
        *,
        retryable: bool = True,
        message_id: str | None = None,
    ) -> FailureAction:
        if retryable and attempt < self._max_attempts:
            await self._retry(event, attempt, error, message_id)
            return FailureAction.RETRY
        await self._dead_letter(event, attempt, error, message_id)
        return FailureAction.DEAD_LETTER

    async def _retry(
        self, event: PaymentCreatedEvent, attempt: int, error: BaseException, message_id: str | None
    ) -> None:
        queue = self._topology.retry_queue_for(attempt)
        logger.warning(
            "Attempt %s/%s for payment %s failed (%s), retrying via %s",
            attempt,
            self._max_attempts,
            event.payment_id,
            describe(error),
            queue.name,
        )
        async with asyncio.timeout(PUBLISH_TIMEOUT):
            await self._broker.publish(
                event,
                queue=queue,
                persist=True,
                mandatory=True,
                message_id=message_id,
                headers={ATTEMPT_HEADER: attempt + 1, LAST_ERROR_HEADER: describe(error)},
            )

    async def _dead_letter(
        self, event: PaymentCreatedEvent, attempt: int, error: BaseException, message_id: str | None
    ) -> None:
        logger.error(
            "Payment %s moved to DLQ after %s attempt(s): %s",
            event.payment_id,
            attempt,
            describe(error),
        )
        if self._on_dead_letter is not None:
            try:
                async with asyncio.timeout(PUBLISH_TIMEOUT):
                    await self._on_dead_letter(event.payment_id)
            except Exception as exc:
                logger.warning(
                    "Could not mark payment %s as dead-lettered: %s",
                    event.payment_id,
                    describe(exc),
                )
        async with asyncio.timeout(PUBLISH_TIMEOUT):
            await self._broker.publish(
                event,
                exchange=self._topology.dlx,
                routing_key=PAYMENTS_DLQ_NAME,
                persist=True,
                mandatory=True,
                message_id=message_id,
                headers={ATTEMPT_HEADER: attempt, LAST_ERROR_HEADER: describe(error)},
            )
