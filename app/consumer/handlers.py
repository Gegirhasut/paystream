import logging

from faststream import AckPolicy
from faststream.rabbit import Channel, RabbitBroker, RabbitMessage

from app.consumer.failures import FailureHandler, current_attempt
from app.messaging.topology import Topology
from app.schemas.events import PaymentCreatedEvent
from app.services.exceptions import PaymentNotFoundError
from app.services.processor import PaymentProcessor

logger = logging.getLogger(__name__)


def register_handlers(
    broker: RabbitBroker,
    topology: Topology,
    processor: PaymentProcessor,
    failures: FailureHandler,
    prefetch_count: int,
) -> None:
    @broker.subscriber(
        topology.queue,
        topology.exchange,
        channel=Channel(prefetch_count=prefetch_count),
        # Anything that escapes the handler (e.g. a message that cannot be
        # parsed) is rejected without requeue and lands in the DLQ via DLX.
        ack_policy=AckPolicy.REJECT_ON_ERROR,
    )
    async def handle_payment_created(event: PaymentCreatedEvent, message: RabbitMessage) -> None:
        attempt = current_attempt(message.headers)
        logger.info("Processing payment %s, attempt %s", event.payment_id, attempt)
        try:
            await processor.process(event.payment_id)
        except PaymentNotFoundError as exc:
            await failures.handle(
                event, attempt, exc, retryable=False, message_id=message.message_id
            )
        except Exception as exc:
            await failures.handle(event, attempt, exc, message_id=message.message_id)
