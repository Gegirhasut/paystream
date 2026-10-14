import asyncio
import logging
from typing import Any

from faststream.rabbit import Channel, RabbitBroker

logger = logging.getLogger(__name__)


def create_broker(url: str, **kwargs: Any) -> RabbitBroker:
    """RabbitMQ broker whose `mandatory` publishes really fail when unroutable.

    Publisher confirms are on by default, but without `on_return_raises` a message
    that no queue accepts is still confirmed and silently dropped, `mandatory`
    or not. The relay would mark such an event published and the payment would
    stay `pending` forever. With it, the publish raises and is retried.
    """
    return RabbitBroker(url, default_channel=Channel(on_return_raises=True), **kwargs)


async def connect_with_retry(
    broker: RabbitBroker, initial_delay: float = 1.0, max_delay: float = 30.0
) -> None:
    """Wait for RabbitMQ instead of crashing when it is not up yet."""
    delay = initial_delay
    while True:
        try:
            await broker.connect()
            return
        except Exception as exc:
            logger.warning("RabbitMQ is unavailable (%s), retrying in %.0fs", exc, delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, max_delay)
