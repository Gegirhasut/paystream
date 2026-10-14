import asyncio
import logging
from collections.abc import Callable

from faststream.rabbit import RabbitBroker

from app.logging_setup import describe_error
from app.messaging.topology import Topology

logger = logging.getLogger(__name__)


class SubscriptionWatchdog:
    """Notices that RabbitMQ silently dropped the subscription and restarts the consumer.

    When the queue is deleted (by hand, by a policy, ...) while the connection
    stays up, the broker cancels the subscription. Nothing reconnects, nothing is
    logged: the process keeps running, the relay re-creates the queue, messages
    pile up in it and payments stay `pending` forever.

    Every `interval` the topology is declared again (this also brings back a
    deleted retry queue or DLQ) and RabbitMQ reports the consumers of the main
    queue. Zero twice in a row means this process is not consuming: `on_lost` is
    called to shut it down gracefully, and Docker / Kubernetes start it again.
    While the broker is unreachable the check is skipped: the robust connection
    restores the subscription itself after reconnecting.
    """

    def __init__(
        self,
        broker: RabbitBroker,
        topology: Topology,
        on_lost: Callable[[], None],
        interval: float = 15.0,
        misses_allowed: int = 1,
    ) -> None:
        self._broker = broker
        self._topology = topology
        self._on_lost = on_lost
        self._interval = interval
        self._misses_allowed = misses_allowed
        self.lost = False

    async def check(self, misses: int) -> int:
        """One check; returns the updated number of consecutive misses."""
        try:
            consumers = await self._topology.declare(self._broker)
        except Exception as exc:
            logger.warning("Watchdog could not reach RabbitMQ: %s", describe_error(exc))
            return misses
        if consumers > 0:
            return 0
        misses += 1
        if misses > self._misses_allowed:
            logger.error(
                "Queue %s has no consumers: the subscription was dropped by the broker, "
                "restarting the consumer",
                self._topology.queue.name,
            )
            self.lost = True
            self._on_lost()
        return misses

    async def run(self) -> None:
        misses = 0
        while not self.lost:
            await asyncio.sleep(self._interval)
            misses = await self.check(misses)
