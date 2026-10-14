"""RabbitMQ topology (see README for the diagram).

- `payments` (direct) -> `payments.new`: work queue; rejected messages are
  dead-lettered via `payments.dlx` into `payments.new.dlq`.
- `payments.new.retry.<ms>`: one delay queue per backoff step. When the TTL
  expires the message is dead-lettered back to `payments`. A queue per step
  (instead of per-message TTL) avoids head-of-line blocking.
- After the last attempt the consumer publishes the message to the DLQ.
"""

from dataclasses import dataclass

from aio_pika import RobustQueue
from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue

PAYMENTS_EXCHANGE_NAME = "payments"
PAYMENTS_DLX_NAME = "payments.dlx"
PAYMENTS_NEW_ROUTING_KEY = "payments.new"
PAYMENTS_NEW_QUEUE_NAME = "payments.new"
PAYMENTS_DLQ_NAME = "payments.new.dlq"

ATTEMPT_HEADER = "x-attempt"
LAST_ERROR_HEADER = "x-last-error"


def retry_delays(max_attempts: int, base_delay: float) -> list[float]:
    """Exponential backoff between attempts: base, base*2, base*4, ..."""
    return [base_delay * 2**step for step in range(max_attempts - 1)]


@dataclass(frozen=True)
class Topology:
    exchange: RabbitExchange
    dlx: RabbitExchange
    queue: RabbitQueue
    dlq: RabbitQueue
    retry_queues: tuple[RabbitQueue, ...]

    def retry_queue_for(self, attempt: int) -> RabbitQueue:
        """Delay queue used after the given (1-based) failed attempt."""
        return self.retry_queues[attempt - 1]

    async def declare(self, broker: RabbitBroker) -> int:
        """Creates whatever is missing on the server; existing objects are left as is.

        FastStream caches declared objects and skips the server call next time, so
        a queue deleted on the server would never come back. Hence the explicit
        `declare()` on each of them (a no-op for an object that already exists).

        Returns how many consumers the main queue has (see the consumer watchdog).
        """
        exchange = await broker.declare_exchange(self.exchange)
        dlx = await broker.declare_exchange(self.dlx)
        for declared_exchange in (exchange, dlx):
            await declared_exchange.declare()

        queue = await broker.declare_queue(self.queue)
        declared = await queue.declare()
        await queue.bind(exchange, routing_key=PAYMENTS_NEW_ROUTING_KEY)

        dlq = await _declare_queue(broker, self.dlq)
        await dlq.bind(dlx, routing_key=PAYMENTS_DLQ_NAME)

        for retry_queue in self.retry_queues:
            await _declare_queue(broker, retry_queue)
        return declared.consumer_count or 0


async def _declare_queue(broker: RabbitBroker, queue: RabbitQueue) -> RobustQueue:
    declared = await broker.declare_queue(queue)
    await declared.declare()
    return declared


def build_topology(max_attempts: int, base_delay: float) -> Topology:
    exchange = RabbitExchange(PAYMENTS_EXCHANGE_NAME, type=ExchangeType.DIRECT, durable=True)
    dlx = RabbitExchange(PAYMENTS_DLX_NAME, type=ExchangeType.DIRECT, durable=True)

    queue = RabbitQueue(
        PAYMENTS_NEW_QUEUE_NAME,
        durable=True,
        routing_key=PAYMENTS_NEW_ROUTING_KEY,
        arguments={
            "x-dead-letter-exchange": PAYMENTS_DLX_NAME,
            "x-dead-letter-routing-key": PAYMENTS_DLQ_NAME,
        },
    )
    dlq = RabbitQueue(PAYMENTS_DLQ_NAME, durable=True, routing_key=PAYMENTS_DLQ_NAME)

    retry_queues = tuple(
        RabbitQueue(
            # Delay in the name: new backoff settings never clash with existing queue args.
            f"{PAYMENTS_NEW_QUEUE_NAME}.retry.{int(delay * 1000)}ms",
            durable=True,
            arguments={
                "x-message-ttl": int(delay * 1000),
                "x-dead-letter-exchange": PAYMENTS_EXCHANGE_NAME,
                "x-dead-letter-routing-key": PAYMENTS_NEW_ROUTING_KEY,
            },
        )
        for delay in retry_delays(max_attempts, base_delay)
    )
    return Topology(exchange=exchange, dlx=dlx, queue=queue, dlq=dlq, retry_queues=retry_queues)
