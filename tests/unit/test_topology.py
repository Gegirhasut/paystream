import pytest
from faststream.rabbit import RabbitQueue

from app.messaging.topology import (
    PAYMENTS_DLQ_NAME,
    PAYMENTS_DLX_NAME,
    PAYMENTS_EXCHANGE_NAME,
    PAYMENTS_NEW_ROUTING_KEY,
    build_topology,
    retry_delays,
)


def has_arguments(queue: RabbitQueue, expected: dict) -> bool:
    return expected.items() <= dict(queue.arguments).items()


def test_retry_delays_are_exponential() -> None:
    assert retry_delays(3, 2) == [2, 4]
    assert retry_delays(5, 0.5) == [0.5, 1, 2, 4]
    assert retry_delays(1, 2) == []


def test_main_queue_dead_letters_into_dlq() -> None:
    topology = build_topology(max_attempts=3, base_delay=2)

    assert topology.queue.name == PAYMENTS_NEW_ROUTING_KEY
    assert has_arguments(
        topology.queue,
        {
            "x-dead-letter-exchange": PAYMENTS_DLX_NAME,
            "x-dead-letter-routing-key": PAYMENTS_DLQ_NAME,
        },
    )
    assert topology.dlq.name == PAYMENTS_DLQ_NAME


def test_retry_queues_return_messages_to_main_exchange() -> None:
    topology = build_topology(max_attempts=3, base_delay=2)

    assert [q.name for q in topology.retry_queues] == [
        "payments.new.retry.2000ms",
        "payments.new.retry.4000ms",
    ]
    for queue, ttl in zip(topology.retry_queues, (2000, 4000), strict=True):
        assert has_arguments(
            queue,
            {
                "x-message-ttl": ttl,
                "x-dead-letter-exchange": PAYMENTS_EXCHANGE_NAME,
                "x-dead-letter-routing-key": PAYMENTS_NEW_ROUTING_KEY,
            },
        )
    assert topology.retry_queue_for(1) is topology.retry_queues[0]
    assert topology.retry_queue_for(2) is topology.retry_queues[1]
    with pytest.raises(IndexError):
        topology.retry_queue_for(3)
