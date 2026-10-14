import asyncio
import signal
from datetime import timedelta

from app.config import Settings, get_settings
from app.db import create_engine, create_session_factory
from app.logging_setup import setup_logging
from app.messaging.connection import connect_with_retry, create_broker
from app.messaging.topology import build_topology
from app.outbox_relay.recovery import StalledPaymentRecovery
from app.outbox_relay.relay import OutboxRelay, RabbitEventPublisher


async def run(settings: Settings) -> None:
    topology = build_topology(settings.retry_max_attempts, settings.retry_base_delay)
    broker = create_broker(settings.rabbitmq_url)
    engine = create_engine(settings)

    try:
        await connect_with_retry(broker)

        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop.set)

        session_factory = create_session_factory(engine)
        relay = OutboxRelay(
            session_factory=session_factory,
            # Declares the topology before the first publish, so no event is unroutable.
            publisher=RabbitEventPublisher(
                broker, topology, timeout=settings.outbox_publish_timeout
            ),
            batch_size=settings.outbox_batch_size,
            poll_interval=settings.outbox_poll_interval,
            retention=timedelta(hours=settings.outbox_retention_hours),
            cleanup_interval=settings.outbox_cleanup_interval,
            max_batch_duration=settings.db_idle_in_transaction_timeout / 2,
            recovery=StalledPaymentRecovery(
                session_factory,
                stalled_after=timedelta(seconds=settings.recovery_after),
                max_attempts=settings.recovery_max_attempts,
                batch_size=settings.outbox_batch_size,
            ),
            recovery_interval=settings.recovery_interval,
        )
        await relay.run(stop)
    finally:
        await broker.stop()
        await engine.dispose()


def main() -> None:
    settings = get_settings()
    setup_logging(settings.log_level)
    asyncio.run(run(settings))


if __name__ == "__main__":
    main()
