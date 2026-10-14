import asyncio
import contextlib
import sys

import httpx
from faststream import FastStream

from app.config import Settings, get_settings
from app.consumer.failures import FailureHandler
from app.consumer.handlers import register_handlers
from app.consumer.watchdog import SubscriptionWatchdog
from app.db import create_engine, create_session_factory
from app.logging_setup import setup_logging
from app.messaging.connection import connect_with_retry, create_broker
from app.messaging.topology import build_topology
from app.services.gateway import EmulatedPaymentGateway
from app.services.processor import PaymentProcessor
from app.services.webhook import HttpWebhookSender


def create_app(settings: Settings) -> tuple[FastStream, SubscriptionWatchdog]:
    # graceful_timeout: on shutdown, in-flight messages are finished, not dropped.
    broker = create_broker(
        settings.rabbitmq_url, graceful_timeout=settings.consumer_graceful_timeout
    )
    topology = build_topology(settings.retry_max_attempts, settings.retry_base_delay)

    engine = create_engine(settings)
    http_client = httpx.AsyncClient(timeout=settings.webhook_timeout)
    processor = PaymentProcessor(
        session_factory=create_session_factory(engine),
        gateway=EmulatedPaymentGateway(
            min_delay=settings.processing_min_delay,
            max_delay=settings.processing_max_delay,
            success_rate=settings.processing_success_rate,
        ),
        webhook_sender=HttpWebhookSender(http_client, timeout=settings.webhook_timeout),
        gateway_timeout=settings.gateway_timeout,
    )
    register_handlers(
        broker,
        topology,
        processor,
        FailureHandler(
            broker,
            topology,
            settings.retry_max_attempts,
            on_dead_letter=processor.mark_dead_lettered,
        ),
        settings.consumer_prefetch_count,
    )

    app = FastStream(broker)
    watchdog = SubscriptionWatchdog(
        broker, topology, on_lost=app.exit, interval=settings.consumer_watchdog_interval
    )
    watchdog_task: asyncio.Task[None] | None = None

    @app.on_startup
    async def declare_topology() -> None:
        # Delay queues and the DLQ must exist before the first message fails.
        await connect_with_retry(broker)
        await topology.declare(broker)

    @app.after_startup
    async def start_watchdog() -> None:
        nonlocal watchdog_task
        watchdog_task = asyncio.create_task(watchdog.run())

    @app.on_shutdown
    async def stop_watchdog() -> None:
        if watchdog_task is not None:
            watchdog_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await watchdog_task

    @app.after_shutdown
    async def release_resources() -> None:
        await http_client.aclose()
        await engine.dispose()

    return app, watchdog


def main() -> None:
    settings = get_settings()
    setup_logging(settings.log_level)
    app, watchdog = create_app(settings)
    asyncio.run(app.run())
    if watchdog.lost:
        sys.exit(1)  # a non-zero exit makes Docker / Kubernetes start a fresh consumer


if __name__ == "__main__":
    main()
