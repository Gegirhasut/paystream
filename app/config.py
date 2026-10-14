from functools import lru_cache
from typing import Self

from pydantic import Field, PositiveFloat, PositiveInt, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    log_level: str = "INFO"

    api_key: str = Field(min_length=1)

    database_url: str = "postgresql+asyncpg://payments:payments@localhost:5432/payments"
    db_pool_size: PositiveInt = 10
    db_echo: bool = False
    # A process that froze (or lost the network) inside a transaction keeps its row
    # locks for as long as PostgreSQL thinks the connection is alive, which can be
    # forever: the restarted consumer gets the message again, blocks on the lock and
    # the payment hangs. PostgreSQL ends such a session after this many seconds of
    # idling inside a transaction.
    db_idle_in_transaction_timeout: PositiveFloat = 120.0
    # Client-side limit per statement: a query to a database that vanished from the
    # network would otherwise wait for TCP to give up (~15 min). Longer than the
    # timeout above, so a lock wait behind a frozen session is resolved, not aborted.
    db_command_timeout: PositiveFloat = 180.0

    # heartbeat=20: a frozen or cut-off worker is noticed in under a minute.
    rabbitmq_url: str = "amqp://guest:guest@localhost:5672/?heartbeat=20"
    # Each in-flight message holds a DB connection while the gateway or the webhook is called,
    # so keep it within the connection pool size.
    consumer_prefetch_count: PositiveInt = 10
    consumer_graceful_timeout: PositiveFloat = 15.0
    consumer_watchdog_interval: PositiveFloat = 15.0

    outbox_poll_interval: PositiveFloat = 0.5
    outbox_batch_size: PositiveInt = 100
    outbox_publish_timeout: PositiveFloat = 10.0
    outbox_retention_hours: PositiveFloat = 24.0
    outbox_cleanup_interval: PositiveFloat = 300.0

    processing_min_delay: float = Field(default=2.0, ge=0)
    processing_max_delay: float = Field(default=5.0, ge=0)
    processing_success_rate: float = Field(default=0.9, ge=0, le=1)
    gateway_timeout: PositiveFloat = 30.0

    retry_max_attempts: PositiveInt = 3
    retry_base_delay: PositiveFloat = 2.0

    webhook_timeout: PositiveFloat = 5.0

    # Recovery of stalled payments: one with no webhook delivered and no progress
    # for this long (its message was lost somewhere) is dispatched again.
    recovery_after: PositiveFloat = 600.0
    recovery_interval: PositiveFloat = 60.0
    recovery_max_attempts: PositiveInt = 5

    @model_validator(mode="after")
    def check_timings(self) -> Self:
        if self.processing_min_delay > self.processing_max_delay:
            raise ValueError("PROCESSING_MIN_DELAY must not exceed PROCESSING_MAX_DELAY")
        if self.processing_max_delay >= self.gateway_timeout:
            raise ValueError("GATEWAY_TIMEOUT must exceed PROCESSING_MAX_DELAY")
        # Transactions idle while the gateway or the webhook is called, and while
        # the relay publishes a batch (see OutboxRelay: at most half of this timeout).
        if self.db_idle_in_transaction_timeout <= max(self.gateway_timeout, self.webhook_timeout):
            raise ValueError(
                "DB_IDLE_IN_TRANSACTION_TIMEOUT must exceed GATEWAY_TIMEOUT and WEBHOOK_TIMEOUT"
            )
        if self.outbox_publish_timeout * 2 >= self.db_idle_in_transaction_timeout:
            raise ValueError(
                "OUTBOX_PUBLISH_TIMEOUT must be under half of DB_IDLE_IN_TRANSACTION_TIMEOUT"
            )
        if self.db_command_timeout <= self.db_idle_in_transaction_timeout:
            raise ValueError("DB_COMMAND_TIMEOUT must exceed DB_IDLE_IN_TRANSACTION_TIMEOUT")
        # A payment still going through its attempts must not count as stalled.
        if self.recovery_after <= self.max_processing_time:
            raise ValueError(
                f"RECOVERY_AFTER must exceed the worst-case processing time "
                f"({self.max_processing_time:.0f}s with the current timeouts and retries)"
            )
        if self.outbox_retention_hours * 3600 <= self.recovery_after:
            raise ValueError("OUTBOX_RETENTION_HOURS must exceed RECOVERY_AFTER")
        return self

    @property
    def max_processing_time(self) -> float:
        """Worst case from publishing an event to its last attempt finishing."""
        delays = sum(self.retry_base_delay * 2**step for step in range(self.retry_max_attempts - 1))
        attempt = self.db_idle_in_transaction_timeout + self.gateway_timeout + self.webhook_timeout
        return delays + self.retry_max_attempts * attempt


@lru_cache
def get_settings() -> Settings:
    return Settings()
