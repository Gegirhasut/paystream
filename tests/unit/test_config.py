import pytest
from pydantic import ValidationError

from app.config import Settings


def test_defaults_match_the_specification(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in (
        "PROCESSING_MIN_DELAY",
        "PROCESSING_MAX_DELAY",
        "PROCESSING_SUCCESS_RATE",
        "RETRY_MAX_ATTEMPTS",
    ):
        monkeypatch.delenv(name, raising=False)
    settings = Settings(api_key="key", _env_file=None)

    assert (settings.processing_min_delay, settings.processing_max_delay) == (2, 5)
    assert settings.processing_success_rate == 0.9
    assert settings.retry_max_attempts == 3


def test_min_delay_must_not_exceed_max_delay() -> None:
    with pytest.raises(ValidationError, match="PROCESSING_MIN_DELAY"):
        Settings(api_key="key", processing_min_delay=6, processing_max_delay=5, _env_file=None)


@pytest.mark.parametrize("rate", [-0.1, 1.1])
def test_success_rate_is_a_probability(rate: float) -> None:
    with pytest.raises(ValidationError):
        Settings(api_key="key", processing_success_rate=rate, _env_file=None)


def test_api_key_is_required(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("API_KEY", raising=False)
    with pytest.raises(ValidationError):
        Settings(_env_file=None)


def test_gateway_timeout_must_exceed_max_delay() -> None:
    with pytest.raises(ValidationError, match="GATEWAY_TIMEOUT"):
        Settings(api_key="key", processing_max_delay=5, gateway_timeout=5, _env_file=None)


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"db_idle_in_transaction_timeout": 30}, "DB_IDLE_IN_TRANSACTION_TIMEOUT"),
        ({"db_idle_in_transaction_timeout": 40, "outbox_publish_timeout": 20}, "OUTBOX_PUBLISH"),
        ({"db_command_timeout": 120}, "DB_COMMAND_TIMEOUT"),
        ({"recovery_after": 300}, "RECOVERY_AFTER"),
        ({"outbox_retention_hours": 0.1}, "OUTBOX_RETENTION_HOURS"),
    ],
)
def test_timeouts_must_be_consistent(overrides: dict, message: str) -> None:
    with pytest.raises(ValidationError, match=message):
        Settings(api_key="key", _env_file=None, **overrides)


def test_defaults_are_consistent() -> None:
    settings = Settings(api_key="key", _env_file=None)
    assert settings.max_processing_time < settings.recovery_after
