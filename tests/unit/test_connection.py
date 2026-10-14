import pytest

from app.messaging.connection import connect_with_retry


class FlakyBroker:
    def __init__(self, failures: int) -> None:
        self.failures = failures
        self.calls = 0

    async def connect(self) -> None:
        self.calls += 1
        if self.calls <= self.failures:
            raise ConnectionError("refused")


async def test_retries_with_backoff_until_connected(monkeypatch: pytest.MonkeyPatch) -> None:
    delays: list[float] = []

    async def fake_sleep(delay: float) -> None:
        delays.append(delay)

    monkeypatch.setattr("app.messaging.connection.asyncio.sleep", fake_sleep)
    broker = FlakyBroker(failures=6)

    await connect_with_retry(broker, initial_delay=1, max_delay=10)  # type: ignore[arg-type]

    assert broker.calls == 7
    assert delays == [1, 2, 4, 8, 10, 10]
