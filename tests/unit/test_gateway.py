import random
import uuid
from collections import Counter

import pytest

from app.models import Payment, PaymentStatus
from app.services.gateway import EmulatedPaymentGateway


def payment_ids(count: int, seed: int = 42) -> list[uuid.UUID]:
    rng = random.Random(seed)
    return [uuid.UUID(int=rng.getrandbits(128), version=4) for _ in range(count)]


async def test_success_rate_is_respected() -> None:
    gateway = EmulatedPaymentGateway(0, 0, success_rate=0.9)

    results = Counter([await gateway.charge(Payment(id=i)) for i in payment_ids(2000)])

    assert 0.87 < results[PaymentStatus.SUCCEEDED] / 2000 < 0.93
    assert set(results) == {PaymentStatus.SUCCEEDED, PaymentStatus.FAILED}


@pytest.mark.parametrize(
    ("rate", "expected"), [(1, PaymentStatus.SUCCEEDED), (0, PaymentStatus.FAILED)]
)
async def test_extreme_rates(rate: float, expected: PaymentStatus) -> None:
    gateway = EmulatedPaymentGateway(0, 0, success_rate=rate)
    assert await gateway.charge(Payment(id=uuid.uuid4())) == expected


async def test_waits_within_bounds(monkeypatch: pytest.MonkeyPatch) -> None:
    delays: list[float] = []

    async def fake_sleep(delay: float) -> None:
        delays.append(delay)

    monkeypatch.setattr("app.services.gateway.asyncio.sleep", fake_sleep)
    gateway = EmulatedPaymentGateway(2, 5, success_rate=1)
    for _ in range(50):
        await gateway.charge(Payment(id=uuid.uuid4()))

    assert all(2 <= d <= 5 for d in delays)


async def test_repeated_charge_returns_the_original_outcome_even_after_restart() -> None:
    payments = [Payment(id=i) for i in payment_ids(50)]

    first = [await EmulatedPaymentGateway(0, 0, success_rate=0.5).charge(p) for p in payments]
    # A new instance stands for a restarted consumer: nothing is kept in memory.
    second = [await EmulatedPaymentGateway(0, 0, success_rate=0.5).charge(p) for p in payments]

    assert first == second
    assert set(first) == {PaymentStatus.SUCCEEDED, PaymentStatus.FAILED}


def test_invalid_bounds() -> None:
    with pytest.raises(ValueError, match="min_delay"):
        EmulatedPaymentGateway(5, 2, success_rate=1)
