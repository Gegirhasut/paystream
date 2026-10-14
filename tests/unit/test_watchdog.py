from app.consumer.watchdog import SubscriptionWatchdog
from app.messaging.topology import build_topology


class FakeTopology:
    def __init__(self, *answers: int | Exception) -> None:
        self.queue = build_topology(3, 2).queue
        self.answers = list(answers)
        self.declarations = 0

    async def declare(self, broker: object) -> int:
        self.declarations += 1
        answer = self.answers.pop(0)
        if isinstance(answer, Exception):
            raise answer
        return answer


async def run_checks(topology: FakeTopology) -> tuple[SubscriptionWatchdog, list[str]]:
    stopped: list[str] = []
    watchdog = SubscriptionWatchdog(None, topology, on_lost=lambda: stopped.append("stop"))  # type: ignore[arg-type]
    misses = 0
    while topology.answers and not watchdog.lost:
        misses = await watchdog.check(misses)
    return watchdog, stopped


async def test_restarts_after_two_checks_without_consumers() -> None:
    watchdog, stopped = await run_checks(FakeTopology(1, 0, 0, 1))
    assert watchdog.lost
    assert stopped == ["stop"]


async def test_a_single_miss_is_tolerated() -> None:
    # e.g. the robust connection is re-subscribing right at that moment
    watchdog, stopped = await run_checks(FakeTopology(0, 1, 0, 1, 0))
    assert not watchdog.lost
    assert stopped == []


async def test_unreachable_broker_is_not_a_miss() -> None:
    watchdog, _ = await run_checks(FakeTopology(0, ConnectionError("down"), 1, 0))
    assert not watchdog.lost
