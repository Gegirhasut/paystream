import pytest

from app.services.egress import BlockedDestinationError, EgressGuard, is_public_address


@pytest.mark.parametrize(
    "address",
    [
        "10.0.0.1",
        "172.16.5.4",
        "192.168.1.1",
        "127.0.0.1",
        "0.0.0.0",
        "169.254.169.254",  # cloud metadata
        "100.64.0.1",  # carrier-grade NAT
        "224.0.0.1",  # multicast
        "::1",
        "::ffff:127.0.0.1",  # IPv4-mapped loopback
        "fd00::1",
        "fe80::1%eth0",
    ],
)
def test_internal_addresses_are_not_public(address: str) -> None:
    assert not is_public_address(address)


@pytest.mark.parametrize("address", ["8.8.8.8", "93.184.216.34", "2001:4860:4860::8888"])
def test_public_addresses(address: str) -> None:
    assert is_public_address(address)


def resolver(answers: dict[str, list[str]]):
    async def resolve(host: str, port: int) -> list[str]:
        return answers[host]

    return resolve


async def test_resolves_to_a_public_address() -> None:
    guard = EgressGuard(resolver=resolver({"merchant.example": ["93.184.216.34"]}))
    assert await guard.resolve("merchant.example", 443) == "93.184.216.34"


@pytest.mark.parametrize(
    "answer",
    [["10.0.0.5"], ["93.184.216.34", "127.0.0.1"], []],
    ids=["internal", "one-of-several-internal", "nothing"],
)
async def test_internal_or_missing_answer_is_blocked(answer: list[str]) -> None:
    guard = EgressGuard(resolver=resolver({"evil.example": answer}))
    with pytest.raises(BlockedDestinationError):
        await guard.resolve("evil.example", 80)


async def test_ip_literal_is_checked_without_dns() -> None:
    guard = EgressGuard(resolver=resolver({}))
    assert await guard.resolve("8.8.8.8", 80) == "8.8.8.8"
    with pytest.raises(BlockedDestinationError):
        await guard.resolve("169.254.169.254", 80)


async def test_allowed_internal_host_is_not_pinned() -> None:
    guard = EgressGuard(["Webhook-Receiver"], resolver=resolver({}))
    assert await guard.resolve("webhook-receiver", 8080) is None
