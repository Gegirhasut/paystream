"""SSRF protection for outgoing webhooks.

`webhook_url` comes from the client, but the request leaves from inside our
network: `http://10.0.0.5/`, `http://postgres:5432/` or the cloud metadata
address `http://169.254.169.254/` would reach internal services. So every
address the host resolves to must be public, and the request then goes to
exactly the checked address (see HttpWebhookSender): a second DNS answer
cannot swap in an internal one (DNS rebinding).
"""

import asyncio
import ipaddress
import socket
from collections.abc import Awaitable, Callable, Iterable

Resolver = Callable[[str, int], Awaitable[list[str]]]


class BlockedDestinationError(Exception):
    """The webhook host is, or resolves to, a non-public address."""


def is_public_address(address: str) -> bool:
    ip = ipaddress.ip_address(address.split("%", 1)[0])  # drop an IPv6 zone id
    if isinstance(ip, ipaddress.IPv6Address) and ip.ipv4_mapped is not None:
        ip = ip.ipv4_mapped  # ::ffff:127.0.0.1 is loopback
    # is_global rules out private, loopback, link-local, CGNAT, reserved, ...;
    # multicast counts as global but is no webhook destination either.
    return ip.is_global and not ip.is_multicast


def is_ip_literal(host: str) -> bool:
    try:
        ipaddress.ip_address(host)
    except ValueError:
        return False
    return True


async def system_resolver(host: str, port: int) -> list[str]:
    infos = await asyncio.get_running_loop().getaddrinfo(host, port, type=socket.SOCK_STREAM)
    return [str(info[4][0]) for info in infos]


class EgressGuard:
    """Decides where a webhook may be sent.

    `allowed_internal_hosts` are trusted host names that may resolve to internal
    addresses (in docker-compose: the demo `webhook-receiver`).
    """

    def __init__(
        self, allowed_internal_hosts: Iterable[str] = (), resolver: Resolver = system_resolver
    ) -> None:
        self._allowed = {host.lower() for host in allowed_internal_hosts}
        self._resolver = resolver

    async def resolve(self, host: str, port: int) -> str | None:
        """Returns the public IP to connect to, or None for an allowed internal host.

        `host` is the ASCII (punycode) form. Raises BlockedDestinationError.
        """
        host = host.lower()
        if host in self._allowed:
            return None
        if is_ip_literal(host):
            addresses = [host]
        else:
            addresses = await self._resolver(host, port)
        if not addresses:
            raise BlockedDestinationError(f"{host} resolves to no address")
        # All of them: an attacker's DNS may answer with a public and an internal one.
        for address in addresses:
            if not is_public_address(address):
                raise BlockedDestinationError(f"{host} resolves to a non-public address {address}")
        return addresses[0]
