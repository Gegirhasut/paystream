"""Lossless JSON parsing for money amounts.

The problem: FastAPI parses the request body with `json.loads`, which turns
every JSON number with a fraction into a float before Pydantic sees it. A float
holds only ~15-17 significant digits, so a valid amount is silently rounded:

    {"amount": 1234567890123456.78}  ->  stored as 1234567890123456.80
    {"amount": 9999999999999999.99}  ->  becomes 1e16 and fails validation

No error is raised, the client just gets charged a different sum. Sending the
amount as a string ("1234567890123456.78") was always exact; numbers were not.
Pydantic's own JSON parser (`model_validate_json`) has the same issue.

The fix: routes using `DecimalJSONRoute` (see app/api/v1/payments.py) parse
fractions straight into Decimal, so a float never appears. Metadata numbers are
converted back to plain floats in app/schemas/payment.py.

The same parser also rejects, as malformed JSON (422), input that otherwise
ends in a 500: duplicate keys (`{"amount": "1", "amount": "1000"}` would
silently keep the last value), strings PostgreSQL cannot store (NUL, unpaired
surrogates) and nesting deep enough to exhaust the recursion limit.
"""

import json
from collections.abc import Callable, Coroutine
from decimal import Decimal
from typing import Any

from fastapi import HTTPException, Request, Response, status
from fastapi.routing import APIRoute

# Numbers beyond the float range are rejected while parsing: no field accepts
# them anyway, and FastAPI renders a rejected Decimal in the 422 response via
# int(Decimal(...)), so "1e999999999" would build a billion-digit integer and
# hang the worker.
MAX_EXPONENT = 308


def _parse_float(value: str) -> Decimal:
    number = Decimal(value)
    if number.adjusted() > MAX_EXPONENT:
        raise json.JSONDecodeError("Number out of range", value, 0)
    return number


# `json.loads` accepts the non-standard NaN/Infinity literals by default. As a
# Decimal they also break FastAPI's 422 rendering (500 instead of 422), so they
# are reported as malformed JSON instead.
def _reject_constant(name: str) -> None:
    raise json.JSONDecodeError(f"{name} is not a valid JSON number", name, 0)


def _unique_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result = dict(pairs)
    if len(result) != len(pairs):
        raise json.JSONDecodeError("Duplicate keys are not allowed", "", 0)
    return result


# Far more than any real payload needs, far less than the recursion limit.
MAX_DEPTH = 32


def _check_document(document: Any) -> None:
    """Rejects deep nesting and strings PostgreSQL rejects (it is a 500 otherwise).

    Iterative on purpose: a recursive walk would itself hit the recursion limit.
    """
    stack = [(document, 0)]
    while stack:
        value, depth = stack.pop()
        if isinstance(value, str):
            # NUL is refused by TEXT and JSONB; lone surrogates cannot be UTF-8 encoded.
            if "\x00" in value or (not value.isascii() and not _is_utf8(value)):
                raise json.JSONDecodeError("Strings must not contain NUL or lone surrogates", "", 0)
        elif isinstance(value, dict | list):
            if depth >= MAX_DEPTH:
                raise json.JSONDecodeError(f"Nesting deeper than {MAX_DEPTH} levels", "", 0)
            children = [*value, *value.values()] if isinstance(value, dict) else value
            stack.extend((child, depth + 1) for child in children)


def _is_utf8(value: str) -> bool:
    try:
        value.encode()
    except UnicodeEncodeError:
        return False
    return True


# Uvicorn reads a body of any size into memory. This is far above any valid
# request (metadata is capped at 16 KB) and keeps a huge one from exhausting RAM.
MAX_BODY_BYTES = 1024 * 1024


class DecimalJSONRequest(Request):
    """Parses JSON numbers with a fraction as Decimal instead of float.

    With float, `1234567890123456.78` silently becomes `1234567890123456.8`,
    which is unacceptable for money. NaN, Infinity, duplicate keys, NUL and
    too deep nesting are rejected with 422 (see the module docstring).
    """

    async def body(self) -> bytes:
        if not hasattr(self, "_body"):
            chunks: list[bytes] = []
            size = 0
            async for chunk in self.stream():
                size += len(chunk)
                if size > MAX_BODY_BYTES:
                    raise HTTPException(
                        status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                        detail=f"Request body must not exceed {MAX_BODY_BYTES} bytes",
                    )
                chunks.append(chunk)
            self._body = b"".join(chunks)
        return self._body

    async def json(self) -> Any:
        if not hasattr(self, "_json"):
            try:
                document = json.loads(
                    await self.body(),
                    parse_float=_parse_float,
                    parse_constant=_reject_constant,
                    object_pairs_hook=_unique_keys,
                )
            except json.JSONDecodeError:
                raise
            except RecursionError:
                raise json.JSONDecodeError("Nesting is too deep", "", 0) from None
            except ValueError:
                # Invalid UTF-8, an integer over Python's 4300-digit limit: 422 like
                # any other malformed JSON, not FastAPI's generic 400.
                raise json.JSONDecodeError("Invalid JSON document", "", 0) from None
            _check_document(document)
            self._json = document
        return self._json


class DecimalJSONRoute(APIRoute):
    def get_route_handler(self) -> Callable[[Request], Coroutine[Any, Any, Response]]:
        handler = super().get_route_handler()

        async def decimal_json_handler(request: Request) -> Response:
            return await handler(DecimalJSONRequest(request.scope, request.receive))

        return decimal_json_handler
