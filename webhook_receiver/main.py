"""Tiny merchant stub for local demos: records the webhooks it receives.

POST /webhooks          -> 200, stores the payload
POST /webhooks/flaky    -> 500 for the first two calls per payment, then 200
POST /webhooks/fail     -> always 500 (the message ends up in the DLQ)
GET  /webhooks          -> everything received so far
"""

import logging
from collections import Counter
from typing import Any

from fastapi import FastAPI, Request, Response, status

logger = logging.getLogger("webhook_receiver")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")

app = FastAPI(title="Webhook receiver (demo)")
received: list[dict[str, Any]] = []
calls: Counter[str] = Counter()

FLAKY_FAILURES = 2


async def _record(request: Request, endpoint: str, response_status: int) -> dict[str, Any]:
    payload = await request.json()
    entry = {"endpoint": endpoint, "status_code": response_status, "payload": payload}
    received.append(entry)
    logger.info("%s -> %s: %s", endpoint, response_status, payload)
    return payload


@app.post("/webhooks")
async def ok(request: Request) -> dict[str, str]:
    await _record(request, "ok", status.HTTP_200_OK)
    return {"status": "received"}


@app.post("/webhooks/flaky")
async def flaky(request: Request, response: Response) -> dict[str, str]:
    payload = await request.json()
    key = str(payload.get("payment_id"))
    calls[key] += 1
    code = status.HTTP_500_INTERNAL_SERVER_ERROR if calls[key] <= FLAKY_FAILURES else 200
    await _record(request, "flaky", code)
    response.status_code = code
    return {"status": "error" if code != 200 else "received"}


@app.post("/webhooks/fail", status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)
async def fail(request: Request) -> dict[str, str]:
    await _record(request, "fail", status.HTTP_500_INTERNAL_SERVER_ERROR)
    return {"status": "error"}


@app.get("/webhooks")
async def list_received() -> list[dict[str, Any]]:
    return received
