"""Tiny merchant stub for local demos: records the webhooks it receives.

POST /webhooks          -> 200, stores the payload
POST /webhooks/flaky    -> 500 for the first two calls per payment, then 200
POST /webhooks/fail     -> always 500 (the message ends up in the DLQ)
GET  /webhooks          -> everything received so far

Like a real merchant it verifies the signature first: a webhook with a missing,
wrong or stale signature gets 401.
"""

import hashlib
import hmac
import json
import logging
import os
import time
from collections import Counter
from typing import Any

from fastapi import FastAPI, Request, Response, status

logger = logging.getLogger("webhook_receiver")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")

app = FastAPI(title="Webhook receiver (demo)")
received: list[dict[str, Any]] = []
calls: Counter[str] = Counter()

FLAKY_FAILURES = 2
SECRET = os.environ.get("WEBHOOK_SECRET", "")
MAX_AGE_SECONDS = 300  # older signatures are treated as replays


def signature_is_valid(request: Request, body: bytes) -> bool:
    timestamp = request.headers.get("X-Webhook-Timestamp", "")
    signature = request.headers.get("X-Webhook-Signature", "")
    if not SECRET or not timestamp.isdigit():
        return False
    if abs(time.time() - int(timestamp)) > MAX_AGE_SECONDS:
        return False
    digest = hmac.new(SECRET.encode(), timestamp.encode() + b"." + body, hashlib.sha256)
    return hmac.compare_digest(f"sha256={digest.hexdigest()}", signature)


async def _receive(request: Request, response: Response, endpoint: str, code: int) -> dict:
    body = await request.body()
    if not signature_is_valid(request, body):
        code = status.HTTP_401_UNAUTHORIZED
    payload = json.loads(body)
    received.append({"endpoint": endpoint, "status_code": code, "payload": payload})
    logger.info("%s -> %s: %s", endpoint, code, payload)
    response.status_code = code
    return {"status": "received" if code == status.HTTP_200_OK else "error"}


@app.post("/webhooks")
async def ok(request: Request, response: Response) -> dict[str, str]:
    return await _receive(request, response, "ok", status.HTTP_200_OK)


@app.post("/webhooks/flaky")
async def flaky(request: Request, response: Response) -> dict[str, str]:
    key = request.headers.get("X-Payment-Id", "")
    calls[key] += 1
    code = status.HTTP_500_INTERNAL_SERVER_ERROR if calls[key] <= FLAKY_FAILURES else 200
    return await _receive(request, response, "flaky", code)


@app.post("/webhooks/fail")
async def fail(request: Request, response: Response) -> dict[str, str]:
    return await _receive(request, response, "fail", status.HTTP_500_INTERNAL_SERVER_ERROR)


@app.get("/webhooks")
async def list_received() -> list[dict[str, Any]]:
    return received
