import json
import math
import re
import uuid
from datetime import datetime
from decimal import Decimal
from typing import Annotated, Any

from pydantic import AfterValidator, AnyHttpUrl, BaseModel, BeforeValidator, ConfigDict, Field

from app.models import Currency, PaymentStatus

CENTS = Decimal("0.01")
MAX_METADATA_BYTES = 16 * 1024

# Python's Decimal() also takes "1_000", " 10 ", "1e2" and non-ASCII digits, so a
# typo like "10_00" would quietly become 1000.00. Strings must be plain decimals.
AMOUNT_STRING = re.compile(r"[0-9]+(\.[0-9]+)?")


def _check_amount_string(value: Any) -> Any:
    if isinstance(value, str) and not AMOUNT_STRING.fullmatch(value):
        raise ValueError("amount must be a decimal number like 1500.00")
    return value


# Arrives as Decimal both from a JSON string and a JSON number (app/api/routing.py
# keeps numbers away from float). Quantized so that "10.5" and "10.50" are the
# same amount and therefore the same idempotent request, not a 409.
Amount = Annotated[
    Decimal,
    BeforeValidator(_check_amount_string),
    Field(gt=0, max_digits=18, decimal_places=2),
    AfterValidator(lambda value: value.quantize(CENTS)),
]


def _to_json_value(value: Any) -> Any:
    # The API parses fractions as Decimal (see app.api.routing); metadata keeps
    # plain JSON numbers. NaN/Infinity are not valid JSON and JSONB rejects them.
    if isinstance(value, dict):
        return {key: _to_json_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_to_json_value(item) for item in value]
    if isinstance(value, Decimal):
        value = float(value)
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError("metadata must not contain NaN or Infinity")
    return value


def _normalize_metadata(value: dict[str, Any]) -> dict[str, Any]:
    value = _to_json_value(value)
    if len(json.dumps(value, ensure_ascii=False).encode()) > MAX_METADATA_BYTES:
        raise ValueError(f"metadata must not exceed {MAX_METADATA_BYTES} bytes")
    return value


Metadata = Annotated[dict[str, Any], AfterValidator(_normalize_metadata)]


class PaymentCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    amount: Amount = Field(examples=["1500.00"])
    currency: Currency
    description: str = Field(default="", max_length=1024)
    metadata: Metadata = Field(default_factory=dict)
    webhook_url: AnyHttpUrl = Field(examples=["https://merchant.example.com/webhooks/payments"])


class PaymentAccepted(BaseModel):
    payment_id: uuid.UUID
    status: PaymentStatus
    created_at: datetime


class PaymentRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any] = Field(validation_alias="metadata_")
    status: PaymentStatus
    idempotency_key: str
    webhook_url: str
    created_at: datetime
    processed_at: datetime | None
