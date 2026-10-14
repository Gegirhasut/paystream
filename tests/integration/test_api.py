import asyncio
import json
import uuid

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.api.main import create_app
from app.api.routing import MAX_BODY_BYTES
from app.config import Settings
from app.messaging.topology import PAYMENTS_NEW_ROUTING_KEY
from app.models import OutboxEvent, Payment
from app.schemas.payment import MAX_METADATA_BYTES
from tests.factories import payment_payload

UNREACHABLE_DB = "postgresql+asyncpg://payments:payments@127.0.0.1:1/payments"


def idem() -> dict[str, str]:
    return {"Idempotency-Key": str(uuid.uuid4())}


async def post_raw(client: AsyncClient, **raw_fields: str):
    """POST with fields inserted verbatim, e.g. a JSON number that float cannot hold."""
    payload = payment_payload(**{name: f"@{name}@" for name in raw_fields})
    body = json.dumps(payload)
    for name, raw in raw_fields.items():
        body = body.replace(json.dumps(f"@{name}@"), raw)
    return await client.post(
        "/api/v1/payments",
        content=body,
        headers={**idem(), "Content-Type": "application/json"},
    )


async def count(session_factory: async_sessionmaker[AsyncSession], model: type) -> int:
    async with session_factory() as session:
        return await session.scalar(select(func.count()).select_from(model))


class TestAuth:
    async def test_missing_api_key(self, client: AsyncClient) -> None:
        response = await client.post(
            "/api/v1/payments", json=payment_payload(), headers={**idem(), "X-API-Key": ""}
        )
        assert response.status_code == 401

    async def test_wrong_api_key_on_get(self, client: AsyncClient) -> None:
        response = await client.get(
            f"/api/v1/payments/{uuid.uuid4()}", headers={"X-API-Key": "wrong"}
        )
        assert response.status_code == 401

    async def test_non_ascii_api_key_is_rejected(self, client: AsyncClient) -> None:
        response = await client.get(
            f"/api/v1/payments/{uuid.uuid4()}", headers={"X-API-Key": "ключ".encode()}
        )
        assert response.status_code == 401

    async def test_health_is_public(self, client: AsyncClient) -> None:
        response = await client.get("/health", headers={"X-API-Key": ""})
        assert response.status_code == 200
        assert response.json() == {"status": "ok"}

    async def test_health_reports_unavailable_database(self, settings: Settings) -> None:
        app = create_app(settings.model_copy(update={"database_url": UNREACHABLE_DB}))
        async with (
            app.router.lifespan_context(app),
            AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as client,
        ):
            response = await client.get("/health")
        assert response.status_code == 503
        assert response.json() == {"status": "unavailable"}


class TestCreatePayment:
    async def test_accepted(
        self, client: AsyncClient, session_factory: async_sessionmaker[AsyncSession]
    ) -> None:
        response = await client.post("/api/v1/payments", json=payment_payload(), headers=idem())

        assert response.status_code == 202
        body = response.json()
        assert set(body) == {"payment_id", "status", "created_at"}
        assert body["status"] == "pending"

        async with session_factory() as session:
            payment = await session.get(Payment, uuid.UUID(body["payment_id"]))
            events = (await session.scalars(select(OutboxEvent))).all()

        assert payment is not None
        assert payment.metadata_ == {"order_id": 42, "customer": {"email": "user@example.com"}}
        # The event is stored in the same transaction as the payment.
        assert len(events) == 1
        event = events[0]
        assert event.aggregate_id == payment.id
        assert event.routing_key == PAYMENTS_NEW_ROUTING_KEY
        assert event.published_at is None
        assert event.payload["payment_id"] == str(payment.id)
        assert event.payload["event_id"] == str(event.id)

    async def test_idempotency_key_is_required(self, client: AsyncClient) -> None:
        response = await client.post("/api/v1/payments", json=payment_payload())
        assert response.status_code == 422

    async def test_same_key_same_body_returns_original_payment(
        self, client: AsyncClient, session_factory: async_sessionmaker[AsyncSession]
    ) -> None:
        headers = idem()
        first = await client.post("/api/v1/payments", json=payment_payload(), headers=headers)
        second = await client.post("/api/v1/payments", json=payment_payload(), headers=headers)

        assert first.status_code == second.status_code == 202
        assert first.json() == second.json()
        assert "Idempotent-Replayed" not in first.headers
        assert second.headers["Idempotent-Replayed"] == "true"
        assert await count(session_factory, Payment) == 1
        assert await count(session_factory, OutboxEvent) == 1

    async def test_equivalent_amount_formats_are_the_same_request(
        self, client: AsyncClient
    ) -> None:
        headers = idem()
        responses = [
            await client.post(
                "/api/v1/payments", json=payment_payload(amount=amount), headers=headers
            )
            for amount in ("10.5", "10.50", 10.5, 10)
        ]
        assert [r.status_code for r in responses] == [202, 202, 202, 409]
        assert len({r.json()["payment_id"] for r in responses[:3]}) == 1

    async def test_same_key_different_body_conflicts(
        self, client: AsyncClient, session_factory: async_sessionmaker[AsyncSession]
    ) -> None:
        headers = idem()
        await client.post("/api/v1/payments", json=payment_payload(), headers=headers)
        response = await client.post(
            "/api/v1/payments", json=payment_payload(amount="1.00"), headers=headers
        )

        assert response.status_code == 409
        assert await count(session_factory, Payment) == 1

    async def test_concurrent_requests_with_same_key_create_one_payment(
        self, client: AsyncClient, session_factory: async_sessionmaker[AsyncSession]
    ) -> None:
        headers = idem()
        responses = await asyncio.gather(
            *(
                client.post("/api/v1/payments", json=payment_payload(), headers=headers)
                for _ in range(5)
            )
        )

        assert {r.status_code for r in responses} == {202}
        assert len({r.json()["payment_id"] for r in responses}) == 1
        assert await count(session_factory, Payment) == 1
        assert await count(session_factory, OutboxEvent) == 1

    @pytest.mark.parametrize(
        "payload",
        [
            pytest.param(payment_payload(amount="0"), id="zero-amount"),
            pytest.param(payment_payload(amount="-5"), id="negative-amount"),
            pytest.param(payment_payload(amount="1.001"), id="sub-cent-amount"),
            pytest.param(payment_payload(amount="1" * 17 + ".00"), id="amount-too-large"),
            pytest.param(payment_payload(amount="NaN"), id="nan-amount"),
            pytest.param(payment_payload(amount="Infinity"), id="infinite-amount"),
            pytest.param(payment_payload(amount="10_00"), id="underscore-amount"),
            pytest.param(payment_payload(amount=" 10.00"), id="padded-amount"),
            pytest.param(payment_payload(amount="+10.00"), id="signed-amount"),
            pytest.param(payment_payload(amount="1e2"), id="exponent-string-amount"),
            pytest.param(payment_payload(amount="\u0661\u0660"), id="non-ascii-digits-amount"),
            pytest.param(payment_payload(amount="10."), id="trailing-dot-amount"),
            pytest.param(payment_payload(currency="GBP"), id="unsupported-currency"),
            pytest.param(payment_payload(currency="rub"), id="lowercase-currency"),
            pytest.param(payment_payload(webhook_url="not-a-url"), id="invalid-url"),
            pytest.param(payment_payload(webhook_url="ftp://a.b/c"), id="non-http-url"),
            pytest.param(payment_payload(metadata="string"), id="metadata-not-object"),
            pytest.param(
                payment_payload(metadata={"blob": "x" * MAX_METADATA_BYTES}), id="metadata-too-big"
            ),
            pytest.param(payment_payload(description="x" * 1025), id="description-too-long"),
            pytest.param(payment_payload(unknown_field=1), id="unknown-field"),
            pytest.param(
                {k: v for k, v in payment_payload().items() if k != "amount"}, id="missing-amount"
            ),
        ],
    )
    async def test_validation(
        self,
        client: AsyncClient,
        session_factory: async_sessionmaker[AsyncSession],
        payload: dict,
    ) -> None:
        response = await client.post("/api/v1/payments", json=payload, headers=idem())

        assert response.status_code == 422
        assert await count(session_factory, Payment) == 0

    async def test_max_amount_is_accepted(self, client: AsyncClient) -> None:
        response = await client.post(
            "/api/v1/payments", json=payment_payload(amount="9" * 16 + ".99"), headers=idem()
        )
        assert response.status_code == 202

    @pytest.mark.parametrize(
        ("raw", "stored"),
        [
            ("1234567890123456.78", "1234567890123456.78"),  # float would give ...456.8
            ("9999999999999999.99", "9999999999999999.99"),  # float would give 1e16
            ("0.1", "0.10"),
            ("100", "100.00"),
            ("1e2", "100.00"),
        ],
    )
    async def test_json_number_amount_is_stored_exactly(
        self, client: AsyncClient, raw: str, stored: str
    ) -> None:
        response = await post_raw(client, amount=raw)
        assert response.status_code == 202

        details = await client.get(f"/api/v1/payments/{response.json()['payment_id']}")
        assert details.json()["amount"] == stored

    @pytest.mark.parametrize(
        "fields",
        [
            pytest.param({"amount": "NaN"}, id="nan-amount"),
            pytest.param({"amount": "Infinity"}, id="infinite-amount"),
            pytest.param({"amount": "-Infinity"}, id="negative-infinite-amount"),
            pytest.param({"amount": "10.001"}, id="sub-cent-amount"),
            pytest.param({"amount": "10000000000000000"}, id="amount-too-large"),
            pytest.param({"metadata": '{"rate": NaN}'}, id="nan-in-metadata"),
            pytest.param({"metadata": '{"a": [Infinity]}'}, id="infinity-in-metadata"),
            pytest.param({"metadata": '{"big": 1e400}'}, id="out-of-range-in-metadata"),
            pytest.param({"metadata": '{"big": 1.8e308}'}, id="float-overflow-in-metadata"),
            pytest.param({"amount": "1e999999999"}, id="huge-exponent-amount"),
            pytest.param({"amount": '"1.00", "amount": "1000.00"'}, id="duplicate-key"),
            pytest.param({"metadata": '{"a": 1, "a": 2}'}, id="duplicate-key-in-metadata"),
            pytest.param({"description": '"a\\u0000b"'}, id="nul-in-description"),
            pytest.param({"description": '"\\ud800"'}, id="lone-surrogate-in-description"),
            pytest.param({"metadata": '{"k": "a\\u0000b"}'}, id="nul-in-metadata"),
            pytest.param({"metadata": '{"a\\u0000": 1}'}, id="nul-in-metadata-key"),
            pytest.param({"metadata": '{"k": "\\udfff"}'}, id="lone-surrogate-in-metadata"),
            pytest.param({"metadata": '{"a":' * 40 + "1" + "}" * 40}, id="deep-metadata"),
            pytest.param({"metadata": '{"a":' * 5000 + "1" + "}" * 5000}, id="very-deep-metadata"),
            pytest.param({"metadata": '{"a":' + "[" * 900 + "]" * 900 + "}"}, id="deep-list"),
            pytest.param({"metadata": '{"a": ' + "9" * 5000 + "}"}, id="over-long-integer"),
        ],
    )
    async def test_raw_json_validation(
        self,
        client: AsyncClient,
        session_factory: async_sessionmaker[AsyncSession],
        fields: dict[str, str],
    ) -> None:
        response = await post_raw(client, **fields)

        assert response.status_code == 422
        assert await count(session_factory, Payment) == 0

    async def test_reasonable_nesting_and_unicode_are_accepted(self, client: AsyncClient) -> None:
        metadata = {"a": {"b": {"c": [[["deep enough"]]]}}, "name": "Заказ №42 🚀"}
        created = await client.post(
            "/api/v1/payments", json=payment_payload(metadata=metadata), headers=idem()
        )
        assert created.status_code == 202

        details = await client.get(f"/api/v1/payments/{created.json()['payment_id']}")
        assert details.json()["metadata"] == metadata

    async def test_metadata_numbers_stay_numbers(self, client: AsyncClient) -> None:
        metadata = {"rate": 1.5, "count": 3, "nested": [0.25, {"x": -2.5}], "flag": True}
        created = await client.post(
            "/api/v1/payments", json=payment_payload(metadata=metadata), headers=idem()
        )

        details = await client.get(f"/api/v1/payments/{created.json()['payment_id']}")
        assert details.json()["metadata"] == metadata

    async def test_malformed_json(self, client: AsyncClient) -> None:
        response = await client.post(
            "/api/v1/payments",
            content="{not json",
            headers={**idem(), "Content-Type": "application/json"},
        )
        assert response.status_code == 422

    async def test_invalid_utf8_body(self, client: AsyncClient) -> None:
        body = json.dumps(payment_payload(description="@")).encode().replace(b"@", b"\xff")
        response = await client.post(
            "/api/v1/payments",
            content=body,
            headers={**idem(), "Content-Type": "application/json"},
        )
        assert response.status_code == 422

    async def test_oversized_body_is_rejected(
        self, client: AsyncClient, session_factory: async_sessionmaker[AsyncSession]
    ) -> None:
        payload = payment_payload(description="x" * (MAX_BODY_BYTES + 1))
        response = await client.post("/api/v1/payments", json=payload, headers=idem())

        assert response.status_code == 413
        assert await count(session_factory, Payment) == 0

    @pytest.mark.parametrize("key", ["", "k" * 256], ids=["empty", "too-long"])
    async def test_invalid_idempotency_key(self, client: AsyncClient, key: str) -> None:
        response = await client.post(
            "/api/v1/payments", json=payment_payload(), headers={"Idempotency-Key": key}
        )
        assert response.status_code == 422


class TestGetPayment:
    async def test_details(self, client: AsyncClient) -> None:
        created = await client.post(
            "/api/v1/payments", json=payment_payload(currency="EUR"), headers=idem()
        )
        payment_id = created.json()["payment_id"]

        response = await client.get(f"/api/v1/payments/{payment_id}")

        assert response.status_code == 200
        body = response.json()
        assert body["id"] == payment_id
        assert body["amount"] == "1500.50"
        assert body["currency"] == "EUR"
        assert body["description"] == "Order #42"
        assert body["metadata"] == {"order_id": 42, "customer": {"email": "user@example.com"}}
        assert body["status"] == "pending"
        assert body["webhook_url"] == "https://merchant.example.com/webhooks/payments"
        assert body["processed_at"] is None
        assert body["created_at"]

    async def test_not_found(self, client: AsyncClient) -> None:
        response = await client.get(f"/api/v1/payments/{uuid.uuid4()}")
        assert response.status_code == 404

    async def test_invalid_id(self, client: AsyncClient) -> None:
        response = await client.get("/api/v1/payments/not-a-uuid")
        assert response.status_code == 422
