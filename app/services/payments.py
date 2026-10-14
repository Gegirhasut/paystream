import hashlib
import json
import uuid
from dataclasses import dataclass

from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.messaging.topology import PAYMENTS_NEW_ROUTING_KEY
from app.models import OutboxEvent, Payment, PaymentStatus
from app.repositories.outbox import OutboxRepository
from app.repositories.payments import PaymentRepository
from app.schemas.events import PaymentCreatedEvent
from app.schemas.payment import PaymentCreate
from app.services.exceptions import IdempotencyConflictError, PaymentNotFoundError

PAYMENT_CREATED_EVENT = "payment.created"


@dataclass(frozen=True, slots=True)
class CreatePaymentResult:
    payment: Payment
    created: bool


def payment_created_event(payment: Payment) -> OutboxEvent:
    """Outbox event that makes the consumer process the payment."""
    event = PaymentCreatedEvent(
        event_id=uuid.uuid4(),
        payment_id=payment.id,
        amount=payment.amount,
        currency=payment.currency,
    )
    return OutboxEvent(
        id=event.event_id,
        aggregate_type="payment",
        aggregate_id=payment.id,
        event_type=PAYMENT_CREATED_EVENT,
        routing_key=PAYMENTS_NEW_ROUTING_KEY,
        payload=event.model_dump(mode="json"),
    )


def request_fingerprint(data: PaymentCreate) -> str:
    canonical = json.dumps(data.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


class PaymentService:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session
        self._payments = PaymentRepository(session)
        self._outbox = OutboxRepository(session)

    async def get(self, payment_id: uuid.UUID) -> Payment:
        payment = await self._payments.get(payment_id)
        if payment is None:
            raise PaymentNotFoundError(payment_id)
        return payment

    async def create(self, data: PaymentCreate, idempotency_key: str) -> CreatePaymentResult:
        fingerprint = request_fingerprint(data)

        existing = await self._payments.get_by_idempotency_key(idempotency_key)
        if existing is not None:
            return self._replay(existing, fingerprint)

        payment = Payment(
            id=uuid.uuid4(),
            amount=data.amount,
            currency=data.currency,
            description=data.description,
            metadata_=data.metadata,
            status=PaymentStatus.PENDING,
            idempotency_key=idempotency_key,
            request_hash=fingerprint,
            webhook_url=str(data.webhook_url),
        )
        # Payment and event are committed atomically; the relay publishes the event.
        self._payments.add(payment)
        self._outbox.add(payment_created_event(payment))
        try:
            await self._session.commit()
        except IntegrityError:
            # A concurrent request with the same key won the race.
            await self._session.rollback()
            existing = await self._payments.get_by_idempotency_key(idempotency_key)
            if existing is None:
                raise
            return self._replay(existing, fingerprint)

        return CreatePaymentResult(payment=payment, created=True)

    @staticmethod
    def _replay(existing: Payment, fingerprint: str) -> CreatePaymentResult:
        if existing.request_hash != fingerprint:
            raise IdempotencyConflictError(existing.idempotency_key)
        return CreatePaymentResult(payment=existing, created=False)
