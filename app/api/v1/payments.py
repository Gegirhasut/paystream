import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, Response, status

from app.api.deps import get_payment_service
from app.api.routing import DecimalJSONRoute
from app.schemas.payment import PaymentAccepted, PaymentCreate, PaymentRead
from app.services.exceptions import IdempotencyConflictError, PaymentNotFoundError
from app.services.payments import PaymentService

# DecimalJSONRoute: amounts sent as JSON numbers are parsed without float rounding
# (see app/api/routing.py for the details).
router = APIRouter(prefix="/payments", tags=["payments"], route_class=DecimalJSONRoute)

IdempotencyKey = Annotated[
    str,
    Header(
        alias="Idempotency-Key",
        min_length=1,
        max_length=255,
        description="Unique key of the operation; repeating it returns the original payment.",
    ),
]


@router.post(
    "",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=PaymentAccepted,
    responses={
        status.HTTP_409_CONFLICT: {
            "description": "Idempotency key was already used with a different body"
        },
    },
)
async def create_payment(
    data: PaymentCreate,
    idempotency_key: IdempotencyKey,
    response: Response,
    service: Annotated[PaymentService, Depends(get_payment_service)],
) -> PaymentAccepted:
    try:
        result = await service.create(data, idempotency_key)
    except IdempotencyConflictError:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Idempotency-Key has already been used with a different request body",
        ) from None

    if not result.created:
        response.headers["Idempotent-Replayed"] = "true"
    payment = result.payment
    return PaymentAccepted(
        payment_id=payment.id, status=payment.status, created_at=payment.created_at
    )


@router.get(
    "/{payment_id}",
    response_model=PaymentRead,
    responses={status.HTTP_404_NOT_FOUND: {"description": "Payment not found"}},
)
async def get_payment(
    payment_id: uuid.UUID,
    service: Annotated[PaymentService, Depends(get_payment_service)],
) -> PaymentRead:
    try:
        payment = await service.get(payment_id)
    except PaymentNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Payment not found"
        ) from None
    return PaymentRead.model_validate(payment)
