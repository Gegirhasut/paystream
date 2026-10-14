class ServiceError(Exception):
    pass


class IdempotencyConflictError(ServiceError):
    """The idempotency key was already used with a different request body."""


class PaymentNotFoundError(ServiceError):
    pass
