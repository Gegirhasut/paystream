import logging


def describe_error(error: BaseException) -> str:
    # str() of some errors (TimeoutError, CancelledError) is empty.
    return f"{type(error).__name__}: {error}" if str(error) else type(error).__name__


def setup_logging(level: str) -> None:
    logging.basicConfig(
        level=level.upper(),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
