"""A frozen worker must not hold row locks forever, a statement must not wait forever."""

import asyncio

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from app.config import Settings
from app.db import create_engine, create_session_factory
from app.models import Payment
from tests.factories import create_payment


@pytest.fixture
async def short_timeouts_engine(settings: Settings, engine: AsyncEngine):
    # model_copy skips validation: these values are far below the production rules.
    fast = settings.model_copy(
        update={"db_idle_in_transaction_timeout": 0.5, "db_command_timeout": 1.0}
    )
    engine = create_engine(fast)
    yield engine
    await engine.dispose()


async def test_session_idle_in_transaction_releases_its_locks(
    short_timeouts_engine: AsyncEngine,
) -> None:
    session_factory = create_session_factory(short_timeouts_engine)
    payment = await create_payment(session_factory)

    frozen = session_factory()
    await frozen.begin()
    await frozen.get(Payment, payment.id, with_for_update=True)
    # The "frozen" worker does nothing; PostgreSQL ends its session.

    async with asyncio.timeout(5), session_factory.begin() as session:
        locked = await session.get(Payment, payment.id, with_for_update=True)
    assert locked is not None

    with pytest.raises(Exception):  # noqa: B017  (the server closed the session)
        await frozen.commit()
    await frozen.close()


async def test_statement_is_bounded(short_timeouts_engine: AsyncEngine) -> None:
    async with short_timeouts_engine.connect() as connection:
        with pytest.raises(TimeoutError):
            await connection.execute(text("SELECT pg_sleep(10)"))
