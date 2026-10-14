import os
from collections.abc import AsyncIterator, Iterator
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import NullPool

os.environ.setdefault("API_KEY", "test-api-key")
os.environ.setdefault(
    "DATABASE_URL", "postgresql+asyncpg://payments:payments@localhost:5432/payments_test"
)

from app.api.main import create_app
from app.config import Settings

ROOT = Path(__file__).resolve().parent.parent
API_KEY = "test-api-key"


@pytest.fixture(scope="session")
def settings() -> Settings:
    return Settings(
        api_key=API_KEY,
        database_url=os.environ["DATABASE_URL"],
        rabbitmq_url=os.environ.get("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/"),
        processing_min_delay=0,
        processing_max_delay=0,
        retry_base_delay=0.2,
        log_level="WARNING",
    )


@pytest.fixture(scope="session")
def migrated_db(settings: Settings) -> Iterator[None]:
    """Apply real Alembic migrations once: tests double as a migration check."""
    config = Config(str(ROOT / "alembic.ini"))
    config.set_main_option("script_location", str(ROOT / "migrations"))
    config.set_main_option("sqlalchemy.url", settings.database_url)
    config.attributes["configure_logger"] = False
    command.downgrade(config, "base")
    command.upgrade(config, "head")
    yield


@pytest.fixture
async def engine(settings: Settings, migrated_db: None) -> AsyncIterator[AsyncEngine]:
    engine = create_async_engine(settings.database_url, poolclass=NullPool)
    async with engine.begin() as connection:
        await connection.execute(text("TRUNCATE payments, outbox"))
    yield engine
    await engine.dispose()


@pytest.fixture
def session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False)


@pytest.fixture
async def client(settings: Settings, engine: AsyncEngine) -> AsyncIterator[AsyncClient]:
    app = create_app(settings)
    async with (
        app.router.lifespan_context(app),
        AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://testserver",
            headers={"X-API-Key": API_KEY},
        ) as client,
    ):
        yield client
