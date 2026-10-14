from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.config import Settings


def create_engine(settings: Settings) -> AsyncEngine:
    return create_async_engine(
        settings.database_url,
        echo=settings.db_echo,
        pool_size=settings.db_pool_size,
        pool_pre_ping=True,
        # See the settings: neither a frozen worker nor a vanished database can
        # hold a lock or a statement forever.
        connect_args={
            "command_timeout": settings.db_command_timeout,
            "server_settings": {
                "idle_in_transaction_session_timeout": str(
                    int(settings.db_idle_in_transaction_timeout * 1000)
                ),
            },
        },
    )


def create_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False)
