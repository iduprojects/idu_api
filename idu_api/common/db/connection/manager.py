"""Connection manager class and get_connection function are defined here."""

from asyncio import Lock
from typing import AsyncIterator

from loguru import logger
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine


class PostgresConnectionManager:
    """Connection manager for PostgreSQL database"""

    def __init__(
        self,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
        pool_size: int = 10,
        application_name: str | None = None,
    ) -> None:
        """Initialize connection manager entity."""
        self._engine: AsyncEngine | None = None
        self._host = host
        self._port = port
        self._database = database
        self._user = user
        self._password = password
        self._pool_size = pool_size
        self._application_name = application_name

    @property
    def initialized(self) -> bool:
        return self._engine is not None

    async def refresh(self) -> None:
        """(Re-)create connection engine."""
        if self._engine is not None:
            await self._engine.dispose()
            self._engine = None

        logger.info(
            "Creating pool with max_size = {} on postgresql://{}@{}:{}/{}",
            self._pool_size,
            self._user,
            self._host,
            self._port,
            self._database,
        )
        self._engine = create_async_engine(
            f"postgresql+asyncpg://{self._user}:{self._password}@{self._host}:{self._port}/{self._database}",
            future=True,
            pool_size=max(1, self._pool_size - 5),
            max_overflow=5,
        )
        try:
            async with self._engine.connect() as conn:
                cur = await conn.execute(select(1))
                assert cur.fetchone()[0] == 1
        except Exception as exc:
            self._engine = None
            raise RuntimeError("something wrong with database connection, aborting") from exc

    async def shutdown(self) -> None:
        """Dispose connection pool and deinitialize."""
        if self._engine is not None:
            async with Lock():
                if self._engine is not None:
                    await self._engine.dispose()
                self._engine = None

    async def get_connection(self) -> AsyncIterator[AsyncConnection]:
        """Get an async connection to the database."""
        if self._engine is None:
            async with Lock():
                if self._engine is None:
                    await self.refresh()
        async with self._engine.connect() as conn:
            if self._application_name is not None:
                await conn.execute(text(f'SET application_name TO "{self._application_name}"'))
            yield conn
