"""Connection manager class and get_connection function are defined here."""

from asyncio import Lock
from contextlib import asynccontextmanager
from itertools import cycle
from typing import Any, AsyncIterator

import structlog
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine

from idu_api.urban_api.config import DBConfig


class PostgresConnectionManager:
    """Connection manager for PostgreSQL database"""

    def __init__(
        self,
        master: DBConfig,
        replicas: list[DBConfig],
        logger: structlog.stdlib.BoundLogger,
        engine_options: dict[str, Any] | None = None,
        application_name: str | None = None,
    ) -> None:
        """Initialize connection manager entity."""
        self._master_engine: AsyncEngine | None = None
        self._replica_engines: list[AsyncEngine] = []
        self._master = master
        self._replicas = replicas
        self._lock = Lock()
        self._logger = logger
        self._engine_options = engine_options or {}
        self._application_name = application_name
        # Iterator for round-robin through replicas
        self._replica_cycle = None

    async def update(
        self,
        master: DBConfig | None = None,
        replicas: list[DBConfig] | None = None,
        logger: structlog.stdlib.BoundLogger | None = None,
        application_name: str | None = None,
        engine_options: dict[str, Any] | None = None,
    ) -> None:
        """Initialize connection manager entity."""
        async with self._lock:
            self._master = master or self._master
            self._replicas = replicas or self._replicas
            self._logger = logger or self._logger
            self._application_name = application_name or self._application_name
            self._engine_options = engine_options or self._engine_options

            if self.initialized:
                await self.refresh()

    @property
    def initialized(self) -> bool:
        return self._master_engine is not None

    async def refresh(self) -> None:
        """(Re-)create connection engine."""
        await self.shutdown()

        await self._logger.ainfo(
            "creating postgres master connection pool",
            max_size=self._master.pool_size,
            user=self._master.user,
            host=self._master.host,
            port=self._master.port,
            database=self._master.database,
        )
        self._master_engine = create_async_engine(
            f"postgresql+asyncpg://{self._master.user}:{self._master.password}@{self._master.host}"
            f":{self._master.port}/{self._master.database}",
            future=True,
            pool_size=max(1, self._master.pool_size - 5),
            max_overflow=5,
            **self._engine_options,
        )
        try:
            async with self._master_engine.connect() as conn:
                cur = await conn.execute(select(1))
                assert cur.fetchone()[0] == 1
        except Exception as exc:
            self._master_engine = None
            raise RuntimeError("something wrong with database connection, aborting") from exc

        if len(self._replicas) > 0:
            for replica in self._replicas:
                await self._logger.ainfo(
                    "creating postgres readonly connection pool",
                    max_size=replica.pool_size,
                    user=replica.user,
                    host=replica.host,
                    port=replica.port,
                    database=replica.database,
                )
                replica_engine = create_async_engine(
                    f"postgresql+asyncpg://{replica.user}:{replica.password}@{replica.host}:{replica.port}/{replica.database}",
                    future=True,
                    pool_size=max(1, self._master.pool_size - 5),
                    max_overflow=5,
                    **self._engine_options,
                )
                try:
                    async with replica_engine.connect() as conn:
                        cur = await conn.execute(select(1))
                        assert cur.fetchone()[0] == 1
                        self._replica_engines.append(replica_engine)
                except Exception as exc:
                    await replica_engine.dispose()
                    await self._logger.awarning("error connecting to replica", host=replica.host, error=repr(exc))

        if self._replica_engines:
            self._replica_cycle = cycle(self._replica_engines)
        else:
            self._replica_cycle = None
            await self._logger.awarning("no available replicas, read queries will go to the master")

    async def shutdown(self) -> None:
        """Dispose connection pool and deinitialize."""
        if self.initialized:
            async with self._lock:
                if self.initialized:
                    await self._master_engine.dispose()
                self._master_engine = None
        for engine in self._replica_engines:
            await engine.dispose()
        self._replica_engines.clear()

    @asynccontextmanager
    async def get_connection(self) -> AsyncIterator[AsyncConnection]:
        """Get an async connection to the database with read-write ability."""
        if not self.initialized:
            async with self._lock:
                if not self.initialized:
                    await self.refresh()
        async with self._master_engine.connect() as conn:
            if self._application_name is not None:
                await conn.execute(text(f'SET application_name TO "{self._application_name}"'))
                await conn.commit()
            yield conn

    @asynccontextmanager
    async def get_ro_connection(self) -> AsyncIterator[AsyncConnection]:
        """Get an async connection to the database which can be read-only and will attempt to use replica instances
        of the database."""
        if not self.initialized:
            async with self._lock:
                if not self.initialized:
                    await self.refresh()

        # If there are no replicas or cycle is not set up, use master
        if not self._replica_engines or self._replica_cycle is None:
            async with self.get_connection() as conn:
                yield conn
            return

        # Select the next replica (round-robin)
        engine = next(self._replica_cycle)
        try:
            conn = await engine.connect()
            if self._application_name is not None:
                await conn.execute(text(f'SET application_name TO "{self._application_name}"'))
                await conn.commit()
        except Exception as exc:
            await self._logger.awarning(
                "error connecting to replica, falling back to master", error=repr(exc), error_type=type(exc)
            )
            async with self.get_connection() as conn:
                yield conn
            return

        try:
            yield conn
        finally:
            await conn.close()
