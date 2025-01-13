"""Dependency injection middleware is defined here."""

from asyncio import Lock
from typing import Any, Protocol

import structlog
from fastapi import FastAPI, Request
from sqlalchemy.ext.asyncio import AsyncConnection
from starlette.middleware.base import BaseHTTPMiddleware

from idu_api.common.db.connection.manager import PostgresConnectionManager


class DependencyInitializer(Protocol):
    def __call__(self, conn: AsyncConnection, **kwargs: Any) -> Any: ...


class PassServicesDependenciesMiddleware(BaseHTTPMiddleware):
    """Construct given service objects with a new Postgres connection from pool.
    Services initializer functions must have database connection as first and only positional argument.
    And `logger` should be only required keyword argument.
    """

    def __init__(
        self,
        app: FastAPI,
        connection_manager: PostgresConnectionManager,
        **dependencies: DependencyInitializer,
    ):
        super().__init__(app)
        self._connection_manager = connection_manager
        self._dependencies = dependencies
        self._lock = Lock()

    async def refresh(self):
        async with self._lock:
            await self._connection_manager.refresh()

    async def shutdown(self):
        async with self._lock:
            await self._connection_manager.shutdown()

    async def dispatch(self, request: Request, call_next):
        logger: structlog.stdlib.BoundLogger = request.state.logger
        async with self._connection_manager.get_connection() as conn:
            for dependency, init in self._dependencies.items():
                setattr(request.state, dependency, init(conn, logger=logger))
            return await call_next(request)
