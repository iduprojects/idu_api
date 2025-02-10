"""Dependency injection middleware is defined here."""

from asyncio import Lock
from typing import Any, Protocol

import structlog
from fastapi import FastAPI, Request
from starlette.middleware.base import BaseHTTPMiddleware

from idu_api.common.db.connection.manager import PostgresConnectionManager


class DependencyInitializer(Protocol):  # pylint: disable=too-few-public-methods
    def __call__(self, conn: PostgresConnectionManager, **kwargs: Any) -> Any: ...


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
        for dependency, init in self._dependencies.items():
            setattr(request.state, dependency, init(self._connection_manager, logger=logger))
        return await call_next(request)
