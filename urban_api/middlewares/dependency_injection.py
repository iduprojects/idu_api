"""Dependency indejction middleware is defined here."""

from asyncio import Lock
from typing import Any, Callable

from fastapi import FastAPI, Request
from sqlalchemy.ext.asyncio import AsyncConnection
from starlette.middleware.base import BaseHTTPMiddleware

from urban_api.db.connection.manager import PostgresConnectionManager


class PassServicesDependencies(BaseHTTPMiddleware):
    def __init__(
        self,
        app: FastAPI,
        connection_manager: PostgresConnectionManager,
        **dependencies: dict[str, Callable[[AsyncConnection], Any]],
    ):
        super().__init__(app)
        self._connection_manager = connection_manager
        self._dependencies = dependencies

    async def refresh(self):
        async with Lock():
            await self._connection_manager.refresh()

    async def shutdown(self):
        async with Lock():
            await self._connection_manager.shutdown()

    async def dispatch(self, request: Request, call_next):
        get_conn = anext(self._connection_manager.get_connection())
        conn = await get_conn
        request.state.conn = conn  # to be removed after all handler use services

        for dependency, init in self._dependencies.items():
            setattr(request.state, dependency, init(conn))
        return await call_next(request)
