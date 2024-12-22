import time
import uuid

import structlog
from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware

from idu_api.urban_api.dto.users.users import UserDTO
from idu_api.urban_api.exceptions.base import IduApiError


class LoggingMiddleware(BaseHTTPMiddleware):  # pylint: disable=too-few-public-methods
    """Middleware for logging requests. Using `state.user` data and `state.logger` to log details."""

    def __init__(self, app):
        super().__init__(app)

    async def dispatch(self, request: Request, call_next):
        request_id = uuid.uuid4()
        logger: structlog.stdlib.BoundLogger = request.app.state.logger
        logger = logger.bind(request_id=str(request_id))
        request.state.logger = logger
        try:
            user: UserDTO | None = request.state.user

            await logger.ainfo(
                "handling request",
                client=request.client.host,
                path_params=request.path_params,
                method=request.method,
                url=str(request.url),
                user=user,
            )
        except Exception:  # pylint: disable=broad-except
            await logger.aexception("error on logging request")

        time_begin = time.monotonic_ns()
        try:
            result = await call_next(request)

            time_finish = time.monotonic_ns()
            await logger.ainfo("request handled successfully", time_consumed=round((time_finish - time_begin) / 1e9, 3))

            return result
        except Exception as exc:  # pylint: disable=broad-except
            time_finish = time.monotonic_ns()

            if isinstance(exc, IduApiError):
                log_func = logger.aerror
            else:
                log_func = logger.aexception
            await log_func("failed to handle request", time_consumed=round((time_finish - time_begin) / 1e9, 3))
            raise
