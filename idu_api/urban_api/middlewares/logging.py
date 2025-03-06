import time
import uuid
from http.client import HTTPException

import structlog
from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware

from idu_api.urban_api.dto.users.users import UserDTO
from idu_api.urban_api.exceptions.base import IduApiError
from idu_api.urban_api.prometheus import metrics


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

        metrics.REQUESTS_COUNTER.labels(method=request.method, path=request.url.path).inc(1)
        time_begin = time.monotonic_ns()
        try:
            result = await call_next(request)

            time_finish = time.monotonic_ns()
            duration_seconds = (time_finish - time_begin) / 1e9
            await logger.ainfo("request handled successfully", time_consumed=round(duration_seconds, 3))
            metrics.REQUEST_TIME.observe(duration_seconds)
            metrics.SUCCESS_COUNTER.labels(status_code=result.status_code).inc(1)
            return result
        except Exception as exc:
            time_finish = time.monotonic_ns()
            duration_seconds = (time_finish - time_begin) / 1e9
            metrics.REQUEST_TIME.observe(duration_seconds)
            metrics.ERRORS_COUNTER.labels(
                method=request.method,
                path=request.url.path,
                error_type=str(type(exc)),
            ).inc(1)

            if isinstance(exc, (IduApiError, HTTPException)):
                log_func = logger.aerror
            else:
                log_func = logger.aexception
            await log_func("failed to handle request", time_consumed=round(duration_seconds, 3))
            raise
