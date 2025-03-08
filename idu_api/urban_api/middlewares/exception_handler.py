"""Exception handling middleware is defined here."""

import itertools
import traceback
from http.client import HTTPException

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from idu_api.urban_api.exceptions import IduApiError
from idu_api.urban_api.prometheus import metrics
from idu_api.urban_api.utils.logging import get_handler_from_path


class ExceptionHandlerMiddleware(BaseHTTPMiddleware):  # pylint: disable=too-few-public-methods
    """Handle exceptions, so they become http response code 500 - Internal Server Error.

    If debug is activated in app configuration, then stack trace is returned, otherwise only a generic error message.
    Message is sent to logger error stream anyway.
    """

    def __init__(self, app: FastAPI, debug: list[bool]):
        """Passing debug as a list with single element is a hack to be able to change the value
        on the application startup.
        """
        super().__init__(app)
        self._debug = debug

    async def dispatch(self, request: Request, call_next):
        try:
            return await call_next(request)
        except Exception as exc:  # pylint: disable=broad-except
            status_code = 500
            if isinstance(exc, (IduApiError, HTTPException)):
                status_code = getattr(exc, "status_code", 500)

            metrics.ERRORS_COUNTER.labels(
                method=request.method,
                path=get_handler_from_path(request.url.path),
                error_type=type(exc).__name__,
                status_code=status_code,
            ).inc(1)

            if self._debug[0]:
                return JSONResponse(
                    {
                        "error": str(exc),
                        "error_type": type(exc).__name__,
                        "path": request.url.path,
                        "params": request.url.query,
                        "trace": list(
                            itertools.chain.from_iterable(
                                map(lambda x: x.split("\n"), traceback.format_tb(exc.__traceback__))
                            )
                        ),
                    },
                    status_code=status_code,
                )
            return JSONResponse({"code": status_code, "message": "exception occured"}, status_code=status_code)
