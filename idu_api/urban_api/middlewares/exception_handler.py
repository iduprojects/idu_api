"""Exception handling middleware is defined here."""

import itertools
import traceback

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from idu_api.urban_api.exceptions import IduApiError


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
            error_status = 500
            if isinstance(exc, IduApiError):
                error_status = getattr(exc, "status_code", 500)

            if self._debug[0]:
                return JSONResponse(
                    {
                        "error": str(exc),
                        "error_type": str(type(exc)),
                        "path": request.url.path,
                        "params": request.url.query,
                        "trace": list(
                            itertools.chain.from_iterable(
                                map(lambda x: x.split("\n"), traceback.format_tb(exc.__traceback__))
                            )
                        ),
                    },
                    status_code=error_status,
                )
            return JSONResponse({"code": error_status, "message": "exception occured"}, status_code=error_status)
