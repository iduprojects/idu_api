"""Exception handling middleware is defined here."""

import itertools
import traceback

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from loguru import logger
from starlette.middleware.base import BaseHTTPMiddleware


class ExceptionHandlerMiddleware(BaseHTTPMiddleware):  # pylint: disable=too-few-public-methods
    """Handle exceptions, so they become http response code 500 - Internal Server Error.

    If debug is activated in app configuration, then stack trace is returned, otherwise only a generic error message.
    Message is sent to logger error stream anyway.
    """

    def __init__(self, app: FastAPI, debug: bool):
        super().__init__(app)
        self._debug = debug

    async def dispatch(self, request: Request, call_next):
        try:
            return await call_next(request)
        except Exception as exc:  # pylint: disable=broad-except
            if len(error_message := repr(exc)) > 300:
                error_message = f"{error_message[:300]}...({len(error_message) - 300} ommitted)"
            logger.opt(colors=True).error(
                "<cyan>{} {}</cyan> - '<red>{}</red>': {}",
                (f"{request.client.host}:{request.client.port}" if request.client is not None else "<unknown user>"),
                request.method,
                exc,
                error_message,
            )

            logger.debug("{} Traceback:\n{}", error_message, exc, "".join(traceback.format_tb(exc.__traceback__)))
            if self._debug:
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
                    status_code=500,
                )
            return JSONResponse({"code": 500, "message": "exception occured"}, status_code=500)
