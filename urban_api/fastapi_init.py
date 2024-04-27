import traceback
import itertools

from loguru import logger
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

from .version import VERSION, LAST_UPDATE
from .db.connection.session import SessionManager
from .endpoints import list_of_routes
from .config.app_settings_global import app_settings


def bind_routes(application: FastAPI, prefix: str) -> None:
    """
    Bind all routes to application.
    """
    for route in list_of_routes:
        application.include_router(route, prefix=(prefix if "/" not in {r.path for r in route.routes} else ""))


def get_app(prefix: str = "/api") -> FastAPI:
    """
    Create application and all dependable objects.
    """
    description = "This is a simple note taking service"

    application = FastAPI(
        title="Noted API",
        description=description,
        docs_url="/api/docs",
        openapi_url="/api/openapi",
        version=f"{VERSION} ({LAST_UPDATE})",
    )
    bind_routes(application, prefix)

    origins = ["*"]

    application.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    return application


app = get_app()


@app.exception_handler(Exception)
async def internal_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """
    Function that handles exceptions to become http response code 500 - Internal Server Error.

    If debug is activated in app configuration, then stack trace is returned, otherwise only a generic error message.
    Message is sent to logger error stream anyway.
    """
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
    if app_settings.debug:
        return JSONResponse(
            {
                "error": str(exc),
                "error_type": str(type(exc)),
                "path": request.url.path,
                "params": request.url.query,
                "trace": list(
                    itertools.chain.from_iterable(
                        map(lambda x: x.split("\n"), traceback.format_tb(exc.__traceback__)))
                ),
            },
            status_code=500,
        )
    return JSONResponse({"code": 500, "message": "exception occured"}, status_code=500)


@app.on_event("startup")
async def startup_event():
    """
    Function that runs on an application startup. Database connection pool is initialized here.
    """
    await SessionManager().refresh()


@app.on_event("shutdown")
async def shutdown_event():
    """
    Function that runs on an application shutdown. Database connection pool is destructed here.
    """
    await SessionManager().shutdown()
