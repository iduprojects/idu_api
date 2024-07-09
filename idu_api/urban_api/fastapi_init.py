"""FastAPI application initialization is performed here."""

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi_pagination import add_pagination

from idu_api.common.db.connection.manager import PostgresConnectionManager
from idu_api.urban_api.config import UrbanAPIConfig
from idu_api.urban_api.logic.impl.physical_objects import PhysicalObjectsServiceImpl
from idu_api.urban_api.logic.impl.territories import TerritoriesServiceImpl
from idu_api.urban_api.middlewares.dependency_injection import PassServicesDependencies
from idu_api.urban_api.middlewares.exception_handler import ExceptionHandlerMiddleware

from .handlers import list_of_routes
from .version import LAST_UPDATE, VERSION


def bind_routes(application: FastAPI, prefix: str) -> None:
    """Bind all routes to application."""
    for route in list_of_routes:
        application.include_router(route, prefix=(prefix if "/" not in {r.path for r in route.routes} else ""))


def get_app(prefix: str = "/api") -> FastAPI:
    """Create application and all dependable objects."""

    description = "This is a Digital Territories Platform API to access and manipulate basic territories data."

    application = FastAPI(
        title="Digital Territories Platform Data API",
        description=description,
        docs_url=None,
        openapi_url=f"{prefix}/openapi",
        version=f"{VERSION} ({LAST_UPDATE})",
        terms_of_service="http://swagger.io/terms/",
        contact={"email": "idu@itmo.ru"},
        license_info={"name": "Apache 2.0", "url": "http://www.apache.org/licenses/LICENSE-2.0.html"},
        lifespan=lifespan,
    )
    bind_routes(application, prefix)

    @application.get(f"{prefix}/docs", include_in_schema=False)
    async def custom_swagger_ui_html():
        return get_swagger_ui_html(
            openapi_url=app.openapi_url,
            title=app.title + " - Swagger UI",
            oauth2_redirect_url=app.swagger_ui_oauth2_redirect_url,
            swagger_js_url="https://unpkg.com/swagger-ui-dist@5.11.7/swagger-ui-bundle.js",
            swagger_css_url="https://unpkg.com/swagger-ui-dist@5.11.7/swagger-ui.css",
        )

    origins = ["*"]

    application.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    add_pagination(application)

    connection_manager = PostgresConnectionManager("", 0, "", "", "", 0, "")

    application.add_middleware(
        ExceptionHandlerMiddleware,
        debug=[False],  # reinitialized on startup
    )
    application.add_middleware(
        PassServicesDependencies,
        connection_manager=connection_manager,  # reinitialized on startup
        territories_service=TerritoriesServiceImpl,
        physical_objects_service=PhysicalObjectsServiceImpl,
    )

    return application


@asynccontextmanager
async def lifespan(application: FastAPI):
    """Lifespan function.

    Initializes database connection in pass_services_dependencies middleware
    """
    for middleware in application.user_middleware:
        if middleware.cls == PassServicesDependencies:
            app_config = UrbanAPIConfig.try_from_env()
            connection_manager: PostgresConnectionManager = middleware.kwargs["connection_manager"]
            await connection_manager.update(
                app_config.db_addr,
                app_config.db_port,
                app_config.db_name,
                app_config.db_user,
                app_config.db_pass,
                app_config.db_pool_size,
                app_config.application_name,
            )
            await connection_manager.refresh()
        elif middleware.cls == ExceptionHandlerMiddleware:
            middleware.kwargs["debug"][0] = app_config.debug

    yield

    for middleware in application.user_middleware:
        if middleware.cls == PassServicesDependencies:
            connection_manager: PostgresConnectionManager = middleware.kwargs["connection_manager"]
            await connection_manager.shutdown()


app = get_app()
