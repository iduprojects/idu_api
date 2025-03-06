"""FastAPI application initialization is performed here."""

import os
from contextlib import asynccontextmanager
from typing import Callable

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi_pagination import add_pagination

from idu_api.common.db.connection.manager import PostgresConnectionManager
from idu_api.urban_api.config import UrbanAPIConfig
from idu_api.urban_api.logic.impl.functional_zones import FunctionalZonesServiceImpl
from idu_api.urban_api.logic.impl.indicators import IndicatorsServiceImpl
from idu_api.urban_api.logic.impl.object_geometries import ObjectGeometriesServiceImpl
from idu_api.urban_api.logic.impl.physical_object_types import PhysicalObjectTypesServiceImpl
from idu_api.urban_api.logic.impl.physical_objects import PhysicalObjectsServiceImpl
from idu_api.urban_api.logic.impl.projects import UserProjectServiceImpl
from idu_api.urban_api.logic.impl.service_types import ServiceTypesServiceImpl
from idu_api.urban_api.logic.impl.services import ServicesDataServiceImpl
from idu_api.urban_api.logic.impl.system import SystemServiceImpl
from idu_api.urban_api.logic.impl.territories import TerritoriesServiceImpl
from idu_api.urban_api.logic.impl.urban_objects import UrbanObjectsServiceImpl
from idu_api.urban_api.middlewares.authentication import AuthenticationMiddleware
from idu_api.urban_api.middlewares.dependency_injection import PassServicesDependenciesMiddleware
from idu_api.urban_api.middlewares.exception_handler import ExceptionHandlerMiddleware
from idu_api.urban_api.middlewares.logging import LoggingMiddleware
from idu_api.urban_api.prometheus import server as prometheus_server
from idu_api.urban_api.utils.auth_client import AuthenticationClient
from idu_api.urban_api.utils.logging import configure_logging

from .handlers import list_of_routers
from .version import LAST_UPDATE, VERSION


def bind_routes(application: FastAPI, prefix: str, debug: bool) -> None:
    """Bind all routes to application."""
    for router in list_of_routers:
        if not debug:
            to_remove = []
            for i, route in enumerate(router.routes):
                if "debug" in route.path:
                    to_remove.append(i)
            for i in to_remove[::-1]:
                del router.routes[i]
        if len(router.routes) > 0:
            application.include_router(router, prefix=(prefix if "/" not in {r.path for r in router.routes} else ""))


def get_app(prefix: str = "/api") -> FastAPI:
    """Create application and all dependable objects."""

    app_config: UrbanAPIConfig = UrbanAPIConfig.from_file_or_default(os.getenv("CONFIG_PATH"))

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
    bind_routes(application, prefix, app_config.app.debug)

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

    connection_manager = PostgresConnectionManager(..., [], ...)
    auth_client = AuthenticationClient(0, 0, False, "")

    def ignore_kwargs(func: Callable) -> Callable:
        def wrapped(*args, **_kwargs):
            return func(*args)

        return wrapped

    application.state.config = app_config

    application.add_middleware(
        PassServicesDependenciesMiddleware,
        connection_manager=connection_manager,  # reinitialized on startup
        functional_zones_service=ignore_kwargs(FunctionalZonesServiceImpl),
        indicators_service=ignore_kwargs(IndicatorsServiceImpl),
        object_geometries_service=ignore_kwargs(ObjectGeometriesServiceImpl),
        physical_object_types_service=ignore_kwargs(PhysicalObjectTypesServiceImpl),
        physical_objects_service=ignore_kwargs(PhysicalObjectsServiceImpl),
        service_types_service=ignore_kwargs(ServiceTypesServiceImpl),
        services_data_service=ignore_kwargs(ServicesDataServiceImpl),
        territories_service=ignore_kwargs(TerritoriesServiceImpl),
        urban_objects_service=ignore_kwargs(UrbanObjectsServiceImpl),
        user_project_service=UserProjectServiceImpl,
        system_service=SystemServiceImpl,
    )
    application.add_middleware(
        LoggingMiddleware,
    )
    application.add_middleware(
        AuthenticationMiddleware,
        auth_client=auth_client,  # reinitialized on startup
    )
    application.add_middleware(
        ExceptionHandlerMiddleware,
        debug=[False],  # reinitialized on startup
    )

    return application


@asynccontextmanager
async def lifespan(application: FastAPI):
    """Lifespan function.

    Initializes database connection in pass_services_dependencies middleware.
    """
    app_config: UrbanAPIConfig = application.state.config
    loggers_dict = {logger_config.filename: logger_config.level for logger_config in app_config.logging.files}
    logger = configure_logging(app_config.logging.level, loggers_dict)
    application.state.logger = logger

    for middleware in application.user_middleware:
        if middleware.cls == PassServicesDependenciesMiddleware:
            connection_manager: PostgresConnectionManager = middleware.kwargs["connection_manager"]
            await connection_manager.update(
                master=app_config.db.master,
                replicas=app_config.db.replicas,
                logger=logger,
                application_name=app_config.app.name,
            )
            await connection_manager.refresh()
        elif middleware.cls == ExceptionHandlerMiddleware:
            middleware.kwargs["debug"][0] = app_config.app.debug
        elif middleware.cls == AuthenticationMiddleware:
            auth_client: AuthenticationClient = middleware.kwargs["auth_client"]
            auth_client.update(
                app_config.auth.cache_size,
                app_config.auth.cache_ttl,
                app_config.auth.validate,
                app_config.auth.url,
            )

    if not app_config.prometheus.disable:
        prometheus_server.start_server(port=app_config.prometheus.port)

    yield

    for middleware in application.user_middleware:
        if middleware.cls == PassServicesDependenciesMiddleware:
            connection_manager: PostgresConnectionManager = middleware.kwargs["connection_manager"]
            await connection_manager.shutdown()

    if not app_config.prometheus.disable:
        prometheus_server.stop_server()


app = get_app()
