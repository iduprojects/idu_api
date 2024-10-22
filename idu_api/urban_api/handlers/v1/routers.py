"""Api routers are defined here.

It is needed to import files which use these routers to initialize handlers.
"""

from fastapi import APIRouter

from .projects import routers_list as projects_routers
from .territories import routers_list as territories_routers

indicators_router = APIRouter(tags=["indicators"], prefix="/v1")

service_types_router = APIRouter(tags=["service_types"], prefix="/v1")

functional_zones_router = APIRouter(tags=["functional_zones"], prefix="/v1")

physical_object_types_router = APIRouter(tags=["physical_object_types"], prefix="/v1")

physical_objects_router = APIRouter(tags=["physical_objects"], prefix="/v1")

object_geometries_router = APIRouter(tags=["object_geometries"], prefix="/v1")

services_router = APIRouter(tags=["services"], prefix="/v1")

urban_objects_router = APIRouter(tags=["urban_objects"], prefix="/v1")

routers_list = [
    indicators_router,
    services_router,
    functional_zones_router,
    physical_object_types_router,
    physical_objects_router,
    object_geometries_router,
    service_types_router,
    urban_objects_router,
    *territories_routers,
    *projects_routers,
]

__all__ = [
    "routers_list",
]
