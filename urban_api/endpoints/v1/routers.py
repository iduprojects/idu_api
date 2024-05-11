"""
Api routers are defined here.

It is needed to import files which use these routers to initialize endpoints.
"""

from fastapi import APIRouter

indicators_router = APIRouter(tags=["indicators"], prefix="/v1")

territories_router = APIRouter(tags=["territories"], prefix="/v1")

service_types_router = APIRouter(tags=["service_types"], prefix="/v1")

functional_zones_router = APIRouter(tags=["functional_zones"], prefix="/v1")

physical_objects_router = APIRouter(tags=["physical_objects"], prefix="/v1")

object_geometries_router = APIRouter(tags=["object_geometries"], prefix="/v1")

services_router = APIRouter(tags=["services"], prefix="/v1")

routers_list = [
    indicators_router,
    territories_router,
    services_router,
    functional_zones_router,
    physical_objects_router,
    object_geometries_router,
    services_router,
]

__all__ = [
    "routers_list",
]
