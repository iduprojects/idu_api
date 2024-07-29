"""Services handlers are defined here."""

from fastapi import Path, Query, Request
from sqlalchemy.ext.asyncio import AsyncConnection
from starlette import status

from idu_api.urban_api.logic.services import (
    add_service_to_db,
    add_service_to_object_in_db,
    delete_service_in_db,
    get_service_with_territories_by_id_from_db,
    patch_service_to_db,
    put_service_to_db,
)
from idu_api.urban_api.schemas import (
    ServicesData,
    ServicesDataPatch,
    ServicesDataPost,
    ServicesDataPut,
    ServiceWithTerritories,
)
from idu_api.urban_api.schemas.urban_objects import UrbanObject

from .routers import services_router


@services_router.post(
    "/services",
    response_model=ServicesData,
    status_code=status.HTTP_201_CREATED,
)
async def add_service(request: Request, service: ServicesDataPost) -> ServicesData:
    """Add a service to a given physical object."""
    conn: AsyncConnection = request.state.conn

    service_dto = await add_service_to_db(conn, service)

    return ServicesData.from_dto(service_dto)


@services_router.put(
    "/services/{service_id}",
    response_model=ServicesData,
    status_code=status.HTTP_200_OK,
)
async def put_service(
    request: Request,
    service: ServicesDataPut,
    service_id: int = Path(..., description="Service id", gt=0),
) -> ServicesData:
    """Update the given service - all attributes."""
    conn: AsyncConnection = request.state.conn

    service_dto = await put_service_to_db(conn, service, service_id)

    return ServicesData.from_dto(service_dto)


@services_router.patch(
    "/services/{service_id}",
    response_model=ServicesData,
    status_code=status.HTTP_200_OK,
)
async def patch_service(
    request: Request,
    service: ServicesDataPatch,
    service_id: int = Path(..., description="Service id", gt=0),
) -> ServicesData:
    """Update the given service - only given attributes."""
    conn: AsyncConnection = request.state.conn

    service_dto = await patch_service_to_db(conn, service, service_id)

    return ServicesData.from_dto(service_dto)


@services_router.delete(
    "/services/{service_id}",
    response_model=dict,
    status_code=status.HTTP_200_OK,
)
async def delete_service(
    request: Request,
    service_id: int = Path(..., description="Service id", gt=0),
) -> dict:
    """Delete service object by given id."""
    conn: AsyncConnection = request.state.conn

    return await delete_service_in_db(conn, service_id)


@services_router.get(
    "/services/{service_id}",
    response_model=ServiceWithTerritories,
    status_code=status.HTTP_200_OK,
)
async def get_service_by_id_with_territory(
    request: Request,
    service_id: int = Path(..., description="Service id", gt=0),
) -> ServiceWithTerritories:
    """Get service by id with parent territory"""
    conn: AsyncConnection = request.state.conn

    service = await get_service_with_territories_by_id_from_db(conn, service_id)

    return ServiceWithTerritories.from_dto(service)


@services_router.post(
    "/services/{service_id}",
    response_model=UrbanObject,
    status_code=status.HTTP_200_OK,
)
async def add_service_to_objects(
    request: Request,
    service_id: int = Path(..., description="Service id"),
    physical_object_id: int = Query(..., description="Physical object id"),
    object_geometry_id: int = Query(..., description="Object geometry id"),
) -> UrbanObject:
    """Add an existing service to physical object"""
    conn: AsyncConnection = request.state.conn

    urban_object_dto = await add_service_to_object_in_db(conn, service_id, physical_object_id, object_geometry_id)

    return UrbanObject.from_dto(urban_object_dto)
