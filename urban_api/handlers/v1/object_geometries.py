"""Object geometries handlers are defined here."""

from fastapi import Path, Request
from sqlalchemy.ext.asyncio import AsyncConnection
from starlette import status

from urban_api.logic.object_geometries import (
    get_physical_objects_by_object_geometry_id_from_db,
    patch_object_geometry_to_db,
    put_object_geometry_to_db,
)
from urban_api.schemas import ObjectGeometries, ObjectGeometriesPatch, ObjectGeometriesPut, PhysicalObjectsData

from .routers import object_geometries_router


@object_geometries_router.get(
    "/object_geometries/{object_geometry_id}/physical_objects",
    response_model=list[PhysicalObjectsData],
    status_code=status.HTTP_200_OK,
)
async def get_physical_object_by_geometry_id(
    request: Request,
    object_geometry_id: int = Path(..., description="Object geometry id"),
) -> list[PhysicalObjectsData]:
    """Get physical objects for the given object geometry."""
    conn: AsyncConnection = request.state.conn

    physical_objects = await get_physical_objects_by_object_geometry_id_from_db(conn, object_geometry_id)

    return [PhysicalObjectsData.from_dto(physical_object) for physical_object in physical_objects]


@object_geometries_router.put(
    "/object_geometries/{object_geometry_id}",
    response_model=ObjectGeometries,
    status_code=status.HTTP_200_OK,
)
async def put_object_geometry(
    request: Request,
    object_geometry: ObjectGeometriesPut,
    object_geometry_id: int = Path(..., description="Object geometry id"),
) -> ObjectGeometries:
    """Update object geometry - all attributes."""
    conn: AsyncConnection = request.state.conn

    object_geometry_dto = await put_object_geometry_to_db(conn, object_geometry, object_geometry_id)

    return ObjectGeometries.from_dto(object_geometry_dto)


@object_geometries_router.patch(
    "/object_geometries/{object_geometry_id}",
    response_model=ObjectGeometries,
    status_code=status.HTTP_200_OK,
)
async def patch_object_geometry(
    request: Request,
    object_geometry: ObjectGeometriesPatch,
    object_geometry_id: int = Path(..., description="Object geometry id"),
) -> ObjectGeometries:
    """Update object geometry - only given attributes."""
    conn: AsyncConnection = request.state.conn

    object_geometry_dto = await patch_object_geometry_to_db(conn, object_geometry, object_geometry_id)

    return ObjectGeometries.from_dto(object_geometry_dto)
