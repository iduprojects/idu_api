"""Object geometries handlers are defined here."""

from fastapi import Body, Path, Query, Request
from sqlalchemy.ext.asyncio import AsyncConnection
from starlette import status

from idu_api.urban_api.logic.object_geometries import (
    add_object_geometry_to_physical_object_in_db,
    delete_object_geometry_in_db,
    get_object_geometry_by_ids_from_db,
    get_physical_objects_by_object_geometry_id_from_db,
    patch_object_geometry_to_db,
    put_object_geometry_to_db,
)
from idu_api.urban_api.schemas import (
    ObjectGeometries,
    ObjectGeometriesPatch,
    ObjectGeometriesPost,
    ObjectGeometriesPut,
    PhysicalObjectsData,
)
from idu_api.urban_api.schemas.urban_objects import UrbanObject

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


@object_geometries_router.get(
    "/object_geometries",
    response_model=list[ObjectGeometries],
    status_code=status.HTTP_200_OK,
)
async def get_object_geometries_by_ids(
    request: Request, object_geometries_ids: str = Query(..., description="list of identifiers separated by comma")
) -> list[ObjectGeometries]:
    """Get list of object geometries data."""
    conn: AsyncConnection = request.state.conn

    object_geometries_ids = [int(geometry.strip()) for geometry in object_geometries_ids.split(",")]

    object_geometries = await get_object_geometry_by_ids_from_db(conn, object_geometries_ids)

    return [ObjectGeometries.from_dto(object_geometry) for object_geometry in object_geometries]


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


@object_geometries_router.delete(
    "/object_geometries/{object_geometry_id}",
    response_model=dict,
    status_code=status.HTTP_200_OK,
)
async def delete_object_geometry(
    request: Request,
    object_geometry_id: int = Path(..., description="Object geometry id"),
) -> dict:
    """Delete object geometry by given id."""
    conn: AsyncConnection = request.state.conn

    return await delete_object_geometry_in_db(conn, object_geometry_id)


@object_geometries_router.post(
    "/object_geometries/{physical_object_id}",
    response_model=UrbanObject,
    status_code=status.HTTP_200_OK,
)
async def add_object_geometry_to_physical_object(
    request: Request,
    physical_object_id: int = Path(..., description="Physical object id"),
    object_geometry: ObjectGeometriesPost = Body(..., description="Object Geometry"),
) -> UrbanObject:
    """Add object geometry to physical object"""
    conn: AsyncConnection = request.state.conn

    urban_object = await add_object_geometry_to_physical_object_in_db(conn, physical_object_id, object_geometry)

    return UrbanObject.from_dto(urban_object)
