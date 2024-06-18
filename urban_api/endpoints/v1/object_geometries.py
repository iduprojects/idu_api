"""
Object geometries endpoints are defined here.
"""

from typing import List

from fastapi import Depends, Path
from sqlalchemy.ext.asyncio import AsyncConnection
from starlette import status

from urban_api.db.connection import get_connection
from urban_api.logic.object_geometries import (
    get_physical_objects_by_object_geometry_id_from_db,
    patch_object_geometry_to_db,
    put_object_geometry_to_db,
)
from urban_api.schemas import ObjectGeometries, ObjectGeometriesPatch, ObjectGeometriesPut, PhysicalObjectsData

from .routers import object_geometries_router


@object_geometries_router.get(
    "/object_geometries/{object_geometry_id}/physical_objects",
    response_model=List[PhysicalObjectsData],
    status_code=status.HTTP_200_OK,
)
async def get_physical_object_by_geometry_id(
    object_geometry_id: int = Path(..., description="Object geometry id"),
    connection: AsyncConnection = Depends(get_connection),
) -> List[PhysicalObjectsData]:
    """
    Summary:
        Get physical objects by object geometry id

    Description:
        Get a list of all physical objects by object geometry id
    """

    physical_objects = await get_physical_objects_by_object_geometry_id_from_db(object_geometry_id, connection)

    return [PhysicalObjectsData.from_dto(physical_object) for physical_object in physical_objects]


@object_geometries_router.put(
    "/object_geometries/{object_geometry_id}",
    response_model=ObjectGeometries,
    status_code=status.HTTP_200_OK,
)
async def put_object_geometry(
    object_geometry: ObjectGeometriesPut,
    object_geometry_id: int = Path(..., description="Object geometry id"),
    connection: AsyncConnection = Depends(get_connection),
) -> ObjectGeometries:
    """
    Summary:
        Put object geometry

    Description:
        Put object geometry
    """

    object_geometry_dto = await put_object_geometry_to_db(object_geometry, object_geometry_id, connection)

    return ObjectGeometries.from_dto(object_geometry_dto)


@object_geometries_router.patch(
    "/object_geometries/{object_geometry_id}",
    response_model=ObjectGeometries,
    status_code=status.HTTP_200_OK,
)
async def patch_object_geometry(
    object_geometry: ObjectGeometriesPatch,
    object_geometry_id: int = Path(..., description="Object geometry id"),
    connection: AsyncConnection = Depends(get_connection),
) -> ObjectGeometries:
    """
    Summary:
        Patch object geometry

    Description:
        Patch object geometry
    """

    object_geometry_dto = await patch_object_geometry_to_db(object_geometry, object_geometry_id, connection)

    return ObjectGeometries.from_dto(object_geometry_dto)
