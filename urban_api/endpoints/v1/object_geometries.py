"""
Object geometries endpoints are defined here.
"""

from typing import Dict, List

from fastapi import Depends, Path, Query
from sqlalchemy.ext.asyncio import AsyncConnection
from starlette import status

from urban_api.db.connection import get_connection
from urban_api.logic.object_geometries import get_physical_objects_by_object_geometry_id_from_db
from urban_api.schemas import PhysicalObjectsData

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
