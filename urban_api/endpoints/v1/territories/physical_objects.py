"""Physical objects territories-related endpoints are defined here."""

from fastapi import Depends, Path, Query
from fastapi_pagination import paginate
from sqlalchemy.ext.asyncio import AsyncConnection
from starlette import status

from urban_api.db.connection import get_connection
from urban_api.logic.territories import (
    get_physical_objects_by_territory_id_from_db,
    get_physical_objects_with_geometry_by_territory_id_from_db,
)
from urban_api.schemas import PhysicalObjectsData, PhysicalObjectWithGeometry
from urban_api.schemas.pages import Page

from .routers import territories_router


@territories_router.get(
    "/territory/{territory_id}/physical_objects",
    response_model=Page[PhysicalObjectsData],
    status_code=status.HTTP_200_OK,
)
async def get_physical_objects_by_territory_id(
    territory_id: int = Path(description="territory id", gt=0),
    physical_object_type: int | None = Query(None, description="Physical object type id", gt=0),
    name: str | None = Query(None, description="Filter physical_objects by name substring (case-insensitive)"),
    connection: AsyncConnection = Depends(get_connection),
) -> Page[PhysicalObjectsData]:
    """
    Summary:
        Get physical_objects for territory

    Description:
        Get physical_objects for territory, physical_object_type could be specified in parameters
    """

    physical_objects = await get_physical_objects_by_territory_id_from_db(
        territory_id, connection, physical_object_type, name
    )
    physical_objects = [PhysicalObjectsData.from_dto(physical_object) for physical_object in physical_objects]

    return paginate(physical_objects)


@territories_router.get(
    "/territory/{territory_id}/physical_objects_with_geometry",
    response_model=Page[PhysicalObjectWithGeometry],
    status_code=status.HTTP_200_OK,
)
async def get_physical_objects_with_geometry_by_territory_id(
    territory_id: int = Path(description="territory id", gt=0),
    physical_object_type: int | None = Query(None, description="Physical object type id", gt=0),
    name: str | None = Query(None, description="Filter physical_objects by name substring (case-insensitive)"),
    connection: AsyncConnection = Depends(get_connection),
) -> Page[PhysicalObjectWithGeometry]:
    """
    Summary:
        Get physical_objects with geometry for territory

    Description:
        Get physical_objects for territory, physical_object_type could be specified in parameters
    """
    physical_objects_with_geometry_dto = await get_physical_objects_with_geometry_by_territory_id_from_db(
        territory_id, connection, physical_object_type, name
    )
    physical_objects = [PhysicalObjectWithGeometry.from_dto(obj) for obj in physical_objects_with_geometry_dto]

    return paginate(physical_objects)
