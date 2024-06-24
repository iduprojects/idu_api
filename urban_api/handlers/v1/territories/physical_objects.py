"""Physical objects territories-related handlers are defined here."""

from fastapi import Path, Query, Request
from fastapi_pagination import paginate
from starlette import status

from urban_api.logic.territories import TerritoriesService
from urban_api.schemas import PhysicalObjectsData, PhysicalObjectWithGeometry
from urban_api.schemas.pages import Page

from .routers import territories_router


@territories_router.get(
    "/territory/{territory_id}/physical_objects",
    response_model=Page[PhysicalObjectsData],
    status_code=status.HTTP_200_OK,
)
async def get_physical_objects_by_territory_id(
    request: Request,
    territory_id: int = Path(description="territory id", gt=0),
    physical_object_type_id: int | None = Query(None, description="Physical object type id", gt=0),
    name: str | None = Query(None, description="Filter physical_objects by name substring (case-insensitive)"),
) -> Page[PhysicalObjectsData]:
    """Get physical_objects for territory.

    physical_object_type could be specified in parameters.
    """
    territories_service: TerritoriesService = request.state.territories_service

    physical_objects = await territories_service.get_physical_objects_by_territory_id(
        territory_id, physical_object_type_id, name
    )
    physical_objects = [PhysicalObjectsData.from_dto(physical_object) for physical_object in physical_objects]

    return paginate(physical_objects)


@territories_router.get(
    "/territory/{territory_id}/physical_objects_with_geometry",
    response_model=Page[PhysicalObjectWithGeometry],
    status_code=status.HTTP_200_OK,
)
async def get_physical_objects_with_geometry_by_territory_id(
    request: Request,
    territory_id: int = Path(description="territory id", gt=0),
    physical_object_type_id: int | None = Query(None, description="Physical object type id", gt=0),
    name: str | None = Query(None, description="Filter physical_objects by name substring (case-insensitive)"),
) -> Page[PhysicalObjectWithGeometry]:
    """Get physical_objects for territory.

    physical_object_type could be specified in parameters.
    """
    territories_service: TerritoriesService = request.state.territories_service

    physical_objects_with_geometry_dto = await territories_service.get_physical_objects_with_geometry_by_territory_id(
        territory_id, physical_object_type_id, name
    )
    physical_objects = [PhysicalObjectWithGeometry.from_dto(obj) for obj in physical_objects_with_geometry_dto]

    return paginate(physical_objects)
