"""Physical objects territories-related handlers (v2) are defined here."""

from fastapi import Path, Query, Request
from starlette import status

from idu_api.urban_api.logic.territories import TerritoriesService
from idu_api.urban_api.schemas import PhysicalObjectsData, PhysicalObjectWithGeometry
from idu_api.urban_api.schemas.enums import Ordering
from idu_api.urban_api.schemas.pages import CursorPage
from idu_api.urban_api.schemas.physical_objects import PhysicalObjectsOrderByField
from idu_api.urban_api.utils.pagination import paginate

from .routers import territories_router


@territories_router.get(
    "/territory/{territory_id}/physical_objects",
    response_model=CursorPage[PhysicalObjectsData],
    status_code=status.HTTP_200_OK,
)
async def get_physical_objects_by_territory_id(
    request: Request,
    territory_id: int = Path(..., description="territory id", gt=0),
    physical_object_type_id: int | None = Query(None, description="Physical object type id", gt=0),
    name: str | None = Query(None, description="Filter physical_objects by name substring (case-insensitive)"),
    order_by: PhysicalObjectsOrderByField = Query(  # should be Optional, but swagger is generated wrongly then
        None, description="Attribute to set ordering (created_at or updated_at)"
    ),
    ordering: Ordering = Query(
        Ordering.ASC, description="Order type (ascending or descending) if ordering field is set"
    ),
) -> CursorPage[PhysicalObjectsData]:
    """Get physical_objects for territory.

    physical_object_type could be specified in parameters.
    """
    territories_service: TerritoriesService = request.state.territories_service

    order_by_value = order_by.value if order_by is not None else None

    physical_objects = await territories_service.get_physical_objects_by_territory_id(
        territory_id, physical_object_type_id, name, order_by_value, ordering.value
    )

    return paginate(
        physical_objects.items,
        physical_objects.total,
        transformer=lambda x: [PhysicalObjectsData.from_dto(item) for item in x],
        additional_data=physical_objects.cursor_data,
    )


@territories_router.get(
    "/territory/{territory_id}/physical_objects_with_geometry",
    response_model=CursorPage[PhysicalObjectWithGeometry],
    status_code=status.HTTP_200_OK,
)
async def get_physical_objects_with_geometry_by_territory_id(
    request: Request,
    territory_id: int = Path(..., description="territory id", gt=0),
    physical_object_type_id: int | None = Query(None, description="Physical object type id", gt=0),
    name: str | None = Query(None, description="Filter physical_objects by name substring (case-insensitive)"),
    order_by: PhysicalObjectsOrderByField = Query(  # should be Optional, but swagger is generated wrongly then
        None, description="Attribute to set ordering (created_at or updated_at)"
    ),
    ordering: Ordering = Query(
        Ordering.ASC, description="Order type (ascending or descending) if ordering field is set"
    ),
) -> CursorPage[PhysicalObjectWithGeometry]:
    """Get physical_objects for territory.

    physical_object_type could be specified in parameters.
    """
    territories_service: TerritoriesService = request.state.territories_service

    order_by_value = order_by.value if order_by is not None else None

    physical_objects = await territories_service.get_physical_objects_with_geometry_by_territory_id(
        territory_id, physical_object_type_id, name, order_by_value, ordering.value
    )

    return paginate(
        physical_objects.items,
        physical_objects.total,
        transformer=lambda x: [PhysicalObjectWithGeometry.from_dto(item) for item in x],
        additional_data=physical_objects.cursor_data,
    )
