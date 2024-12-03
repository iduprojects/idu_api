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
    territory_id: int = Path(..., description="territory identifier", gt=0),
    physical_object_type_id: int | None = Query(None, description="to filter by physical object type", gt=0),
    physical_object_function_id: int | None = Query(None, description="to filter by physical object function", gt=0),
    name: str | None = Query(None, description="filter physical objects by name substring (case-insensitive)"),
    include_child_territories: bool = Query(True, description="to get from child territories"),
    cities_only: bool = Query(False, description="to get only for cities"),
    order_by: PhysicalObjectsOrderByField = Query(  # should be Optional, but swagger is generated wrongly then
        None, description="attribute to set ordering (created_at or updated_at)"
    ),
    ordering: Ordering = Query(
        Ordering.ASC, description="order type (ascending or descending) if ordering field is set"
    ),
) -> CursorPage[PhysicalObjectsData]:
    """Get physical objects for territory.

    physical object type, cities only and physical object function could be specified in parameters.
    """
    territories_service: TerritoriesService = request.state.territories_service

    order_by_value = order_by.value if order_by is not None else None

    physical_objects = await territories_service.get_physical_objects_by_territory_id(
        territory_id,
        physical_object_type_id,
        physical_object_function_id,
        name,
        include_child_territories,
        cities_only,
        order_by_value,
        ordering.value,
        paginate=True,
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
    territory_id: int = Path(..., description="territory identifier", gt=0),
    physical_object_type_id: int | None = Query(None, description="to filter by physical object type", gt=0),
    physical_object_function_id: int | None = Query(None, description="to filter by physical object function", gt=0),
    name: str | None = Query(None, description="filter physical objects by name substring (case-insensitive)"),
    include_child_territories: bool = Query(True, description="to get from child territories"),
    cities_only: bool = Query(False, description="to get only for cities"),
    order_by: PhysicalObjectsOrderByField = Query(  # should be Optional, but swagger is generated wrongly then
        None, description="attribute to set ordering (created_at or updated_at)"
    ),
    ordering: Ordering = Query(
        Ordering.ASC, description="order type (ascending or descending) if ordering field is set"
    ),
) -> CursorPage[PhysicalObjectWithGeometry]:
    """Get physical objects with geometry for territory.

    physical object type, cities only and physical object function could be specified in parameters.
    """
    territories_service: TerritoriesService = request.state.territories_service

    order_by_value = order_by.value if order_by is not None else None

    physical_objects = await territories_service.get_physical_objects_with_geometry_by_territory_id(
        territory_id,
        physical_object_type_id,
        physical_object_function_id,
        name,
        include_child_territories,
        cities_only,
        order_by_value,
        ordering.value,
        paginate=True,
    )

    return paginate(
        physical_objects.items,
        physical_objects.total,
        transformer=lambda x: [PhysicalObjectWithGeometry.from_dto(item) for item in x],
        additional_data=physical_objects.cursor_data,
    )
