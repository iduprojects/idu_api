"""Services territories-related handlers (v2) are defined here."""

from fastapi import Path, Query, Request
from starlette import status

from idu_api.urban_api.logic.territories import TerritoriesService
from idu_api.urban_api.schemas import ServicesData, ServicesDataWithGeometry
from idu_api.urban_api.schemas.enums import Ordering
from idu_api.urban_api.schemas.pages import CursorPage
from idu_api.urban_api.schemas.services import ServicesOrderByField
from idu_api.urban_api.utils.pagination import paginate

from .routers import territories_router


@territories_router.get(
    "/territory/{territory_id}/services",
    response_model=CursorPage[ServicesData],
    status_code=status.HTTP_200_OK,
)
async def get_services_by_territory_id(
    request: Request,
    territory_id: int = Path(..., description="territory id", gt=0),
    service_type_id: int | None = Query(None, description="Service type id", gt=0),
    name: str | None = Query(None, description="Filter services by name substring (case-insensitive)"),
    order_by: ServicesOrderByField = Query(  # should be Optional, but swagger is generated wrongly then
        None, description="Attribute to set ordering (created_at or updated_at)"
    ),
    ordering: Ordering = Query(
        Ordering.ASC, description="Order type (ascending or descending) if ordering field is set"
    ),
) -> CursorPage[ServicesData]:
    """Get services for territory by id.

    service type and name could be specified in parameters.
    """
    territories_service: TerritoriesService = request.state.territories_service

    order_by_value = order_by.value if order_by is not None else None

    services = await territories_service.get_services_by_territory_id(
        territory_id, service_type_id, name, order_by_value, ordering.value
    )

    return paginate(
        services.items,
        services.total,
        transformer=lambda x: [ServicesData.from_dto(item) for item in x],
        additional_data=services.cursor_data,
    )


@territories_router.get(
    "/territory/{territory_id}/services_with_geometry",
    response_model=CursorPage[ServicesDataWithGeometry],
    status_code=status.HTTP_200_OK,
)
async def get_services_with_geometry_by_territory_id(
    request: Request,
    territory_id: int = Path(..., description="territory id", gt=0),
    service_type_id: int | None = Query(None, description="Service type id", gt=0),
    name: str | None = Query(None, description="Filter services by name substring (case-insensitive)"),
    order_by: ServicesOrderByField = Query(  # should be Optional, but swagger is generated wrongly then
        None, description="Attribute to set ordering (created_at or updated_at)"
    ),
    ordering: Ordering = Query(
        Ordering.ASC, description="Order type (ascending or descending) if ordering field is set"
    ),
) -> CursorPage[ServicesDataWithGeometry]:
    """Get services for territory by id.

    service type could be specified in parameters.
    """
    territories_service: TerritoriesService = request.state.territories_service

    order_by_value = order_by.value if order_by is not None else None

    services = await territories_service.get_services_with_geometry_by_territory_id(
        territory_id, service_type_id, name, order_by_value, ordering.value
    )

    return paginate(
        services.items,
        services.total,
        transformer=lambda x: [ServicesDataWithGeometry.from_dto(item) for item in x],
        additional_data=services.cursor_data,
    )
