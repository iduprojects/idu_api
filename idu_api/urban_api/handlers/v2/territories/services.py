"""Services territories-related handlers (v2) are defined here."""

from fastapi import HTTPException, Path, Query, Request
from starlette import status

from idu_api.urban_api.logic.territories import TerritoriesService
from idu_api.urban_api.schemas import Service, ServiceWithGeometry
from idu_api.urban_api.schemas.enums import OrderByField, Ordering
from idu_api.urban_api.schemas.pages import CursorPage
from idu_api.urban_api.utils.pagination import paginate

from .routers import territories_router


@territories_router.get(
    "/territory/{territory_id}/services",
    response_model=CursorPage[Service],
    status_code=status.HTTP_200_OK,
)
async def get_services_by_territory_id(
    request: Request,
    territory_id: int = Path(..., description="territory id", gt=0),
    service_type_id: int | None = Query(None, description="service type identifier", gt=0),
    urban_function_id: int | None = Query(None, description="urban function identifier", gt=0),
    name: str | None = Query(None, description="filter services by name substring (case-insensitive)"),
    include_child_territories: bool = Query(True, description="to get from child territories"),
    cities_only: bool = Query(False, description="to get only cities or not"),
    order_by: OrderByField = Query(  # should be Optional, but swagger is generated wrongly then
        None, description="attribute to set ordering (created_at or updated_at)"
    ),
    ordering: Ordering = Query(
        Ordering.ASC, description="order type (ascending or descending) if ordering field is set"
    ),
) -> CursorPage[Service]:
    """
    ## Get services for a given territory.

    **WARNING 1:** Set `cities_only = True` only if you want to get entities from child territories.

    **WARNING 2:** You can only filter by service type or urban function.

    ### Parameters:
    - **territory_id** (int, Path): Unique identifier of the territory.
    - **service_type_id** (int | None, Query): Filters results by service type.
    - **urban_function_id** (int | None, Query): Filters results by urban function.
    - **name** (str | None, Query): Filters results by a case-insensitive substring match.
    - **include_child_territories** (bool, Query): If True, includes data from child territories (default: true).
      Note: This can be unsafe for high-level territories due to potential performance issues.
    - **cities_only** (bool, Query): If True, retrieves data only for cities (default: false).
    - **order_by** (OrderByField, Query): Defines the sorting attribute - service_id (default), created_at or updated_at.
    - **ordering** (Ordering, Query): Specifies sorting order - ascending (default) or descending.
    - **cursor** (str, Query): Cursor (encrypted `service_id`) for the next page.
    - **page_size** (int, Query): Defines the number of services per page (default: 10).

    ### Returns:
    - **CursorPage[Service]**: A paginated list of services, including cursor-based pagination data.

    ### Errors:
    - **400 Bad Request**: If `cities_only` is set to True and `include_child_territories` is set to False or
    set both `service_type_id` and `urban_function_id`.
    - **404 Not Found**: If the territory does not exist.
    """
    territories_service: TerritoriesService = request.state.territories_service

    if not include_child_territories and cities_only:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="You can use cities_only parameter only with including child territories",
        )

    if service_type_id is not None and urban_function_id is not None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Please, choose either service_type_id or urban_function_id",
        )

    order_by_value = order_by.value if order_by is not None else None

    services = await territories_service.get_services_by_territory_id(
        territory_id,
        service_type_id,
        urban_function_id,
        name,
        include_child_territories,
        cities_only,
        order_by_value,
        ordering.value,
        paginate=True,
    )

    return paginate(
        services.items,
        services.total,
        transformer=lambda x: [Service.from_dto(item) for item in x],
        additional_data=services.cursor_data,
    )


@territories_router.get(
    "/territory/{territory_id}/services_with_geometry",
    response_model=CursorPage[ServiceWithGeometry],
    status_code=status.HTTP_200_OK,
)
async def get_services_with_geometry_by_territory_id(
    request: Request,
    territory_id: int = Path(..., description="territory identifier", gt=0),
    service_type_id: int | None = Query(None, description="to filter by service type", gt=0),
    urban_function_id: int | None = Query(None, description="to filter by urban function", gt=0),
    name: str | None = Query(None, description="to filter services by name substring (case-insensitive)"),
    include_child_territories: bool = Query(True, description="to get from child territories"),
    cities_only: bool = Query(False, description="to get only for cities"),
    order_by: OrderByField = Query(  # should be Optional, but swagger is generated wrongly then
        None, description="Attribute to set ordering (created_at or updated_at)"
    ),
    ordering: Ordering = Query(
        Ordering.ASC, description="Order type (ascending or descending) if ordering field is set"
    ),
) -> CursorPage[ServiceWithGeometry]:
    """
    ## Get services with geometry for a given territory.

    **WARNING 1:** Set `cities_only = True` only if you want to get entities from child territories.

    **WARNING 2:** You can only filter by service type or urban function.

    ### Parameters:
    - **territory_id** (int, Path): Unique identifier of the territory.
    - **service_type_id** (int | None, Query): Filters results by service type.
    - **urban_function_id** (int | None, Query): Filters results by urban function.
    - **name** (str | None, Query): Filters results by a case-insensitive substring match.
    - **include_child_territories** (bool, Query): If True, includes data from child territories (default: true).
      Note: This can be unsafe for high-level territories due to potential performance issues.
    - **cities_only** (bool, Query): If True, retrieves data only for cities (default: false).
    - **order_by** (OrderByField, Query): Defines the sorting attribute - service_id (default), created_at or updated_at.
    - **ordering** (Ordering, Query): Specifies sorting order - ascending (default) or descending.
    - **cursor** (str, Query): Cursor (encrypted `service_id`) for the next page.
    - **page_size** (int, Query): Defines the number of services per page (default: 10).

    ### Returns:
    - **CursorPage[ServiceWithGeometry]**: A paginated list of services, including cursor-based pagination data.

    ### Errors:
    - **400 Bad Request**: If `cities_only` is set to True and `include_child_territories` is set to False or
    set both `service_type_id` and `urban_function_id`.
    - **404 Not Found**: If the territory does not exist.
    """
    territories_service: TerritoriesService = request.state.territories_service

    if not include_child_territories and cities_only:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="You can use cities_only parameter only with including child territories",
        )

    if service_type_id is not None and urban_function_id is not None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Please, choose either service_type_id or urban_function_id",
        )

    order_by_value = order_by.value if order_by is not None else None

    services = await territories_service.get_services_with_geometry_by_territory_id(
        territory_id,
        service_type_id,
        urban_function_id,
        name,
        include_child_territories,
        cities_only,
        order_by_value,
        ordering.value,
        paginate=True,
    )

    return paginate(
        services.items,
        services.total,
        transformer=lambda x: [ServiceWithGeometry.from_dto(item) for item in x],
        additional_data=services.cursor_data,
    )
