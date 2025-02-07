"""Physical objects territories-related handlers (v2) are defined here."""

from fastapi import HTTPException, Path, Query, Request
from starlette import status

from idu_api.urban_api.logic.territories import TerritoriesService
from idu_api.urban_api.schemas import PhysicalObject, PhysicalObjectWithGeometry
from idu_api.urban_api.schemas.enums import OrderByField, Ordering
from idu_api.urban_api.schemas.pages import CursorPage
from idu_api.urban_api.utils.pagination import paginate

from .routers import territories_router


@territories_router.get(
    "/territory/{territory_id}/physical_objects",
    response_model=CursorPage[PhysicalObject],
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
    order_by: OrderByField = Query(  # should be Optional, but swagger is generated wrongly then
        None, description="attribute to set ordering (created_at or updated_at)"
    ),
    ordering: Ordering = Query(
        Ordering.ASC, description="order type (ascending or descending) if ordering field is set"
    ),
) -> CursorPage[PhysicalObject]:
    """
    ## Get physical objects for a given territory.

    **WARNING 1**: Set `cities_only = True` only if you want to get entities from child territories.

    **WARNING 2:** You can only filter by physical object type or physical object function.

    ### Parameters:
    - **territory_id** (int, Path): Unique identifier of the territory.
    - **physical_object_type_id** (int | None, Query): Filters results by physical object type.
    - **physical_object_function_id** (int | None, Query): Filters results by physical object function.
    - **name** (str | None, Query): Filters results by a case-insensitive substring match.
    - **include_child_territories** (bool, Query): If True, includes data from child territories (default: true).
      Note: This can be unsafe for high-level territories due to potential performance issues.
    - **cities_only** (bool, Query): If True, retrieves data only for cities (default: false).
    - **order_by** (PhysicalObjectsOrderByField, Query): Defines the sorting attribute - physical_object_id (default), created_at or updated_at.
    - **ordering** (Ordering, Query): Specifies sorting order - ascending (default) or descending.
    - **cursor** (str, Query): Cursor (encrypted `physical_object_id`) for the next page.
    - **page_size** (int, Query): Defines the number of physical objects per page (default: 10).

    ### Returns:
    - **CursorPage[PhysicalObject]**: A paginated list of physical objects, including cursor-based pagination data.

    ### Errors:
    - **400 Bad Request**: If `cities_only` is set to True and `include_child_territories` is set to False or
    set both `physical_object_type_id` and `physical_object_function_id`.
    - **404 Not Found**: If the territory does not exist.
    """
    territories_service: TerritoriesService = request.state.territories_service

    if not include_child_territories and cities_only:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="You can use cities_only parameter only with including child territories",
        )

    if physical_object_type_id is not None and physical_object_function_id is not None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Please, choose either physical_object_type_id or physical_object_function_id",
        )

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
        transformer=lambda x: [PhysicalObject.from_dto(item) for item in x],
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
    order_by: OrderByField = Query(  # should be Optional, but swagger is generated wrongly then
        None, description="attribute to set ordering (created_at or updated_at)"
    ),
    ordering: Ordering = Query(
        Ordering.ASC, description="order type (ascending or descending) if ordering field is set"
    ),
) -> CursorPage[PhysicalObjectWithGeometry]:
    """
    ## Get physical objects with geometry for a given territory.

    **WARNING 1**: Set `cities_only = True` only if you want to get entities from child territories.

    **WARNING 2:** You can only filter by physical object type or physical object function.

    ### Parameters:
    - **territory_id** (int, Path): Unique identifier of the territory.
    - **physical_object_type_id** (int | None, Query): Filters results by physical object type.
    - **physical_object_function_id** (int | None, Query): Filters results by physical object function.
    - **name** (str | None, Query): Filters results by a case-insensitive substring match.
    - **include_child_territories** (bool, Query): If True, includes data from child territories (default: true).
      Note: This can be unsafe for high-level territories due to potential performance issues.
    - **cities_only** (bool, Query): If True, retrieves data only for cities (default: false).
    - **order_by** (PhysicalObjectsOrderByField, Query): Defines the sorting attribute - physical_object_id (default), created_at or updated_at.
    - **ordering** (Ordering, Query): Specifies sorting order - ascending (default) or descending.
    - **cursor** (str, Query): Cursor (encrypted `physical_object_id`) for the next page.
    - **page_size** (int, Query): Defines the number of physical objects per page (default: 10).

    ### Returns:
    - **CursorPage[PhysicalObjectWithGeometry]**: A paginated list of physical objects, including cursor-based pagination data.

    ### Errors:
    - **400 Bad Request**: If `cities_only` is set to True and `include_child_territories` is set to False or
    set both `physical_object_type_id` and `physical_object_function_id`.
    - **404 Not Found**: If the territory does not exist.
    """
    territories_service: TerritoriesService = request.state.territories_service

    if not include_child_territories and cities_only:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="You can use cities_only parameter only with including child territories",
        )

    if physical_object_type_id is not None and physical_object_function_id is not None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Please, choose either physical_object_type_id or physical_object_function_id",
        )

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
