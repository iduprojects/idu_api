"""Territories handlers (v2) are defined here."""

from datetime import date

from fastapi import HTTPException, Query, Request
from starlette import status

from idu_api.urban_api.logic.territories import TerritoriesService
from idu_api.urban_api.schemas import (
    Territory,
    TerritoryWithoutGeometry,
)
from idu_api.urban_api.schemas.enums import OrderByField, Ordering
from idu_api.urban_api.schemas.pages import CursorPage
from idu_api.urban_api.utils.pagination import paginate

from .routers import territories_router


@territories_router.get(
    "/territories",
    response_model=CursorPage[Territory],
    status_code=status.HTTP_200_OK,
)
async def get_territory_by_parent_id(
    request: Request,
    parent_id: int | None = Query(
        None, description="parent territory identifier to filter, should be skipped to get top level territories"
    ),
    get_all_levels: bool = Query(
        False, description="getting full subtree of territories (unsafe for high level parents)"
    ),
    territory_type_id: int | None = Query(None, description="to filter by territory type"),
    name: str | None = Query(None, description="to filter territories by name substring (case-insensitive)"),
    cities_only: bool = Query(False, description="to get only for cities"),
    created_at: date | None = Query(None, description="to filter by created date"),
    order_by: OrderByField = Query(  # should be Optional, but swagger is generated wrongly then
        None, description="attribute to set ordering (created_at or updated_at)"
    ),
    ordering: Ordering = Query(
        Ordering.ASC, description="order type (ascending or descending) if ordering field is set"
    ),
) -> CursorPage[Territory]:
    """
    ## Get a paginated list of territories by parent identifier.

    **WARNING:** Set `cities_only = True` only if you want to get entities from all levels.

    ### Parameters:
    - **parent_id** (int | None, Query): Unique identifier of the parent territory. If skipped, returns the highest level territories.
    - **get_all_levels** (bool, Query): If True, retrieves the full subtree of territories (default: false).
      Note: This can be unsafe for high-level territories due to potential performance issues.
    - **territory_type_id** (int | None, Query): Filters results by territory type.
    - **name** (str | None, Query): Filters results by a case-insensitive substring match.
    - **cities_only** (bool, Query): If True, retrieves data only for cities (default: false).
    - **created_at** (date | None, Query): Returns territories created at the specified date.
    - **order_by** (OrderByField, Query): Defines the sorting attribute - territory_id (default), created_at or updated_at.
    - **ordering** (Ordering, Query): Specifies sorting order - ascending (default) or descending.
    - **cursor** (str, Query): Cursor (encrypted `territory_id`) for the next page.
    - **page_size** (int, Query): Defines the number of territories per page (default: 10).

    ### Returns:
    - **CursorPage[Territory]**: A paginated list of territories, including cursor-based pagination data.

    ### Errors:
    - **400 Bad Request**: If `cities_only` is set to True and `get_all_levels` is set to False.
    - **404 Not Found**: If the parent territory does not exist.
    """
    territories_service: TerritoriesService = request.state.territories_service

    if not get_all_levels and cities_only:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="You can use cities_only parameter only with including all levels",
        )

    order_by_value = order_by.value if order_by is not None else None

    territories = await territories_service.get_territories_by_parent_id(
        parent_id,
        get_all_levels,
        territory_type_id,
        name,
        cities_only,
        created_at,
        order_by_value,
        ordering.value,
        paginate=True,
    )

    return paginate(
        territories.items,
        territories.total,
        transformer=lambda x: [Territory.from_dto(item) for item in x],
        additional_data=territories.cursor_data,
    )


@territories_router.get(
    "/territories_without_geometry",
    response_model=CursorPage[TerritoryWithoutGeometry],
    status_code=status.HTTP_200_OK,
)
async def get_territory_without_geometry_by_parent_id(
    request: Request,
    parent_id: int | None = Query(
        None, description="parent territory identifier to filter, should be skipped to get top level territories"
    ),
    get_all_levels: bool = Query(
        False, description="getting full subtree of territories (unsafe for high level parents)"
    ),
    territory_type_id: int | None = Query(None, description="to filter by territory type"),
    name: str | None = Query(None, description="to filter territories by name substring (case-insensitive)"),
    cities_only: bool = Query(False, description="to get only for cities"),
    created_at: date | None = Query(None, description="to filter by created date"),
    order_by: OrderByField = Query(  # should be Optional, but swagger is generated wrongly then
        None, description="attribute to set ordering (created_at or updated_at)"
    ),
    ordering: Ordering = Query(
        Ordering.ASC, description="order type (ascending or descending) if ordering field is set"
    ),
) -> CursorPage[TerritoryWithoutGeometry]:
    """
    ## Get a paginated list of territories without geometry by parent identifier.

    **WARNING:** Set `cities_only = True` only if you want to get entities from all levels.

    ### Parameters:
    - **parent_id** (int | None, Query): Unique identifier of the parent territory. If none, returns the highest level territories.
    - **get_all_levels** (bool, Query): If True, retrieves the full subtree of territories (default: false).
      Note: This can be unsafe for high-level territories due to potential performance issues.
    - **territory_type_id** (int | None, Query): Filters results by territory type.
    - **name** (str | None, Query): Filters results by a case-insensitive substring match.
    - **cities_only** (bool, Query): If True, retrieves data only for cities (default: false).
    - **created_at** (date | None, Query): Returns territories created at the specified date.
    - **order_by** (OrderByField, Query): Defines the sorting attribute - territory_id (default), created_at or updated_at.
    - **ordering** (Ordering, Query): Specifies sorting order - ascending (default) or descending.
    - **cursor** (str, Query): Cursor (encrypted `territory_id`) for the next page.
    - **page_size** (int, Query): Defines the number of territories per page (default: 10).

    ### Returns:
    - **CursorPage[Territory]**: A paginated list of territories, including cursor-based pagination data.

    ### Errors:
    - **400 Bad Request**: If `cities_only` is set to True and `get_all_levels` is set to False.
    - **404 Not Found**: If the parent territory does not exist.
    """
    territories_service: TerritoriesService = request.state.territories_service

    if not get_all_levels and cities_only:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="You can use cities_only parameter only with including all levels",
        )

    order_by_value = order_by.value if order_by is not None else None

    territories = await territories_service.get_territories_without_geometry_by_parent_id(
        parent_id,
        get_all_levels,
        territory_type_id,
        name,
        cities_only,
        created_at,
        order_by_value,
        ordering.value,
        paginate=True,
    )

    return paginate(
        territories.items,
        territories.total,
        transformer=lambda x: [TerritoryWithoutGeometry.from_dto(item) for item in x],
        additional_data=territories.cursor_data,
    )
