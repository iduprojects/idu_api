"""Territories handlers (v2) are defined here."""

from datetime import date

from fastapi import Query, Request
from starlette import status

from idu_api.urban_api.logic.territories import TerritoriesService
from idu_api.urban_api.schemas import (
    TerritoryData,
    TerritoryWithoutGeometry,
)
from idu_api.urban_api.schemas.enums import Ordering
from idu_api.urban_api.schemas.pages import CursorPage
from idu_api.urban_api.schemas.territories import TerritoriesOrderByField
from idu_api.urban_api.utils.pagination import paginate

from .routers import territories_router


@territories_router.get(
    "/territories",
    response_model=CursorPage[TerritoryData],
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
    order_by: TerritoriesOrderByField = Query(  # should be Optional, but swagger is generated wrongly then
        None, description="attribute to set ordering (created_at or updated_at)"
    ),
    ordering: Ordering = Query(
        Ordering.ASC, description="order type (ascending or descending) if ordering field is set"
    ),
) -> CursorPage[TerritoryData]:
    """Get a territory or list of territories by parent.

    Territory type could be specified in parameters.
    """
    territories_service: TerritoriesService = request.state.territories_service

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
        transformer=lambda x: [TerritoryData.from_dto(item) for item in x],
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
    order_by: TerritoriesOrderByField = Query(  # should be Optional, but swagger is generated wrongly then
        None, description="attribute to set ordering (created_at or updated_at)"
    ),
    ordering: Ordering = Query(
        Ordering.ASC, description="order type (ascending or descending) if ordering field is set"
    ),
) -> CursorPage[TerritoryWithoutGeometry]:
    """Get territories by parent id."""
    territories_service: TerritoriesService = request.state.territories_service

    order_by_value = order_by.value if order_by is not None else "null"

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
