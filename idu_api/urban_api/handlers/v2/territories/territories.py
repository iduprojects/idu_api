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
    parent_id: int = Query(
        None,
        description="Parent territory id to filter, should be skipped to get top level territories",
    ),
    get_all_levels: bool = Query(
        False, description="Getting full subtree of territories (unsafe for high level parents)"
    ),
    territory_type_id: int | None = Query(None, description="Specifying territory type"),
) -> CursorPage[TerritoryData]:
    """Get a territory or list of territories by parent.

    Territory type could be specified in parameters.
    """
    territories_service: TerritoriesService = request.state.territories_service

    territories = await territories_service.get_territories_by_parent_id(
        parent_id, get_all_levels, territory_type_id, paginate=True
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
        None, description="Parent territory id to filter, should be skipped to get top level territories"
    ),
    get_all_levels: bool = Query(
        False, description="Getting full subtree of territories (unsafe for high level parents)"
    ),
    order_by: TerritoriesOrderByField = Query(  # should be Optional, but swagger is generated wrongly then
        None, description="Attribute to set ordering (created_at or updated_at)"
    ),
    ordering: Ordering = Query(
        Ordering.ASC, description="Order type (ascending or descending) if ordering field is set"
    ),
    created_at: date | None = Query(None, description="Filter by created date"),
    name: str | None = Query(None, description="Filter territories by name substring (case-insensitive)"),
) -> CursorPage[TerritoryWithoutGeometry]:
    """Get territories by parent id."""
    territories_service: TerritoriesService = request.state.territories_service

    order_by_value = order_by.value if order_by is not None else "null"

    territories = await territories_service.get_territories_without_geometry_by_parent_id(
        parent_id, get_all_levels, order_by_value, created_at, name, ordering.value, paginate=True
    )

    return paginate(
        territories.items,
        territories.total,
        transformer=lambda x: [TerritoryWithoutGeometry.from_dto(item) for item in x],
        additional_data=territories.cursor_data,
    )