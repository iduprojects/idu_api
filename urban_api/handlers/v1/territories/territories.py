"""Territories handlers are defined here."""

from datetime import date

from fastapi import HTTPException, Path, Query, Request
from fastapi_pagination import paginate
from starlette import status

from urban_api.logic.territories import TerritoriesService
from urban_api.schemas import (
    TerritoryData,
    TerritoryDataPatch,
    TerritoryDataPost,
    TerritoryDataPut,
    TerritoryWithoutGeometry,
)
from urban_api.schemas.enums import Ordering
from urban_api.schemas.geometries import Geometry
from urban_api.schemas.pages import Page
from urban_api.schemas.territories import TerritoriesOrderByField

from .routers import territories_router


@territories_router.get(
    "/territory/{territory_id}",
    response_model=TerritoryData,
    status_code=status.HTTP_200_OK,
)
async def get_territory_by_id(
    request: Request,
    territory_id: int = Path(description="territory id", gt=0),
) -> TerritoryData:
    """Get a territory by id."""
    territories_service: TerritoriesService = request.state.territories_service

    territory = await territories_service.get_territory_by_id(territory_id)

    return TerritoryData.from_dto(territory)


@territories_router.post(
    "/territory",
    response_model=TerritoryData,
    status_code=status.HTTP_201_CREATED,
)
async def add_territory(
    request: Request,
    territory: TerritoryDataPost,
) -> TerritoryData:
    """Add territory."""
    territories_service: TerritoriesService = request.state.territories_service

    territory_dto = await territories_service.add_territory(territory)

    return TerritoryData.from_dto(territory_dto)


@territories_router.get(
    "/territories",
    response_model=Page[TerritoryData],
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
) -> Page[TerritoryData]:
    """.Get a territory or list of territories by parent.

    territory type could be specified in parameters.
    """
    territories_service: TerritoriesService = request.state.territories_service

    territories = await territories_service.get_territories_by_parent_id(parent_id, get_all_levels, territory_type_id)
    territories = [TerritoryData.from_dto(territory) for territory in territories]

    return paginate(territories)


@territories_router.get(
    "/territories_without_geometry",
    response_model=Page[TerritoryWithoutGeometry],
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
) -> Page[TerritoryWithoutGeometry]:
    """Get territories by parent id."""
    territories_service: TerritoriesService = request.state.territories_service

    order_by_value = order_by.value if order_by is not None else "null"

    territories = await territories_service.get_territories_without_geometry_by_parent_id(
        parent_id, get_all_levels, order_by_value, created_at, name, ordering.value
    )

    results = [TerritoryWithoutGeometry.from_dto(territory) for territory in territories]

    return paginate(results)


@territories_router.post(
    "/common_territory",
    response_model=TerritoryData,
    status_code=status.HTTP_200_OK,
)
async def get_common_territory(
    request: Request,
    geometry: Geometry,
) -> TerritoryData:
    """Get the most deep territory which fully covers given geometry."""
    territories_service: TerritoriesService = request.state.territories_service

    territory = await territories_service.get_common_territory_for_geometry(geometry.as_shapely_geometry())

    if territory is None:
        raise HTTPException(404, "no common territory exists in the database")

    return TerritoryData.from_dto(territory)


@territories_router.post(
    "/territory/{parent_territory_id}/intersecting_territories",
    response_model=list[TerritoryData],
    status_code=status.HTTP_200_OK,
)
async def intersecting_territories(
    request: Request,
    geometry: Geometry,
    parent_territory_id: int = Path(description="parent territory id", gt=0),
) -> list[TerritoryData]:
    """Get list of inner territories of a given parent territory which intersect with given geometry."""
    territories_service: TerritoriesService = request.state.territories_service

    territories = await territories_service.get_intersecting_territories_for_geometry(
        parent_territory_id, geometry.as_shapely_geometry()
    )

    return [TerritoryData.from_dto(territory) for territory in territories]


@territories_router.put(
    "/territory/{territory_id}",
    response_model=TerritoryData,
    status_code=status.HTTP_201_CREATED,
)
async def put_territory(
    request: Request,
    territory: TerritoryDataPut,
    territory_id: int = Path(description="territory id", gt=0),
) -> TerritoryData:
    """Update the given territory - all attributes."""
    territories_service: TerritoriesService = request.state.territories_service

    territory_dto = await territories_service.put_territory(territory_id, territory)

    return TerritoryData.from_dto(territory_dto)


@territories_router.patch(
    "/territory/{territory_id}",
    response_model=TerritoryData,
    status_code=status.HTTP_201_CREATED,
)
async def patch_territory(
    request: Request,
    territory: TerritoryDataPatch,
    territory_id: int = Path(description="territory id", gt=0),
) -> TerritoryData:
    """Update the given territory - only given attributes."""
    territories_service: TerritoriesService = request.state.territories_service

    territory_dto = await territories_service.patch_territory(territory_id, territory)

    return TerritoryData.from_dto(territory_dto)
