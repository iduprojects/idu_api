"""Territories endpoints are defined here."""

from datetime import date

from fastapi import Depends, HTTPException, Path, Query
from fastapi_pagination import paginate
from sqlalchemy.ext.asyncio import AsyncConnection
from starlette import status

from urban_api.db.connection import get_connection
from urban_api.logic.territories import (
    add_territory_to_db,
    get_common_territory_for_geometry,
    get_intersecting_territories_for_geometry,
    get_territories_by_parent_id_from_db,
    get_territories_without_geometry_by_parent_id_from_db,
    get_territory_by_id_from_db,
    patch_territory_to_db,
    put_territory_to_db,
)
from urban_api.schemas import (
    TerritoriesData,
    TerritoriesDataPatch,
    TerritoriesDataPost,
    TerritoriesDataPut,
    TerritoryWithoutGeometry,
)
from urban_api.schemas.enums import Ordering
from urban_api.schemas.geometries import Geometry
from urban_api.schemas.pages import Page
from urban_api.schemas.territories import TerritoriesOrderByField

from .routers import territories_router


@territories_router.get(
    "/territory",
    response_model=TerritoriesData,
    status_code=status.HTTP_200_OK,
)
async def get_territory_by_id(
    territory_id: int = Query(description="territory id", gt=0),
    connection: AsyncConnection = Depends(get_connection),
) -> TerritoriesData:
    """
    Summary:
        Get single territory

    Description:
        Get a territory by id
    """

    territory = await get_territory_by_id_from_db(territory_id, connection)

    return TerritoriesData.from_dto(territory)


@territories_router.post(
    "/territory",
    response_model=TerritoriesData,
    status_code=status.HTTP_201_CREATED,
)
async def add_territory(
    territory: TerritoriesDataPost, connection: AsyncConnection = Depends(get_connection)
) -> TerritoriesData:
    """
    Summary:
        Add territory

    Description:
        Add a territory
    """

    territory_dto = await add_territory_to_db(territory, connection)

    return TerritoriesData.from_dto(territory_dto)


@territories_router.get(
    "/territories",
    response_model=Page[TerritoriesData],
    status_code=status.HTTP_200_OK,
)
async def get_territory_by_parent_id(
    parent_id: int = Query(
        None,
        description="Parent territory id to filter, should be skipped to get top level territories",
    ),
    get_all_levels: bool = Query(
        False, description="Getting full subtree of territories (unsafe for high level parents)"
    ),
    territory_type_id: int | None = Query(None, description="Specifying territory type"),
    connection: AsyncConnection = Depends(get_connection),
) -> Page[TerritoriesData]:
    """
    Summary:
        Get territories by parent id

    Description:
        Get a territory or list of territories by parent, territory type could be specified in parameters
    """

    territories = await get_territories_by_parent_id_from_db(parent_id, connection, get_all_levels, territory_type_id)
    territories = [TerritoriesData.from_dto(territory) for territory in territories]

    return paginate(territories)


@territories_router.get(
    "/territories_without_geometry",
    response_model=Page[TerritoryWithoutGeometry],
    status_code=status.HTTP_200_OK,
)
async def get_territory_without_geometry_by_parent_id(
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
    connection: AsyncConnection = Depends(get_connection),
) -> Page[TerritoryWithoutGeometry]:
    """
    Summary:
        Get territories by parent id

    Description:
        Get a territory or list of territories without geometry by parent
    """

    order_by_value = order_by.value if order_by is not None else "null"

    territories = await get_territories_without_geometry_by_parent_id_from_db(
        parent_id, connection, get_all_levels, order_by_value, created_at, name, ordering.value
    )

    results = [TerritoryWithoutGeometry.from_dto(territory) for territory in territories]

    return paginate(results)


@territories_router.post(
    "/common_territory",
    response_model=TerritoriesData,
    status_code=status.HTTP_200_OK,
)
async def get_common_territory(
    geometry: Geometry,
    connection: AsyncConnection = Depends(get_connection),
) -> TerritoriesData:
    """
    Summary:
        Get common territory

    Description:
        Get a territory which covers given geometry fully
    """

    territory = await get_common_territory_for_geometry(connection, geometry.as_shapely_geometry())

    if territory is None:
        raise HTTPException(404, "no common territory exists in the database")

    return TerritoriesData.from_dto(territory)


@territories_router.post(
    "/territory/{parent_territory_id}/intersecting_territories",
    response_model=list[TerritoriesData],
    status_code=status.HTTP_200_OK,
)
async def intersecting_territories(
    geometry: Geometry,
    parent_territory_id: int = Path(description="parent territory id", gt=0),
    connection: AsyncConnection = Depends(get_connection),
) -> list[TerritoriesData]:
    """
    Summary:
        Get overlapping territories

    Description:
        Get list of inner territories of a given parent territory which intersect with given geometry.
    """

    territories = await get_intersecting_territories_for_geometry(
        connection, parent_territory_id, geometry.as_shapely_geometry()
    )

    return [TerritoriesData.from_dto(territory) for territory in territories]


@territories_router.put(
    "/territory/{territory_id}",
    response_model=TerritoriesData,
    status_code=status.HTTP_201_CREATED,
)
async def put_territory(
    territory: TerritoriesDataPut,
    territory_id: int = Path(description="territory id", gt=0),
    connection: AsyncConnection = Depends(get_connection),
) -> TerritoriesData:
    """
    Summary:
        Put territory

    Description:
        Put a territory
    """

    territory_dto = await put_territory_to_db(territory_id, territory, connection)

    return TerritoriesData.from_dto(territory_dto)


@territories_router.patch(
    "/territory/{territory_id}",
    response_model=TerritoriesData,
    status_code=status.HTTP_201_CREATED,
)
async def patch_territory(
    territory: TerritoriesDataPatch,
    territory_id: int = Path(description="territory id", gt=0),
    connection: AsyncConnection = Depends(get_connection),
) -> TerritoriesData:
    """
    Summary:
        Patch territory

    Description:
        Patch a territory
    """

    territory_dto = await patch_territory_to_db(territory_id, territory, connection)

    return TerritoriesData.from_dto(territory_dto)
