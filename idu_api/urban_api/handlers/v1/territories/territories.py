"""Territories handlers are defined here."""

from datetime import date

from fastapi import HTTPException, Path, Query, Request
from geojson_pydantic.geometries import Geometry as FeatureGeometry
from starlette import status

from idu_api.urban_api.logic.territories import TerritoriesService
from idu_api.urban_api.schemas import (
    Territory,
    TerritoryPatch,
    TerritoryPost,
    TerritoryPut,
    TerritoryWithoutGeometry,
)
from idu_api.urban_api.schemas.enums import OrderByField, Ordering
from idu_api.urban_api.schemas.geometries import Feature, GeoJSONResponse, Geometry
from idu_api.urban_api.schemas.pages import Page
from idu_api.urban_api.utils.pagination import paginate

from .routers import territories_router


@territories_router.get(
    "/territory/{territory_id}",
    response_model=Territory,
    status_code=status.HTTP_200_OK,
)
async def get_territory_by_id(
    request: Request,
    territory_id: int = Path(..., description="territory identifier", gt=0),
) -> Territory:
    """
    ## Get a territory by its identifier.

    ### Parameters:
    - **territory_id** (int, Path): Unique identifier of the territory.

    ### Returns:
    - **Territory**: The requested territory.

    ### Errors:
    - **404 Not Found**: If the territory does not exist.
    """
    territories_service: TerritoriesService = request.state.territories_service

    territory = await territories_service.get_territory_by_id(territory_id)

    return Territory.from_dto(territory)


@territories_router.post(
    "/territory",
    response_model=Territory,
    status_code=status.HTTP_201_CREATED,
)
async def add_territory(
    request: Request,
    territory: TerritoryPost,
) -> Territory:
    """
    ## Create a new territory.

    ### Parameters:
    - **territory** (TerritoryPost, Body): Data for the new territory.

    ### Returns:
    - **Territory**: The created territory.

    ### Errors:
    - **404 Not Found**: If the related entity does not exist.
    """
    territories_service: TerritoriesService = request.state.territories_service

    territory_dto = await territories_service.add_territory(territory)

    return Territory.from_dto(territory_dto)


@territories_router.put(
    "/territory/{territory_id}",
    response_model=Territory,
    status_code=status.HTTP_201_CREATED,
    deprecated=True,
)
async def put_territory(
    request: Request,
    territory: TerritoryPut,
    territory_id: int = Path(..., description="territory identifier", gt=0),
) -> Territory:
    """
    ## Update a territory by its identifier (full update).

    **WARNING**: This method has been deprecated since version 0.34.0 and will be removed in version 1.0.
    Instead, use PATCH method.

    ### Parameters:
    - **territory_id** (int, Path): Unique identifier of the territory.
    - **territory** (TerritoryPut, Body): Updated data for the territory.

    ### Returns:
    - **Territory**: The updated territory.

    ### Errors:
    - **404 Not Found**: If the territory (or related entity) does not exist.
    """
    territories_service: TerritoriesService = request.state.territories_service

    territory_dto = await territories_service.put_territory(territory_id, territory)

    return Territory.from_dto(territory_dto)


@territories_router.patch(
    "/territory/{territory_id}",
    response_model=Territory,
    status_code=status.HTTP_201_CREATED,
)
async def patch_territory(
    request: Request,
    territory: TerritoryPatch,
    territory_id: int = Path(..., description="territory identifier", gt=0),
) -> Territory:
    """
    ## Partially update a territory by its identifier.

    ### Parameters:
    - **territory_id** (int, Path): Unique identifier of the territory.
    - **territory** (TerritoryPatch, Body): Fields to update in the territory.

    ### Returns:
    - **Territory**: The updated territory with modified fields.

    ### Errors:
    - **404 Not Found**: If the territory does not exist.
    """
    territories_service: TerritoriesService = request.state.territories_service

    territory_dto = await territories_service.patch_territory(territory_id, territory)

    return Territory.from_dto(territory_dto)


@territories_router.get(
    "/territories",
    response_model=Page[Territory],
    status_code=status.HTTP_200_OK,
)
async def get_territories_by_parent_id(
    request: Request,
    parent_id: int | None = Query(
        None, description="parent territory identifier to filter, should be skipped to get top level territories", gt=0
    ),
    get_all_levels: bool = Query(
        False, description="getting full subtree of territories (unsafe for high level parents)"
    ),
    territory_type_id: int | None = Query(None, description="to filter by territory type", gt=0),
    name: str | None = Query(None, description="to filter territories by name substring (case-insensitive)"),
    cities_only: bool = Query(False, description="to get only cities"),
    created_at: date | None = Query(None, description="to filter by created date"),
    order_by: OrderByField = Query(  # should be Optional, but swagger is generated wrongly then
        None, description="attribute to set ordering (created_at or updated_at)"
    ),
    ordering: Ordering = Query(
        Ordering.ASC, description="order type (ascending or descending) if ordering field is set"
    ),
) -> Page[Territory]:
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
    - **page** (int, Query): Specifies the page number for retrieving territories (default: 1).
    - **page_size** (int, Query): Defines the number of territories per page (default: 10).

    ### Returns:
    - **Page[Territory]**: A paginated list of territories.

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
    )


@territories_router.get(
    "/all_territories",
    response_model=GeoJSONResponse[Feature[FeatureGeometry, TerritoryWithoutGeometry]],
    status_code=status.HTTP_200_OK,
)
async def get_all_territories_by_parent_id(
    request: Request,
    parent_id: int | None = Query(
        None, description="parent territory identifier to filter, should be skipped to get top level territories", gt=0
    ),
    get_all_levels: bool = Query(
        False, description="getting full subtree of territories (unsafe for high level parents)"
    ),
    territory_type_id: int | None = Query(None, description="to filter by territory type", gt=0),
    name: str | None = Query(None, description="to filter territories by name substring (case-insensitive)"),
    cities_only: bool = Query(False, description="to get only for cities"),
    created_at: date | None = Query(None, description="to filter by created date"),
    centers_only: bool = Query(False, description="display only centers"),
) -> GeoJSONResponse[Feature[FeatureGeometry, TerritoryWithoutGeometry]]:
    """
    ## Get all territories as a GeoJSON collection by parent identifier.

    **WARNING:** Set `cities_only = True` only if you want to get entities from all levels.

    ### Parameters:
    - **parent_id** (int | None, Query): Unique identifier of the parent territory. If skipped, returns the highest level territories.
    - **get_all_levels** (bool, Query): If True, retrieves the full subtree of territories (default: false).
      Note: This can be unsafe for high-level territories due to potential performance issues.
    - **territory_type_id** (int | None, Query): Filters results by territory type.
    - **name** (str | None, Query): Filters results by a case-insensitive substring match.
    - **cities_only** (bool, Query): If True, retrieves data only for cities (default: false).
    - **created_at** (date | None, Query): Returns territories created at the specified date.
    - **centers_only** (bool, Query): If True, retrieves only center points of territories (default: false).

    ### Returns:
    - **GeoJSONResponse[Feature[FeatureGeometry, TerritoryWithoutGeometry]]**: A GeoJSON response containing territories.

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

    territories = await territories_service.get_territories_by_parent_id(
        parent_id,
        get_all_levels,
        territory_type_id,
        name,
        cities_only,
        created_at,
        None,
        "asc",
        paginate=False,
    )

    return await GeoJSONResponse.from_list([territory.to_geojson_dict() for territory in territories], centers_only)


@territories_router.get(
    "/territories_without_geometry",
    response_model=Page[TerritoryWithoutGeometry],
    status_code=status.HTTP_200_OK,
)
async def get_territory_without_geometry_by_parent_id(
    request: Request,
    parent_id: int | None = Query(
        None, description="parent territory identifier to filter, should be skipped to get top level territories", gt=0
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
) -> Page[TerritoryWithoutGeometry]:
    """
    ## Get a paginated list of territories without geometry by parent identifier.

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
    - **page** (int, Query): Specifies the page number for retrieving territories (default: 1).
    - **page_size** (int, Query): Defines the number of territories per page (default: 10).

    ### Returns:
    - **Page[TerritoryWithoutGeometry]**: A paginated list of territories without geometry.

    ### Errors:
    - **400 Bad Request**: If `cities_only` is set to True and `get_all_levels` is set to False.
    - **404 Not Found**: If the parent territory does not exist.
    """
    territories_service: TerritoriesService = request.state.territories_service

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
    )


@territories_router.get(
    "/all_territories_without_geometry",
    response_model=list[TerritoryWithoutGeometry],
    status_code=status.HTTP_200_OK,
)
async def get_all_territories_without_geometry_by_parent_id(
    request: Request,
    parent_id: int | None = Query(
        None, description="parent territory identifier to filter, should be skipped to get top level territories", gt=0
    ),
    get_all_levels: bool = Query(
        False, description="getting full subtree of territories (unsafe for high level parents)"
    ),
    territory_type_id: int | None = Query(None, description="to filter by territory type", gt=0),
    name: str | None = Query(None, description="to filter territories by name substring (case-insensitive)"),
    cities_only: bool = Query(False, description="to get only for cities"),
    created_at: date | None = Query(None, description="to filter by created date"),
    order_by: OrderByField = Query(  # should be Optional, but swagger is generated wrongly then
        None, description="attribute to set ordering (created_at or updated_at)"
    ),
    ordering: Ordering = Query(
        Ordering.ASC, description="order type (ascending or descending) if ordering field is set"
    ),
) -> list[TerritoryWithoutGeometry]:
    """
    ## Get a list of all territories without geometry by parent identifier.

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

    ### Returns:
    - **list[TerritoryWithoutGeometry]**: A list of territories without geometry.

    ### Errors:
    - **400 Bad Request**: If `cities_only` is set to True and `get_all_levels` is set to False.
    - **404 Not Found**: If the parent territory does not exist.
    """
    territories_service: TerritoriesService = request.state.territories_service

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
        paginate=False,
    )

    return [TerritoryWithoutGeometry.from_dto(territory) for territory in territories]


@territories_router.post(
    "/common_territory",
    response_model=Territory,
    status_code=status.HTTP_200_OK,
)
async def get_common_territory(
    request: Request,
    geometry: Geometry,
) -> Territory:
    """
    ## Get the most deep territory that fully covers a given geometry.

    ### Parameters:
    - **geometry** (Geometry, Body): Geometry to be checked.
      NOTE: The geometry must have **SRID=4326**.

    ### Returns:
    - **Territory**: The most deep territory covering the given geometry.

    ### Errors:
    - **400 Bad Request**: If an invalid geometry is specified.
    - **404 Not Found**: If no matching territory is found.
    """
    territories_service: TerritoriesService = request.state.territories_service

    try:
        shapely_geom = geometry.as_shapely_geometry()
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e)) from e

    territory = await territories_service.get_common_territory_for_geometry(shapely_geom)

    if territory is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No common territory exists in the database")

    return Territory.from_dto(territory)


@territories_router.post(
    "/territory/{parent_territory_id}/intersecting_territories",
    response_model=list[Territory],
    status_code=status.HTTP_200_OK,
)
async def intersecting_territories(
    request: Request,
    geometry: Geometry,
    parent_territory_id: int = Path(..., description="parent territory identifier", gt=0),
) -> list[Territory]:
    """
    ## Get list of inner territories (only at level of given parent + 1) of a given parent territory which intersect with given geometry.

    ### Parameters:
    - **parent_territory_id** (int, Path): Unique identifier of the parent territory.
    - **geometry** (Geometry, Body): Geometry to be checked.
      NOTE: The geometry must have **SRID=4326**.

    ### Returns:
    - **list[Territory]**: A list of territories intersecting with the given geometry.

    ### Errors:
    - **400 Bad Request**: If an invalid geometry is specified.
    - **404 Not Found**: If the parent territory does not exist.
    """
    territories_service: TerritoriesService = request.state.territories_service

    try:
        shapely_geom = geometry.as_shapely_geometry()
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e)) from e

    territories = await territories_service.get_intersecting_territories_for_geometry(parent_territory_id, shapely_geom)

    return [Territory.from_dto(territory) for territory in territories]
