"""Functional zones territories-related handlers are defined here."""

from fastapi import HTTPException, Path, Query, Request
from geojson_pydantic import Feature
from geojson_pydantic.geometries import Geometry
from starlette import status

from idu_api.urban_api.logic.territories import TerritoriesService
from idu_api.urban_api.schemas import FunctionalZone, FunctionalZoneSource, FunctionalZoneWithoutGeometry, OkResponse
from idu_api.urban_api.schemas.geometries import GeoJSONResponse

from .routers import territories_router


@territories_router.get(
    "/territory/{territory_id}/functional_zone_sources",
    response_model=list[FunctionalZoneSource],
    status_code=status.HTTP_200_OK,
)
async def get_functional_zone_sources_by_territory_id(
    request: Request,
    territory_id: int = Path(..., description="territory identifier", gt=0),
    include_child_territories: bool = Query(True, description="to get from child territories"),
    cities_only: bool = Query(False, description="to get only for cities"),
) -> list[FunctionalZoneSource]:
    """
    ## Get sources of functional zones for a given territory.

    **WARNING:** Set `cities_only = True` only if you want to get entities from child territories.

    ### Parameters:
    - **territory_id** (int, Path): Unique identifier of the territory.
    - **include_child_territories** (bool, Query): If True, includes data from child territories (default: true).
    - **cities_only** (bool, Query): If True, retrieves data only for cities (default: false).

    ### Returns:
    - **list[FunctionalZoneSource]**: A list of functional zone sources, each represented as a (year, source) pair.

    ### Errors:
    - **400 Bad Request**: If `cities_only` is set to True and `include_child_territories` is set to False.
    - **404 Not Found**: If the territory does not exist.
    """
    territories_service: TerritoriesService = request.state.territories_service

    if not include_child_territories and cities_only:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="You can use cities_only parameter only with including child territories",
        )

    sources = await territories_service.get_functional_zones_sources_by_territory_id(
        territory_id, include_child_territories, cities_only
    )

    return [FunctionalZoneSource.from_dto(source) for source in sources]


@territories_router.get(
    "/territory/{territory_id}/functional_zones",
    response_model=list[FunctionalZone],
    status_code=status.HTTP_200_OK,
)
async def get_functional_zones_by_territory_id(
    request: Request,
    territory_id: int = Path(..., description="territory identifier", gt=0),
    year: int = Query(..., description="to filter by year when zones were uploaded"),
    source: str = Query(..., description="to filter by source from which zones were uploaded"),
    functional_zone_type_id: int | None = Query(None, description="to filter by functional zone type", gt=0),
    include_child_territories: bool = Query(
        True, description="to get from child territories (unsafe for high level territories)"
    ),
    cities_only: bool = Query(False, description="to get only for cities"),
) -> list[FunctionalZone]:
    """
    ## Get functional zones for a given territory.

    **WARNING:** Set `cities_only = True` only if you want to get entities from child territories.

    ### Parameters:
    - **territory_id** (int, Path): Unique identifier of the territory.
    - **year** (int, Query): Filters results by the year zones were uploaded.
    - **source** (str, Query): Filters results by the source from which zones were uploaded.
    - **functional_zone_type_id** (int | None, Query): Filters results by functional zone type.
    - **include_child_territories** (bool, Query): If True, includes data from child territories (default: True).
      Note: This can be unsafe for high-level territories due to potential performance issues.
    - **cities_only** (bool, Query): If True, retrieves data only for cities (default: false).

    ### Returns:
    - **list[FunctionalZone]**: A list of functional zones matching the filters.

    ### Errors:
    - **400 Bad Request**: If `cities_only` is set to True and `include_child_territories` is set to False.
    - **404 Not Found**: If the territory does not exist.
    """
    territories_service: TerritoriesService = request.state.territories_service

    if not include_child_territories and cities_only:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="You can use cities_only parameter only with including child territories",
        )

    zones = await territories_service.get_functional_zones_by_territory_id(
        territory_id, year, source, functional_zone_type_id, include_child_territories, cities_only
    )

    return [FunctionalZone.from_dto(zone) for zone in zones]


@territories_router.get(
    "/territory/{territory_id}/functional_zones_geojson",
    response_model=GeoJSONResponse[Feature[Geometry, FunctionalZoneWithoutGeometry]],
    status_code=status.HTTP_200_OK,
)
async def get_functional_zones_geojson_by_territory_id(
    request: Request,
    territory_id: int = Path(..., description="territory identifier", gt=0),
    year: int = Query(..., description="to filter by year when zones were uploaded"),
    source: str = Query(..., description="to filter by source from which zones were uploaded"),
    functional_zone_type_id: int | None = Query(None, description="to filter by functional zone type", gt=0),
    include_child_territories: bool = Query(
        True, description="to get from child territories (unsafe for high level territories)"
    ),
    cities_only: bool = Query(False, description="to get only for cities"),
) -> GeoJSONResponse[Feature[Geometry, FunctionalZoneWithoutGeometry]]:
    """
    ## Get functional zones in GeoJSON format for a given territory.

    **WARNING:** Set cities_only = True only if you want to get entities from child territories.

    ### Parameters:
    - **territory_id** (int, Path): Unique identifier of the territory.
    - **year** (int, Query): Filters results by the year zones were uploaded.
    - **source** (str, Query): Filters results by the source from which zones were uploaded.
    - **functional_zone_type_id** (int | None, Query): Filters results by functional zone type.
    - **include_child_territories** (bool, Query): If True, includes data from child territories (default: True).
      Note: This can be unsafe for high-level territories due to potential performance issues.
    - **cities_only** (bool, Query): If True, retrieves data only for cities (default: false).

    ### Returns:
    - **GeoJSONResponse[Feature[Geometry, FunctionalZoneWithoutGeometry]]**: A GeoJSON response containing functional zones.

    ### Errors:
    - **400 Bad Request**: If `cities_only` is set to True and `include_child_territories` is set to False.
    - **404 Not Found**: If the territory does not exist.
    """
    territories_service: TerritoriesService = request.state.territories_service

    if not include_child_territories and cities_only:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="You can use cities_only parameter only with including child territories",
        )

    zones = await territories_service.get_functional_zones_by_territory_id(
        territory_id, year, source, functional_zone_type_id, include_child_territories, cities_only
    )

    return await GeoJSONResponse.from_list([zone.to_geojson_dict() for zone in zones])


@territories_router.delete(
    "/territory/{territory_id}/functional_zones",
    response_model=OkResponse,
    status_code=status.HTTP_200_OK,
)
async def delete_all_functional_zones_by_territory_id(
    request: Request,
    territory_id: int = Path(..., description="territory identifier", gt=0),
    include_child_territories: bool = Query(True, description="to get from child territories"),
    cities_only: bool = Query(False, description="to get only for cities"),
) -> OkResponse:
    """
    ## Delete all functional zones for a given territory.

    **WARNING:** Set cities_only = True only if you want to get entities from child territories.

    ### Parameters:
    - **territory_id** (int, Path): Unique identifier of the territory.
    - **include_child_territories** (bool, Query): If True, includes data from child territories (default: True).
    - **cities_only** (bool, Query): If True, applies deletion only to cities (default: false).

    ### Returns:
    - **OkResponse**: A confirmation message of the deletion.

    ### Errors:
    - **400 Bad Request**: If `cities_only` is set to True and `include_child_territories` is set to False.
    - **404 Not Found**: If the territory does not exist.
    """
    territories_service: TerritoriesService = request.state.territories_service

    if not include_child_territories and cities_only:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="You can use cities_only parameter only with including child territories",
        )

    await territories_service.delete_all_functional_zones_for_territory(
        territory_id, include_child_territories, cities_only
    )

    return OkResponse()
