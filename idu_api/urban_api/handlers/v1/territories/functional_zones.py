"""Functional zones territories-related handlers are defined here."""

from fastapi import Path, Query, Request
from geojson_pydantic import Feature
from geojson_pydantic.geometries import Geometry
from starlette import status

from idu_api.urban_api.logic.territories import TerritoriesService
from idu_api.urban_api.schemas import FunctionalZoneData, FunctionalZoneSource, FunctionalZoneWithoutGeometry
from idu_api.urban_api.schemas.geometries import GeoJSONResponse

from .routers import territories_router


@territories_router.get(
    "/territory/{territory_id}/functional_zone_sources",
    response_model=list[FunctionalZoneSource],
    status_code=status.HTTP_200_OK,
)
async def get_functional_zone_sources_by_territory_id(
    request: Request,
    territory_id: int = Path(..., description="territory identifier"),
) -> list[FunctionalZoneSource]:
    """Get list of pairs (year, source) of functional zones by territory identifier.

    You must be the owner of the relevant project."""
    territories_service: TerritoriesService = request.state.territories_service

    sources = await territories_service.get_functional_zones_sources_by_territory_id(territory_id)

    return [FunctionalZoneSource.from_dto(source) for source in sources]


@territories_router.get(
    "/territory/{territory_id}/functional_zones",
    response_model=list[FunctionalZoneData],
    status_code=status.HTTP_200_OK,
)
async def get_functional_zones_by_territory_id(
    request: Request,
    territory_id: int = Path(..., description="territory identifier", gt=0),
    year: int = Query(..., description="to filter by year when zones were uploaded"),
    source: str = Query(..., description="to filter by source from which zones were uploaded"),
    functional_zone_type_id: int | None = Query(None, description="to filter by functional zone type", gt=0),
    include_child_territories: bool = Query(True, description="to get from child territories"),
    cities_only: bool = Query(False, description="to get only for cities"),
) -> list[FunctionalZoneData]:
    """Get list of  functional zones by given territory identifier, year and source.

    It could be specified by functional zone type in parameters."""
    territories_service: TerritoriesService = request.state.territories_service

    zones = await territories_service.get_functional_zones_by_territory_id(
        territory_id, year, source, functional_zone_type_id, include_child_territories, cities_only
    )

    return [FunctionalZoneData.from_dto(zone) for zone in zones]


@territories_router.get(
    "/territory/{territory_id}/functional_zones_geojson",
    response_model=GeoJSONResponse[Feature[Geometry, FunctionalZoneWithoutGeometry]],
    status_code=status.HTTP_200_OK,
)
async def get_functional_zones_geojson_by_territory_id(
    request: Request,
    territory_id: int = Path(..., description="territory id", gt=0),
    year: int = Query(..., description="to filter by year when zones were uploaded"),
    source: str = Query(..., description="to filter by source from which zones were uploaded"),
    functional_zone_type_id: int | None = Query(None, description="to filter by functional zone type", gt=0),
    include_child_territories: bool = Query(True, description="to get from child territories"),
    cities_only: bool = Query(False, description="to get only for cities"),
) -> GeoJSONResponse[Feature[Geometry, FunctionalZoneWithoutGeometry]]:
    """Get functional zones by given territory identifier, year and source in geojson format.

    It could be specified by functional zone type in parameters."""
    territories_service: TerritoriesService = request.state.territories_service

    zones = await territories_service.get_functional_zones_by_territory_id(
        territory_id, year, source, functional_zone_type_id, include_child_territories, cities_only
    )

    return await GeoJSONResponse.from_list([zone.to_geojson_dict() for zone in zones])


@territories_router.delete(
    "/territory/{territory_id}/functional_zones",
    response_model=dict,
    status_code=status.HTTP_200_OK,
)
async def delete_all_functional_zones_by_territory_id(
    request: Request,
    territory_id: int = Path(..., description="territory id", gt=0),
) -> dict:
    """Delete all functional zones for territory."""
    territories_service: TerritoriesService = request.state.territories_service

    return await territories_service.delete_all_functional_zones_for_territory(territory_id)
