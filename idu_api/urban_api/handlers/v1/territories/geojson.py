"""Territories geojson handlers are defined here."""

from fastapi import Path, Request
from geojson_pydantic import Feature
from geojson_pydantic.geometries import Geometry
from starlette import status

from idu_api.urban_api.logic.territories import TerritoriesService
from idu_api.urban_api.schemas import TerritoryWithoutGeometry
from idu_api.urban_api.schemas.geometries import GeoJSONResponse

from .routers import territories_router


@territories_router.get(
    "/territory/{territory_id}/territory_geojson",
    response_model=GeoJSONResponse[Feature[Geometry, TerritoryWithoutGeometry]],
    status_code=status.HTTP_200_OK,
)
async def get_territory_geojson_by_territory_id(
    request: Request,
    territory_id: int = Path(..., description="territory id", gt=0),
) -> GeoJSONResponse[Feature[Geometry, TerritoryWithoutGeometry]]:
    """Get geojson for a given territory."""
    territories_service: TerritoriesService = request.state.territories_service

    territory = await territories_service.get_territory_geojson_by_territory_id(territory_id)

    return await GeoJSONResponse.from_list([territory.to_geojson_dict()])
