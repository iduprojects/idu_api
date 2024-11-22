"""Hexagons territories-related handlers are defined here."""

from fastapi import Path, Query, Request
from geojson_pydantic.geometries import Geometry
from starlette import status

from idu_api.urban_api.logic.territories import TerritoriesService
from idu_api.urban_api.schemas import Hexagon, HexagonAttributes, HexagonPost
from idu_api.urban_api.schemas.geometries import Feature, GeoJSONResponse

from .routers import territories_router


@territories_router.get(
    "/territory/{territory_id}/hexagons",
    response_model=GeoJSONResponse[Feature[Geometry, HexagonAttributes]],
    status_code=status.HTTP_200_OK,
)
async def get_hexagons_by_territory_id(
    request: Request,
    territory_id: int = Path(..., description="territory identifier", gt=0),
    centers_only: bool = Query(False, description="display only centers"),
) -> GeoJSONResponse[Feature[Geometry, HexagonAttributes]]:
    """Get list of hexagons for a given territory in geojson format."""
    territories_service: TerritoriesService = request.state.territories_service

    hexagons = await territories_service.get_hexagons_by_territory_id(territory_id)

    return await GeoJSONResponse.from_list([hexagon.to_geojson_dict() for hexagon in hexagons], centers_only)


@territories_router.post(
    "/territory/{territory_id}/hexagons",
    response_model=list[Hexagon],
    status_code=status.HTTP_201_CREATED,
)
async def add_hexagons_by_territory_id(
    request: Request,
    hexagons: list[HexagonPost],
    territory_id: int = Path(..., description="territory identifier", gt=0),
) -> list[Hexagon]:
    """Create hexagons for a given territory."""

    territories_service: TerritoriesService = request.state.territories_service

    hexagons = await territories_service.add_hexagons_by_territory_id(territory_id, hexagons)

    return [Hexagon.from_dto(hexagon) for hexagon in hexagons]


@territories_router.delete(
    "/territory/{territory_id}/hexagons",
    response_model=dict,
    status_code=status.HTTP_200_OK,
)
async def delete_hexagons_by_territory_id(
    request: Request,
    territory_id: int = Path(..., description="territory identifier", gt=0),
) -> dict:
    """Delete all hexagons for a given territory."""
    territories_service: TerritoriesService = request.state.territories_service

    return await territories_service.delete_hexagons_by_territory_id(territory_id)
