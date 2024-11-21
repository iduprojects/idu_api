"""Hexagons territories-related handlers are defined here."""

from fastapi import Path, Request
from starlette import status

from idu_api.urban_api.logic.territories import TerritoriesService
from idu_api.urban_api.schemas import Hexagon, HexagonPost

from .routers import territories_router


@territories_router.get(
    "/territory/{territory_id}/hexagons",
    response_model=list[Hexagon],
    status_code=status.HTTP_200_OK,
)
async def get_hexagons_by_territory_id(
    request: Request,
    territory_id: int = Path(..., description="territory id", gt=0),
) -> list[Hexagon]:
    """Get hexagons for a given territory."""
    territories_service: TerritoriesService = request.state.territories_service

    hexagons = await territories_service.get_hexagons_by_territory_id(territory_id)

    return [Hexagon.from_dto(hexagon) for hexagon in hexagons]


@territories_router.post(
    "/territory/{territory_id}/hexagons",
    response_model=list[Hexagon],
    status_code=status.HTTP_201_CREATED,
)
async def add_hexagons(
    request: Request,
    hexagons: list[HexagonPost],
    territory_id: int = Path(..., description="territory id", gt=0),
) -> list[Hexagon]:
    """Create hexagons for a given territory."""

    territories_service: TerritoriesService = request.state.territories_service

    hexagons = await territories_service.add_hexagons(territory_id, hexagons)

    return [Hexagon.from_dto(hexagon) for hexagon in hexagons]


@territories_router.delete(
    "/territory/{territory_id}/hexagons",
    response_model=dict,
    status_code=status.HTTP_200_OK,
)
async def get_hexagons_by_territory_id(
    request: Request,
    territory_id: int = Path(..., description="territory id", gt=0),
) -> dict:
    """Delete hexagons for a given territory."""
    territories_service: TerritoriesService = request.state.territories_service

    return await territories_service.delete_hexagons_by_territory_id(territory_id)
