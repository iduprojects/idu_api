"""Buildings territories-related handlers are defined here."""

from fastapi import Path, Request
from fastapi_pagination import paginate
from starlette import status

from urban_api.logic.territories import TerritoriesService
from urban_api.schemas import LivingBuildingsWithGeometry
from urban_api.schemas.pages import Page

from .routers import territories_router


@territories_router.get(
    "/territory/{territory_id}/living_buildings_with_geometry",
    response_model=Page[LivingBuildingsWithGeometry],
    status_code=status.HTTP_200_OK,
)
async def get_living_buildings_with_geometry_by_territory_id(
    request: Request,
    territory_id: int = Path(description="territory id", gt=0),
) -> Page[LivingBuildingsWithGeometry]:
    """Get living buildings with geometry for territory."""
    territories_service: TerritoriesService = request.state.territories_service

    buildings = await territories_service.get_living_buildings_with_geometry_by_territory_id(territory_id)
    buildings = [LivingBuildingsWithGeometry.from_dto(building) for building in buildings]

    return paginate(buildings)
