"""Buildings territories-related handlers are defined here."""

from fastapi import Path, Query, Request
from starlette import status

from idu_api.urban_api.logic.territories import TerritoriesService
from idu_api.urban_api.schemas import LivingBuildingsWithGeometry
from idu_api.urban_api.schemas.pages import Page
from idu_api.urban_api.utils.pagination import paginate

from .routers import territories_router


@territories_router.get(
    "/territory/{territory_id}/living_buildings_with_geometry",
    response_model=Page[LivingBuildingsWithGeometry],
    status_code=status.HTTP_200_OK,
)
async def get_living_buildings_with_geometry_by_territory_id(
    request: Request,
    territory_id: int = Path(..., description="territory id", gt=0),
    include_child_territories: bool = Query(True, description="to get from child territories"),
    cities_only: bool = Query(False, description="to get only for cities"),
) -> Page[LivingBuildingsWithGeometry]:
    """Get living buildings with geometry for territory."""
    territories_service: TerritoriesService = request.state.territories_service

    buildings = await territories_service.get_living_buildings_with_geometry_by_territory_id(
        territory_id, include_child_territories, cities_only
    )

    return paginate(
        buildings.items,
        buildings.total,
        transformer=lambda x: [LivingBuildingsWithGeometry.from_dto(item) for item in x],
    )
