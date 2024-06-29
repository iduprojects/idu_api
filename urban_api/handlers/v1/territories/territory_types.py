"""Territory types handlers are defined here."""

from fastapi import Request
from starlette import status

from urban_api.logic.territories import TerritoriesService
from urban_api.schemas import TerritoryType, TerritoryTypesPost

from .routers import territories_router


@territories_router.get(
    "/territory_types",
    response_model=list[TerritoryType],
    status_code=status.HTTP_200_OK,
)
async def get_territory_types(
    request: Request,
) -> list[TerritoryType]:
    """Get territory types list."""
    territories_service: TerritoriesService = request.state.territories_service

    territory_types = await territories_service.get_territory_types()

    return [TerritoryType.from_dto(territory_type) for territory_type in territory_types]


@territories_router.post(
    "/territory_types",
    response_model=TerritoryType,
    status_code=status.HTTP_201_CREATED,
)
async def add_territory_type(request: Request, territory_type: TerritoryTypesPost) -> TerritoryType:
    """Add a territory type."""
    territories_service: TerritoriesService = request.state.territories_service

    territory_type_dto = await territories_service.add_territory_type(territory_type)

    return TerritoryType.from_dto(territory_type_dto)
