"""Territory types handlers are defined here."""

from fastapi import Request
from starlette import status

from urban_api.logic.territories import TerritoriesService
from urban_api.schemas import TerritoryTypes, TerritoryTypesPost

from .routers import territories_router


@territories_router.get(
    "/territory_types",
    response_model=list[TerritoryTypes],
    status_code=status.HTTP_200_OK,
)
async def get_territory_types(
    request: Request,
) -> list[TerritoryTypes]:
    """Get territory types list."""
    territories_service: TerritoriesService = request.state.territories_service

    territory_types = await territories_service.get_territory_types_from_db()

    return [TerritoryTypes.from_dto(territory_type) for territory_type in territory_types]


@territories_router.post(
    "/territory_types",
    response_model=TerritoryTypes,
    status_code=status.HTTP_201_CREATED,
)
async def add_territory_type(request: Request, territory_type: TerritoryTypesPost) -> TerritoryTypes:
    """Add a territory type."""
    territories_service: TerritoriesService = request.state.territories_service

    territory_type_dto = await territories_service.add_territory_type_to_db(territory_type)

    return TerritoryTypes.from_dto(territory_type_dto)
