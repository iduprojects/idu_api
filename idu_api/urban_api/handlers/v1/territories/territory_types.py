"""Territory types handlers are defined here."""

from fastapi import Request
from starlette import status

from idu_api.urban_api.logic.territories import TerritoriesService
from idu_api.urban_api.schemas import TerritoryType, TerritoryTypePost

from .routers import territories_router


@territories_router.get(
    "/territory_types",
    response_model=list[TerritoryType],
    status_code=status.HTTP_200_OK,
)
async def get_territory_types(request: Request) -> list[TerritoryType]:
    """
    ## Retrieve the list of territory types.

    ### Returns:
    - **list[TerritoryType]**: A list of territory types.
    """
    territories_service: TerritoriesService = request.state.territories_service

    territory_types = await territories_service.get_territory_types()

    return [TerritoryType.from_dto(territory_type) for territory_type in territory_types]


@territories_router.post(
    "/territory_types",
    response_model=TerritoryType,
    status_code=status.HTTP_201_CREATED,
)
async def add_territory_type(request: Request, territory_type: TerritoryTypePost) -> TerritoryType:
    """
    ## Create a new territory type.

    ### Parameters:
    - **territory_type** (TerritoryTypePost, Body): Data for the new territory type.

    ### Returns:
    - **TerritoryType**: The created territory type.

    ### Errors:
    - **409 Conflict**: If a territory type with the such name already exists.
    """
    territories_service: TerritoriesService = request.state.territories_service

    territory_type_dto = await territories_service.add_territory_type(territory_type)

    return TerritoryType.from_dto(territory_type_dto)
