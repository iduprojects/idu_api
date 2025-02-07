"""Territory types handlers are defined here."""

from fastapi import Request
from starlette import status

from idu_api.urban_api.logic.territories import TerritoriesService
from idu_api.urban_api.schemas import TargetCityType, TargetCityTypePost, TerritoryType, TerritoryTypePost

from .routers import territories_router


@territories_router.get(
    "/territory_types",
    response_model=list[TerritoryType],
    status_code=status.HTTP_200_OK,
)
async def get_territory_types(request: Request) -> list[TerritoryType]:
    """
    ## Get the list of territory types.

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


@territories_router.get(
    "/target_city_types",
    response_model=list[TargetCityType],
    status_code=status.HTTP_200_OK,
)
async def get_target_city_types(request: Request) -> list[TargetCityType]:
    """
    ## Get the list of target city types.

    ### Returns:
    - **list[TargetCityType]**: A list of target city types.
    """
    territories_service: TerritoriesService = request.state.territories_service

    target_city_types = await territories_service.get_target_city_types()

    return [TargetCityType.from_dto(target_city_type) for target_city_type in target_city_types]


@territories_router.post(
    "/target_city_types",
    response_model=TargetCityType,
    status_code=status.HTTP_201_CREATED,
)
async def add_target_city_type(request: Request, target_city_type: TargetCityTypePost) -> TargetCityType:
    """
    ## Create a new target city type.

    ### Parameters:
    - **target_city_type** (TargetCityTypePost, Body): Data for the new target city type.

    ### Returns:
    - **TargetCityType**: The created target city type.

    ### Errors:
    - **409 Conflict**: If a target city type with the such name already exists.
    """
    territories_service: TerritoriesService = request.state.territories_service

    target_city_type_dto = await territories_service.add_target_city_type(target_city_type)

    return TargetCityType.from_dto(target_city_type_dto)
