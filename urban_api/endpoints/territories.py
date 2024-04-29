"""
Territory endpoints are defined here.
"""
from fastapi import Depends
from starlette import status
from sqlalchemy.ext.asyncio import AsyncConnection

from urban_api.db.connection import get_connection
from urban_api.logic.territories import (
    get_territory_types_from_db,
    add_territory_type_to_db,
    get_territory_by_id_from_db,
    add_territory_to_db,
    get_services_by_territory_id_from_db
)
from urban_api.schemas import (
    TerritoryTypes,
    TerritoryTypesPost,
    TerritoriesData,
    TerritoriesDataPost
)

from .routers import territories_router


@territories_router.get(
    "/territory_types",
    response_model=list[TerritoryTypes],
    status_code=status.HTTP_200_OK,
)
async def get_territory_types(
        connection: AsyncConnection = Depends(get_connection)
) -> list[TerritoryTypes]:
    """
    Summary:
        Get territory types list

    Description:
        Get a list of all territory types
    """

    territory_types = await get_territory_types_from_db(connection)

    return [TerritoryTypes.from_dto(territory_type) for territory_type in territory_types]


@territories_router.post(
    "/territory_types",
    response_model=TerritoryTypes,
    status_code=status.HTTP_201_CREATED,
)
async def add_territory_type(
        territory_type: TerritoryTypesPost,
        connection: AsyncConnection = Depends(get_connection)
) -> TerritoryTypes:
    """
    Summary:
        Add territory type

    Description:
        Add a territory type
    """

    territory_types = await add_territory_type_to_db(territory_type, connection)

    return TerritoryTypes.from_dto(territory_types)


@territories_router.get(
    "/territory",
    response_model=TerritoriesData,
    status_code=status.HTTP_200_OK,
)
async def get_territory_by_id(
        territory_id: int,
        connection: AsyncConnection = Depends(get_connection)
) -> TerritoriesData:
    """
    Summary:
        Get single territory

    Description:
        Get a territory by id
    """

    territory = await get_territory_by_id_from_db(territory_id, connection)

    return TerritoriesData.from_dto(territory)


@territories_router.post(
    "/territory",
    response_model=TerritoriesData,
    status_code=status.HTTP_201_CREATED,
)
async def add_territory(
        territory: TerritoriesDataPost,
        connection: AsyncConnection = Depends(get_connection)
) -> TerritoriesData:
    """
    Summary:
        Add territory type

    Description:
        Add a territory type
    """

    territory = await add_territory_to_db(territory, connection)

    return TerritoriesData.from_dto(territory)


@territories_router.get(
    "/territory/{id}/services",
    response_model=TerritoriesData,
    status_code=status.HTTP_200_OK,
)
async def get_services_by_territory_id(
        territory_id: int,
        service_type: int | None = None,
        connection: AsyncConnection = Depends(get_connection)
) -> TerritoriesData:
    """
    Summary:
        Get services for territory

    Description:
        Get services for territory by id, service type could be specified in parameters
    """

    services = await get_services_by_territory_id_from_db(territory_id, connection, service_type=service_type)

    return TerritoriesData.from_dto(services)
