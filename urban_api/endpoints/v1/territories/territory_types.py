"""Territory types endpoints are defined here."""

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncConnection
from starlette import status

from urban_api.db.connection import get_connection
from urban_api.logic.territories import add_territory_type_to_db, get_territory_types_from_db
from urban_api.schemas import TerritoryTypes, TerritoryTypesPost

from .routers import territories_router


@territories_router.get(
    "/territory_types",
    response_model=list[TerritoryTypes],
    status_code=status.HTTP_200_OK,
)
async def get_territory_types(connection: AsyncConnection = Depends(get_connection)) -> list[TerritoryTypes]:
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
    territory_type: TerritoryTypesPost, connection: AsyncConnection = Depends(get_connection)
) -> TerritoryTypes:
    """
    Summary:
        Add territory type

    Description:
        Add a territory type
    """

    territory_type_dto = await add_territory_type_to_db(territory_type, connection)

    return TerritoryTypes.from_dto(territory_type_dto)
