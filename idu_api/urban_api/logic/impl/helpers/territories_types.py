"""Territories types internal logic is defined here."""

from sqlalchemy import insert, select
from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.common.db.entities import territory_types_dict
from idu_api.urban_api.dto import TerritoryTypeDTO
from idu_api.urban_api.exceptions.logic.common import EntityAlreadyExists
from idu_api.urban_api.logic.impl.helpers.utils import check_existence
from idu_api.urban_api.schemas import TerritoryTypePost


async def get_territory_types_from_db(conn: AsyncConnection) -> list[TerritoryTypeDTO]:
    """Get all territory type objects."""

    statement = select(territory_types_dict).order_by(territory_types_dict.c.territory_type_id)

    return [TerritoryTypeDTO(**data) for data in (await conn.execute(statement)).mappings().all()]


async def add_territory_type_to_db(
    conn: AsyncConnection,
    territory_type: TerritoryTypePost,
) -> TerritoryTypeDTO:
    """Create territory type object."""

    if await check_existence(conn, territory_types_dict, conditions={"name": territory_type.name}):
        raise EntityAlreadyExists("territory type", territory_type.name)

    statement = insert(territory_types_dict).values(**territory_type.model_dump()).returning(territory_types_dict)
    result = (await conn.execute(statement)).mappings().one()

    await conn.commit()

    return TerritoryTypeDTO(**result)
