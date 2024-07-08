"""Territories types internal logic is defined here."""

from fastapi import HTTPException
from sqlalchemy import insert, select
from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.common.db.entities import territory_types_dict
from idu_api.urban_api.dto import TerritoryTypeDTO
from idu_api.urban_api.schemas import TerritoryTypesPost


async def get_territory_types_from_db(conn: AsyncConnection) -> list[TerritoryTypeDTO]:
    """Get all territory type objects."""
    statement = select(territory_types_dict).order_by(territory_types_dict.c.territory_type_id)

    return [TerritoryTypeDTO(**data) for data in (await conn.execute(statement)).mappings().all()]


async def add_territory_type_to_db(
    conn: AsyncConnection,
    territory_type: TerritoryTypesPost,
) -> TerritoryTypeDTO:
    """Create territory type object."""
    statement = select(territory_types_dict).where(territory_types_dict.c.name == territory_type.name)
    result = (await conn.execute(statement)).one_or_none()
    if result is not None:
        raise HTTPException(status_code=400, detail="Invalid input (territory type already exists)")

    statement = (
        insert(territory_types_dict)
        .values(
            name=territory_type.name,
        )
        .returning(territory_types_dict)
    )
    result = (await conn.execute(statement)).mappings().one()

    await conn.commit()

    return TerritoryTypeDTO(**result)
