"""Functional zones internal logic is defined here."""

from sqlalchemy import insert, select
from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.common.db.entities import functional_zone_types_dict
from idu_api.urban_api.dto import FunctionalZoneTypeDTO
from idu_api.urban_api.exceptions.logic.common import EntityAlreadyExists
from idu_api.urban_api.schemas import FunctionalZoneTypePost


async def get_functional_zone_types_from_db(conn: AsyncConnection) -> list[FunctionalZoneTypeDTO]:
    """Get all functional zone type objects."""

    statement = select(functional_zone_types_dict).order_by(functional_zone_types_dict.c.functional_zone_type_id)

    return [FunctionalZoneTypeDTO(**zone_type) for zone_type in (await conn.execute(statement)).mappings().all()]


async def add_functional_zone_type_to_db(
    conn: AsyncConnection,
    functional_zone_type: FunctionalZoneTypePost,
) -> FunctionalZoneTypeDTO:
    """Create a new functional zone type object."""

    statement = select(functional_zone_types_dict).where(functional_zone_types_dict.c.name == functional_zone_type.name)
    result = (await conn.execute(statement)).scalar_one_or_none()
    if result is not None:
        raise EntityAlreadyExists("functional zone type", functional_zone_type.name)

    statement = (
        insert(functional_zone_types_dict)
        .values(
            name=functional_zone_type.name,
            zone_nickname=functional_zone_type.zone_nickname,
            description=functional_zone_type.description,
        )
        .returning(functional_zone_types_dict)
    )
    result = (await conn.execute(statement)).mappings().one()

    await conn.commit()

    return FunctionalZoneTypeDTO(**result)
