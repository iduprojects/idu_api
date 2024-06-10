"""
Territories endpoints logic of getting entities from the database is defined here.
"""

from typing import Callable, List

from fastapi import HTTPException
from sqlalchemy import insert, select
from sqlalchemy.ext.asyncio import AsyncConnection

from urban_api.db.entities import (
    physical_object_types_dict,
)
from urban_api.dto import (
    PhysicalObjectsTypesDTO,
)
from urban_api.schemas import PhysicalObjectsTypesPost

func: Callable


async def get_physical_object_types_from_db(session: AsyncConnection) -> List[PhysicalObjectsTypesDTO]:
    """
    Get all territory type objects
    """

    statement = select(physical_object_types_dict).order_by(physical_object_types_dict.c.physical_object_type_id)

    return [PhysicalObjectsTypesDTO(*data) for data in await session.execute(statement)]


async def add_physical_object_type_to_db(
    physical_object_type: PhysicalObjectsTypesPost,
    session: AsyncConnection,
) -> PhysicalObjectsTypesDTO:
    """
    Create territory type object
    """

    statement = select(physical_object_types_dict).where(physical_object_types_dict.c.name == physical_object_type.name)
    result = (await session.execute(statement)).one_or_none()
    if result is not None:
        raise HTTPException(status_code=400, detail="Invalid input (physical object type already exists)")

    statement = (
        insert(physical_object_types_dict)
        .values(
            name=physical_object_type.name,
        )
        .returning(physical_object_types_dict)
    )
    result = (await session.execute(statement)).mappings().one()

    await session.commit()

    return PhysicalObjectsTypesDTO(**result)
