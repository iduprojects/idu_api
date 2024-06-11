"""
Physical objects endpoints logic of getting entities from the database is defined here.
"""

from typing import Callable, List

from fastapi import HTTPException
from geoalchemy2.functions import ST_AsGeoJSON
from sqlalchemy import cast, select
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncConnection

from urban_api.db.entities import (
    object_geometries_data,
    physical_object_types_dict,
    physical_objects_data,
    urban_objects_data,
)
from urban_api.dto import (
    PhysicalObjectsDataDTO,
)

func: Callable


async def get_physical_objects_by_object_geometry_id_from_db(
    object_geometry_id: int,
    session: AsyncConnection,
) -> List[PhysicalObjectsDataDTO]:
    """
    Get physical object or list of physical objects by object geometry id
    """

    statement = select(object_geometries_data).where(object_geometries_data.c.object_geometry_id == object_geometry_id)
    object_geometry = (await session.execute(statement)).one_or_none()
    if object_geometry is None:
        raise HTTPException(status_code=404, detail="Given object geometry id is not found")

    statement = (
        select(
            physical_objects_data.c.physical_object_id,
            physical_objects_data.c.name,
            physical_objects_data.c.properties,
            object_geometries_data.c.address,
            physical_object_types_dict.c.physical_object_type_id,
            physical_object_types_dict.c.name.label("physical_object_type_name"),
        )
        .select_from(
            physical_objects_data.join(
                urban_objects_data,
                urban_objects_data.c.physical_object_id == physical_objects_data.c.physical_object_id,
            )
            .join(
                physical_object_types_dict,
                physical_object_types_dict.c.physical_object_type_id == physical_objects_data.c.physical_object_type_id,
            )
            .join(
                object_geometries_data,
                urban_objects_data.c.object_geometry_id == object_geometries_data.c.object_geometry_id,
            )
        )
        .where(urban_objects_data.c.object_geometry_id == object_geometry_id)
    )

    result = (await session.execute(statement)).mappings().all()

    return [PhysicalObjectsDataDTO(**physical_object) for physical_object in result]
