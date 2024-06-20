"""
Physical objects endpoints logic of getting entities from the database is defined here.
"""

from typing import Callable, List

from fastapi import HTTPException
from geoalchemy2.functions import ST_AsGeoJSON, ST_GeomFromText
from sqlalchemy import cast, select, text, update
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncConnection

from urban_api.db.entities import (
    object_geometries_data,
    physical_object_types_dict,
    physical_objects_data,
    territories_data,
    urban_objects_data,
)
from urban_api.dto import (
    ObjectGeometryDTO,
    PhysicalObjectsDataDTO,
)
from urban_api.schemas import ObjectGeometriesPatch, ObjectGeometriesPut

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


async def get_object_geometry_by_id_from_db(
    object_geometry_id: int,
    session: AsyncConnection,
) -> ObjectGeometryDTO:
    """
    Create living building object
    """

    statement = select(
        object_geometries_data.c.object_geometry_id,
        object_geometries_data.c.territory_id,
        cast(ST_AsGeoJSON(object_geometries_data.c.geometry), JSONB).label("geometry"),
        cast(ST_AsGeoJSON(object_geometries_data.c.centre_point), JSONB).label("centre_point"),
        object_geometries_data.c.address,
    ).where(object_geometries_data.c.object_geometry_id == object_geometry_id)

    result = (await session.execute(statement)).mappings().one()
    await session.commit()

    return ObjectGeometryDTO(**result)


async def put_object_geometry_to_db(
    object_geometry: ObjectGeometriesPut,
    object_geometry_id: int,
    session: AsyncConnection,
) -> ObjectGeometryDTO:
    """
    Put object geometry
    """

    statement = select(object_geometries_data).where(object_geometries_data.c.object_geometry_id == object_geometry_id)
    requested_object_geometry = (await session.execute(statement)).one_or_none()
    if requested_object_geometry is None:
        raise HTTPException(status_code=404, detail="Given object geometry id is not found")

    statement = select(territories_data).where(territories_data.c.territory_id == object_geometry.territory_id)
    territory = (await session.execute(statement)).one_or_none()
    if territory is None:
        raise HTTPException(status_code=404, detail="Given territory id is not found")

    statement = (
        update(object_geometries_data)
        .where(object_geometries_data.c.object_geometry_id == object_geometry_id)
        .values(
            territory_id=object_geometry.territory_id,
            geometry=ST_GeomFromText(str(object_geometry.geometry.as_shapely_geometry()), text("4326")),
            centre_point=ST_GeomFromText(str(object_geometry.centre_point.as_shapely_geometry()), text("4326")),
            address=object_geometry.address,
        )
        .returning(object_geometries_data)
    )

    result = (await session.execute(statement)).mappings().one()
    await session.commit()

    return await get_object_geometry_by_id_from_db(result.object_geometry_id, session)


async def patch_object_geometry_to_db(
    object_geometry: ObjectGeometriesPatch,
    object_geometry_id: int,
    session: AsyncConnection,
) -> ObjectGeometryDTO:
    """
    Patch object geometry
    """

    statement = select(object_geometries_data).where(object_geometries_data.c.object_geometry_id == object_geometry_id)
    requested_object_geometry = (await session.execute(statement)).one_or_none()
    if requested_object_geometry is None:
        raise HTTPException(status_code=404, detail="Given object geometry id is not found")

    statement = (
        update(object_geometries_data)
        .where(object_geometries_data.c.object_geometry_id == object_geometry_id)
        .returning(object_geometries_data)
    )

    values_to_update = {}
    for k, v in object_geometry.model_dump(exclude={"geometry", "centre_point"}).items():
        if v is not None:
            if k == "territory_id":
                new_statement = select(territories_data).where(
                    territories_data.c.territory_id == object_geometry.territory_id
                )
                territory = (await session.execute(new_statement)).one_or_none()
                if territory is None:
                    raise HTTPException(status_code=404, detail="Given territory id is not found")
            values_to_update.update({k: v})

    values_to_update.update(
        {"geometry": ST_GeomFromText(str(object_geometry.geometry.as_shapely_geometry()), text("4326"))}
    )
    values_to_update.update(
        {"centre_point": ST_GeomFromText(str(object_geometry.centre_point.as_shapely_geometry()), text("4326"))}
    )

    statement = statement.values(**values_to_update)
    result = (await session.execute(statement)).mappings().one()
    await session.commit()

    return await get_object_geometry_by_id_from_db(result.object_geometry_id, session)
