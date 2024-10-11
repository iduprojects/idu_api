"""Physical objects handlers logic of getting entities from the database is defined here."""

from datetime import datetime, timezone
from typing import Callable

from geoalchemy2.functions import ST_AsGeoJSON, ST_GeomFromText
from sqlalchemy import cast, delete, insert, select, text, update
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.common.db.entities import (
    object_geometries_data,
    physical_object_types_dict,
    physical_objects_data,
    territories_data,
    urban_objects_data,
)
from idu_api.common.db.entities.object_geometries import object_geometries_data_id_seq
from idu_api.urban_api.dto import ObjectGeometryDTO, PhysicalObjectDataDTO, UrbanObjectDTO
from idu_api.urban_api.exceptions.logic.common import EntitiesNotFoundByIds, EntityNotFoundById
from idu_api.urban_api.logic.impl.helpers.urban_objects import get_urban_object_by_id_from_db
from idu_api.urban_api.schemas import ObjectGeometriesPatch, ObjectGeometriesPost, ObjectGeometriesPut

func: Callable


async def get_physical_objects_by_object_geometry_id_from_db(
    conn: AsyncConnection,
    object_geometry_id: int,
) -> list[PhysicalObjectDataDTO]:
    """Get physical object or list of physical objects by object geometry id."""

    statement = select(object_geometries_data).where(object_geometries_data.c.object_geometry_id == object_geometry_id)
    object_geometry = (await conn.execute(statement)).one_or_none()
    if object_geometry is None:
        raise EntityNotFoundById(object_geometry_id, "object geometry")

    statement = (
        select(
            physical_objects_data.c.physical_object_id,
            physical_objects_data.c.name,
            physical_objects_data.c.properties,
            physical_object_types_dict.c.physical_object_type_id,
            physical_object_types_dict.c.name.label("physical_object_type_name"),
            physical_objects_data.c.created_at,
            physical_objects_data.c.updated_at,
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
        .distinct()
    )

    result = (await conn.execute(statement)).mappings().all()

    return [PhysicalObjectDataDTO(**physical_object) for physical_object in result]


async def get_object_geometry_by_ids_from_db(
    conn: AsyncConnection,
    object_geometry_ids: list[int],
) -> list[ObjectGeometryDTO]:
    """Get list of object geometries by list of identifiers."""

    statement = select(
        object_geometries_data.c.object_geometry_id,
        object_geometries_data.c.territory_id,
        cast(ST_AsGeoJSON(object_geometries_data.c.geometry), JSONB).label("geometry"),
        cast(ST_AsGeoJSON(object_geometries_data.c.centre_point), JSONB).label("centre_point"),
        object_geometries_data.c.address,
        object_geometries_data.c.osm_id,
        object_geometries_data.c.created_at,
        object_geometries_data.c.updated_at,
    ).where(object_geometries_data.c.object_geometry_id.in_(object_geometry_ids))

    result = (await conn.execute(statement)).mappings().all()

    if len(object_geometry_ids) > len(list(result)):
        raise EntitiesNotFoundByIds("object geometry")

    return [ObjectGeometryDTO(**geom) for geom in result]


async def put_object_geometry_to_db(
    conn: AsyncConnection,
    object_geometry: ObjectGeometriesPut,
    object_geometry_id: int,
) -> ObjectGeometryDTO:
    """Put object geometry."""

    statement = select(object_geometries_data).where(object_geometries_data.c.object_geometry_id == object_geometry_id)
    requested_object_geometry = (await conn.execute(statement)).one_or_none()
    if requested_object_geometry is None:
        raise EntityNotFoundById(object_geometry_id, "object geometry")

    statement = select(territories_data).where(territories_data.c.territory_id == object_geometry.territory_id)
    territory = (await conn.execute(statement)).one_or_none()
    if territory is None:
        raise EntityNotFoundById(object_geometry.territory_id, "territory")

    statement = (
        update(object_geometries_data)
        .where(object_geometries_data.c.object_geometry_id == object_geometry_id)
        .values(
            territory_id=object_geometry.territory_id,
            geometry=ST_GeomFromText(str(object_geometry.geometry.as_shapely_geometry()), text("4326")),
            centre_point=ST_GeomFromText(str(object_geometry.centre_point.as_shapely_geometry()), text("4326")),
            address=object_geometry.address,
            osm_id=object_geometry.osm_id,
            updated_at=datetime.now(timezone.utc),
        )
        .returning(object_geometries_data)
    )

    result = (await conn.execute(statement)).mappings().one()
    await conn.commit()

    return (await get_object_geometry_by_ids_from_db(conn, [result.object_geometry_id]))[0]


async def patch_object_geometry_to_db(
    conn: AsyncConnection,
    object_geometry: ObjectGeometriesPatch,
    object_geometry_id: int,
) -> ObjectGeometryDTO:
    """Patch object geometry."""

    statement = select(object_geometries_data).where(object_geometries_data.c.object_geometry_id == object_geometry_id)
    requested_object_geometry = (await conn.execute(statement)).one_or_none()
    if requested_object_geometry is None:
        raise EntityNotFoundById(object_geometry_id, "object geometry")

    statement = (
        update(object_geometries_data)
        .where(object_geometries_data.c.object_geometry_id == object_geometry_id)
        .values(updated_at=datetime.now(timezone.utc))
        .returning(object_geometries_data)
    )

    values_to_update = {}
    for k, v in object_geometry.model_dump(exclude={"geometry", "centre_point"}, exclude_unset=True).items():
        if k == "territory_id":
            new_statement = select(territories_data).where(
                territories_data.c.territory_id == object_geometry.territory_id
            )
            territory = (await conn.execute(new_statement)).one_or_none()
            if territory is None:
                raise EntityNotFoundById(object_geometry.territory_id, "territory")
        values_to_update.update({k: v})

    if object_geometry.geometry is not None:
        values_to_update.update(
            {"geometry": ST_GeomFromText(str(object_geometry.geometry.as_shapely_geometry()), text("4326"))}
        )
        values_to_update.update(
            {"centre_point": ST_GeomFromText(str(object_geometry.centre_point.as_shapely_geometry()), text("4326"))}
        )

    statement = statement.values(**values_to_update)
    result = (await conn.execute(statement)).mappings().one()
    await conn.commit()

    return (await get_object_geometry_by_ids_from_db(conn, [result.object_geometry_id]))[0]


async def delete_object_geometry_in_db(conn: AsyncConnection, object_geometry_id: int) -> dict:
    """Delete object geometry."""

    statement = select(object_geometries_data).where(object_geometries_data.c.object_geometry_id == object_geometry_id)
    requested_object_geometry = (await conn.execute(statement)).one_or_none()
    if requested_object_geometry is None:
        raise EntityNotFoundById(object_geometry_id, "object geometry")

    statement = delete(object_geometries_data).where(object_geometries_data.c.object_geometry_id == object_geometry_id)
    await conn.execute(statement)
    await conn.commit()

    return {"result": "ok"}


async def add_object_geometry_to_physical_object_in_db(
    conn: AsyncConnection, physical_object_id: int, object_geometry: ObjectGeometriesPost
) -> UrbanObjectDTO:
    """Create object geometry connected with physical object."""

    statement = select(territories_data).where(territories_data.c.territory_id == object_geometry.territory_id)
    territory = (await conn.execute(statement)).one_or_none()
    if territory is None:
        raise EntityNotFoundById(object_geometry.territory_id, "territory")

    statement = select(physical_objects_data).where(physical_objects_data.c.physical_object_id == physical_object_id)
    physical_object = (await conn.execute(statement)).one_or_none()
    if physical_object is None:
        raise EntityNotFoundById(physical_object_id, "physical object")

    statement = (
        insert(object_geometries_data)
        .values(
            territory_id=object_geometry.territory_id,
            geometry=ST_GeomFromText(str(object_geometry.geometry.as_shapely_geometry()), text("4326")),
            centre_point=ST_GeomFromText(str(object_geometry.centre_point.as_shapely_geometry()), text("4326")),
            address=object_geometry.address,
            osm_id=object_geometry.osm_id,
        )
        .returning(object_geometries_data.c.object_geometry_id)
    )
    object_geometry_id = (await conn.execute(statement)).scalar_one()

    statement = (
        insert(urban_objects_data)
        .values(physical_object_id=physical_object_id, object_geometry_id=object_geometry_id)
        .returning(urban_objects_data.c.urban_object_id)
    )

    urban_object_id = (await conn.execute(statement)).scalar_one_or_none()
    await conn.commit()

    return await get_urban_object_by_id_from_db(conn, urban_object_id)
