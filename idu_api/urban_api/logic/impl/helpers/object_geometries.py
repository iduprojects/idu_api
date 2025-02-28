"""Object geometries internal logic is defined here."""

from collections import defaultdict
from typing import Callable

from geoalchemy2.functions import ST_AsEWKB
from sqlalchemy import delete, insert, select, update
from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.common.db.entities import (
    buildings_data,
    object_geometries_data,
    physical_object_functions_dict,
    physical_object_types_dict,
    physical_objects_data,
    territories_data,
    urban_objects_data,
)
from idu_api.urban_api.dto import ObjectGeometryDTO, PhysicalObjectDTO, UrbanObjectDTO
from idu_api.urban_api.exceptions.logic.common import EntitiesNotFoundByIds, EntityNotFoundById, TooManyObjectsError
from idu_api.urban_api.logic.impl.helpers.urban_objects import get_urban_objects_by_ids_from_db
from idu_api.urban_api.logic.impl.helpers.utils import (
    OBJECTS_NUMBER_LIMIT,
    check_existence,
    extract_values_from_model,
)
from idu_api.urban_api.schemas import ObjectGeometryPatch, ObjectGeometryPost, ObjectGeometryPut

func: Callable


async def get_physical_objects_by_object_geometry_id_from_db(
    conn: AsyncConnection,
    object_geometry_id: int,
) -> list[PhysicalObjectDTO]:
    """Get physical object or list of physical objects by object geometry id."""

    if not await check_existence(conn, object_geometries_data, conditions={"object_geometry_id": object_geometry_id}):
        raise EntityNotFoundById(object_geometry_id, "object geometry")

    building_columns = [col for col in buildings_data.c if col.name not in ("physical_object_id", "properties")]
    statement = (
        select(
            physical_objects_data.c.physical_object_id,
            physical_objects_data.c.name,
            physical_objects_data.c.properties,
            physical_object_types_dict.c.physical_object_type_id,
            physical_object_types_dict.c.name.label("physical_object_type_name"),
            physical_object_functions_dict.c.physical_object_function_id,
            physical_object_functions_dict.c.name.label("physical_object_function_name"),
            physical_objects_data.c.created_at,
            physical_objects_data.c.updated_at,
            *building_columns,
            buildings_data.c.properties.label("building_properties"),
            territories_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
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
                physical_object_functions_dict,
                physical_object_functions_dict.c.physical_object_function_id
                == physical_object_types_dict.c.physical_object_function_id,
            )
            .join(
                object_geometries_data,
                urban_objects_data.c.object_geometry_id == object_geometries_data.c.object_geometry_id,
            )
            .join(
                territories_data,
                territories_data.c.territory_id == object_geometries_data.c.territory_id,
            )
            .outerjoin(
                buildings_data,
                buildings_data.c.physical_object_id == physical_objects_data.c.physical_object_id,
            )
        )
        .where(urban_objects_data.c.object_geometry_id == object_geometry_id)
        .distinct()
    )

    result = (await conn.execute(statement)).mappings().all()

    grouped_data = defaultdict(lambda: {"territories": []})
    for row in result:
        key = row.physical_object_id
        if key not in grouped_data:
            grouped_data[key].update({k: v for k, v in row.items() if k in PhysicalObjectDTO.fields()})

        territory = {"territory_id": row["territory_id"], "name": row["territory_name"]}
        grouped_data[key]["territories"].append(territory)

    return [PhysicalObjectDTO(**physical_object) for physical_object in grouped_data.values()]


async def get_object_geometry_by_ids_from_db(conn: AsyncConnection, ids: list[int]) -> list[ObjectGeometryDTO]:
    """Get list of object geometries by list of identifiers."""

    if len(ids) > OBJECTS_NUMBER_LIMIT:
        raise TooManyObjectsError(len(ids), OBJECTS_NUMBER_LIMIT)

    statement = (
        select(
            object_geometries_data.c.object_geometry_id,
            object_geometries_data.c.territory_id,
            ST_AsEWKB(object_geometries_data.c.geometry).label("geometry"),
            ST_AsEWKB(object_geometries_data.c.centre_point).label("centre_point"),
            object_geometries_data.c.address,
            object_geometries_data.c.osm_id,
            object_geometries_data.c.created_at,
            object_geometries_data.c.updated_at,
            territories_data.c.name.label("territory_name"),
        )
        .select_from(
            object_geometries_data.join(
                territories_data,
                territories_data.c.territory_id == object_geometries_data.c.territory_id,
            )
        )
        .where(object_geometries_data.c.object_geometry_id.in_(ids))
    )

    result = (await conn.execute(statement)).mappings().all()
    if len(ids) > len(result):
        raise EntitiesNotFoundByIds("object geometry")

    return [ObjectGeometryDTO(**geom) for geom in result]


async def put_object_geometry_to_db(
    conn: AsyncConnection,
    object_geometry: ObjectGeometryPut,
    object_geometry_id: int,
) -> ObjectGeometryDTO:
    """Put object geometry."""

    if not await check_existence(conn, object_geometries_data, conditions={"object_geometry_id": object_geometry_id}):
        raise EntityNotFoundById(object_geometry_id, "object geometry")

    if not await check_existence(conn, territories_data, conditions={"territory_id": object_geometry.territory_id}):
        raise EntityNotFoundById(object_geometry.territory_id, "territory")

    values = extract_values_from_model(object_geometry, to_update=True)
    statement = (
        update(object_geometries_data)
        .where(object_geometries_data.c.object_geometry_id == object_geometry_id)
        .values(**values)
    )

    await conn.execute(statement)
    await conn.commit()

    return (await get_object_geometry_by_ids_from_db(conn, [object_geometry_id]))[0]


async def patch_object_geometry_to_db(
    conn: AsyncConnection,
    object_geometry: ObjectGeometryPatch,
    object_geometry_id: int,
) -> ObjectGeometryDTO:
    """Patch object geometry."""

    if not await check_existence(conn, object_geometries_data, conditions={"object_geometry_id": object_geometry_id}):
        raise EntityNotFoundById(object_geometry_id, "object geometry")

    if object_geometry.territory_id is not None:
        if not await check_existence(conn, territories_data, conditions={"territory_id": object_geometry.territory_id}):
            raise EntityNotFoundById(object_geometry.territory_id, "territory")

    values = extract_values_from_model(object_geometry, exclude_unset=True, to_update=True)

    statement = (
        update(object_geometries_data)
        .where(object_geometries_data.c.object_geometry_id == object_geometry_id)
        .values(**values)
    )

    await conn.execute(statement)
    await conn.commit()

    return (await get_object_geometry_by_ids_from_db(conn, [object_geometry_id]))[0]


async def delete_object_geometry_in_db(conn: AsyncConnection, object_geometry_id: int) -> dict:
    """Delete object geometry."""

    if not await check_existence(conn, object_geometries_data, conditions={"object_geometry_id": object_geometry_id}):
        raise EntityNotFoundById(object_geometry_id, "object geometry")

    statement = delete(object_geometries_data).where(object_geometries_data.c.object_geometry_id == object_geometry_id)
    await conn.execute(statement)
    await conn.commit()

    return {"status": "ok"}


async def add_object_geometry_to_physical_object_to_db(
    conn: AsyncConnection, physical_object_id: int, object_geometry: ObjectGeometryPost
) -> UrbanObjectDTO:
    """Create object geometry connected with physical object."""

    if not await check_existence(conn, physical_objects_data, conditions={"physical_object_id": physical_object_id}):
        raise EntityNotFoundById(physical_object_id, "physical object")

    if not await check_existence(conn, territories_data, conditions={"territory_id": object_geometry.territory_id}):
        raise EntityNotFoundById(object_geometry.territory_id, "territory")

    values = extract_values_from_model(object_geometry)

    statement = insert(object_geometries_data).values(**values).returning(object_geometries_data.c.object_geometry_id)
    object_geometry_id = (await conn.execute(statement)).scalar_one()

    statement = (
        insert(urban_objects_data)
        .values(physical_object_id=physical_object_id, object_geometry_id=object_geometry_id)
        .returning(urban_objects_data.c.urban_object_id)
    )

    urban_object_id = (await conn.execute(statement)).scalar_one_or_none()
    await conn.commit()

    return (await get_urban_objects_by_ids_from_db(conn, [urban_object_id]))[0]
