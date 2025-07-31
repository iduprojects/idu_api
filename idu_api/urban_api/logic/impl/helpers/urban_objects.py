"""Urban objects internal logic is defined here."""

from geoalchemy2.functions import ST_AsEWKB
from sqlalchemy import delete, select, update
from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.common.db.entities import (
    buildings_data,
    object_geometries_data,
    physical_object_functions_dict,
    physical_object_types_dict,
    physical_objects_data,
    service_types_dict,
    services_data,
    territories_data,
    territory_types_dict,
    urban_functions_dict,
    urban_objects_data,
)
from idu_api.urban_api.dto import UrbanObjectDTO
from idu_api.urban_api.exceptions.logic.common import (
    EntitiesNotFoundByIds,
    EntityAlreadyExists,
    EntityNotFoundById,
    TooManyObjectsError,
)
from idu_api.urban_api.logic.impl.helpers.utils import (
    OBJECTS_NUMBER_LIMIT,
    check_existence,
    extract_values_from_model,
    include_child_territories_cte,
)
from idu_api.urban_api.schemas import UrbanObjectPatch
from idu_api.urban_api.utils.query_filters import EqFilter, apply_filters


async def get_urban_objects_by_ids_from_db(conn: AsyncConnection, ids: list[int]) -> list[UrbanObjectDTO]:
    """Get urban objects by urban object identifiers."""

    if len(ids) > OBJECTS_NUMBER_LIMIT:
        raise TooManyObjectsError(len(ids), OBJECTS_NUMBER_LIMIT)

    building_columns = [col for col in buildings_data.c if col.name not in ("physical_object_id", "properties")]
    statement = (
        select(
            urban_objects_data,
            physical_objects_data.c.physical_object_type_id,
            physical_object_types_dict.c.name.label("physical_object_type_name"),
            physical_object_types_dict.c.physical_object_function_id,
            physical_object_functions_dict.c.name.label("physical_object_function_name"),
            physical_objects_data.c.name.label("physical_object_name"),
            physical_objects_data.c.properties.label("physical_object_properties"),
            physical_objects_data.c.created_at.label("physical_object_created_at"),
            physical_objects_data.c.updated_at.label("physical_object_updated_at"),
            object_geometries_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
            ST_AsEWKB(object_geometries_data.c.geometry).label("geometry"),
            ST_AsEWKB(object_geometries_data.c.centre_point).label("centre_point"),
            object_geometries_data.c.created_at.label("object_geometry_created_at"),
            object_geometries_data.c.updated_at.label("object_geometry_updated_at"),
            services_data.c.name.label("service_name"),
            services_data.c.capacity,
            services_data.c.is_capacity_real,
            services_data.c.properties.label("service_properties"),
            services_data.c.created_at.label("service_created_at"),
            services_data.c.updated_at.label("service_updated_at"),
            object_geometries_data.c.address,
            object_geometries_data.c.osm_id,
            service_types_dict.c.service_type_id,
            service_types_dict.c.urban_function_id,
            urban_functions_dict.c.name.label("urban_function_name"),
            service_types_dict.c.name.label("service_type_name"),
            service_types_dict.c.capacity_modeled.label("service_type_capacity_modeled"),
            service_types_dict.c.code.label("service_type_code"),
            service_types_dict.c.infrastructure_type,
            service_types_dict.c.properties.label("service_type_properties"),
            territory_types_dict.c.territory_type_id,
            territory_types_dict.c.name.label("territory_type_name"),
            *building_columns,
            buildings_data.c.properties.label("building_properties"),
        )
        .select_from(
            urban_objects_data.join(
                physical_objects_data,
                physical_objects_data.c.physical_object_id == urban_objects_data.c.physical_object_id,
            )
            .join(
                object_geometries_data,
                object_geometries_data.c.object_geometry_id == urban_objects_data.c.object_geometry_id,
            )
            .join(
                territories_data,
                territories_data.c.territory_id == object_geometries_data.c.territory_id,
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
            .outerjoin(services_data, services_data.c.service_id == urban_objects_data.c.service_id)
            .outerjoin(service_types_dict, service_types_dict.c.service_type_id == services_data.c.service_type_id)
            .outerjoin(
                urban_functions_dict,
                urban_functions_dict.c.urban_function_id == service_types_dict.c.urban_function_id,
            )
            .outerjoin(
                territory_types_dict, territory_types_dict.c.territory_type_id == services_data.c.territory_type_id
            )
            .outerjoin(
                buildings_data,
                buildings_data.c.physical_object_id == physical_objects_data.c.physical_object_id,
            )
        )
        .where(urban_objects_data.c.urban_object_id.in_(ids))
    )

    urban_objects = (await conn.execute(statement)).mappings().all()
    if len(ids) > len(urban_objects):
        raise EntitiesNotFoundByIds("urban object")

    return [UrbanObjectDTO(**uo) for uo in urban_objects]


async def get_urban_objects_by_physical_object_id_from_db(
    conn: AsyncConnection,
    physical_object_id: int,
) -> list[UrbanObjectDTO]:
    """Get list of urban objects by physical object id."""

    if not await check_existence(conn, physical_objects_data, conditions={"physical_object_id": physical_object_id}):
        raise EntityNotFoundById(physical_object_id, "physical object")

    statement = select(urban_objects_data.c.urban_object_id).where(
        urban_objects_data.c.physical_object_id == physical_object_id
    )
    ids = (await conn.execute(statement)).scalars().all()

    return await get_urban_objects_by_ids_from_db(conn, ids)


async def get_urban_objects_by_object_geometry_id_from_db(
    conn: AsyncConnection,
    object_geometry_id: int,
) -> list[UrbanObjectDTO]:
    """Get list of urban objects by object geometry id."""

    if not await check_existence(conn, object_geometries_data, conditions={"object_geometry_id": object_geometry_id}):
        raise EntityNotFoundById(object_geometry_id, "object geometry")

    statement = select(urban_objects_data.c.urban_object_id).where(
        urban_objects_data.c.object_geometry_id == object_geometry_id
    )
    ids = (await conn.execute(statement)).scalars().all()

    return await get_urban_objects_by_ids_from_db(conn, ids)


async def get_urban_objects_by_service_id_from_db(conn: AsyncConnection, service_id: int) -> list[UrbanObjectDTO]:
    """Get list of urban objects by service id."""

    if not await check_existence(conn, services_data, conditions={"service_id": service_id}):
        raise EntityNotFoundById(service_id, "service")

    statement = select(urban_objects_data.c.urban_object_id).where(urban_objects_data.c.service_id == service_id)
    ids = (await conn.execute(statement)).scalars().all()

    return await get_urban_objects_by_ids_from_db(conn, ids)


async def delete_urban_object_by_id_from_db(conn: AsyncConnection, urban_object_id: int) -> dict:
    """Get urban object by urban object id."""

    if not await check_existence(conn, urban_objects_data, conditions={"urban_object_id": urban_object_id}):
        raise EntityNotFoundById(urban_object_id, "urban object")

    statement = delete(urban_objects_data).where(urban_objects_data.c.urban_object_id == urban_object_id)
    await conn.execute(statement)
    await conn.commit()

    return {"status": "ok"}


async def get_urban_objects_by_territory_id_from_db(
    conn: AsyncConnection,
    territory_id: int,
    service_type_id: int | None,
    physical_object_type_id: int | None,
) -> list[UrbanObjectDTO]:
    """Get a list of urban objects by territory id with service type and physical object type filters."""

    if not await check_existence(conn, territories_data, conditions={"territory_id": territory_id}):
        raise EntityNotFoundById(territory_id, "territory")

    building_columns = [col for col in buildings_data.c if col.name not in ("physical_object_id", "properties")]
    territories_cte = include_child_territories_cte(territory_id)
    statement = (
        select(
            urban_objects_data,
            physical_objects_data.c.physical_object_type_id,
            physical_object_types_dict.c.name.label("physical_object_type_name"),
            physical_object_types_dict.c.physical_object_function_id,
            physical_object_functions_dict.c.name.label("physical_object_function_name"),
            physical_objects_data.c.name.label("physical_object_name"),
            physical_objects_data.c.properties.label("physical_object_properties"),
            physical_objects_data.c.created_at.label("physical_object_created_at"),
            physical_objects_data.c.updated_at.label("physical_object_updated_at"),
            object_geometries_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
            ST_AsEWKB(object_geometries_data.c.geometry).label("geometry"),
            ST_AsEWKB(object_geometries_data.c.centre_point).label("centre_point"),
            object_geometries_data.c.created_at.label("object_geometry_created_at"),
            object_geometries_data.c.updated_at.label("object_geometry_updated_at"),
            services_data.c.name.label("service_name"),
            services_data.c.capacity,
            services_data.c.is_capacity_real,
            services_data.c.properties.label("service_properties"),
            services_data.c.created_at.label("service_created_at"),
            services_data.c.updated_at.label("service_updated_at"),
            object_geometries_data.c.address,
            object_geometries_data.c.osm_id,
            service_types_dict.c.service_type_id,
            service_types_dict.c.urban_function_id,
            urban_functions_dict.c.name.label("urban_function_name"),
            service_types_dict.c.name.label("service_type_name"),
            service_types_dict.c.capacity_modeled.label("service_type_capacity_modeled"),
            service_types_dict.c.code.label("service_type_code"),
            service_types_dict.c.infrastructure_type,
            service_types_dict.c.properties.label("service_type_properties"),
            territory_types_dict.c.territory_type_id,
            territory_types_dict.c.name.label("territory_type_name"),
            *building_columns,
            buildings_data.c.properties.label("building_properties"),
        )
        .select_from(
            urban_objects_data.join(
                physical_objects_data,
                physical_objects_data.c.physical_object_id == urban_objects_data.c.physical_object_id,
            )
            .join(
                object_geometries_data,
                object_geometries_data.c.object_geometry_id == urban_objects_data.c.object_geometry_id,
            )
            .join(
                territories_data,
                territories_data.c.territory_id == object_geometries_data.c.territory_id,
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
            .outerjoin(services_data, services_data.c.service_id == urban_objects_data.c.service_id)
            .outerjoin(service_types_dict, service_types_dict.c.service_type_id == services_data.c.service_type_id)
            .outerjoin(
                urban_functions_dict,
                urban_functions_dict.c.urban_function_id == service_types_dict.c.urban_function_id,
            )
            .outerjoin(
                territory_types_dict, territory_types_dict.c.territory_type_id == services_data.c.territory_type_id
            )
            .outerjoin(
                buildings_data,
                buildings_data.c.physical_object_id == physical_objects_data.c.physical_object_id,
            )
        )
        .where(object_geometries_data.c.territory_id.in_(select(territories_cte.c.territory_id)))
        .order_by(urban_objects_data.c.urban_object_id)
        .distinct()
    )

    statement = apply_filters(
        statement,
        EqFilter(physical_objects_data, "physical_object_type_id", physical_object_type_id),
        EqFilter(services_data, "service_type_id", service_type_id),
    )

    urban_objects = (await conn.execute(statement)).mappings().all()

    return [UrbanObjectDTO(**urban_object) for urban_object in urban_objects]


async def patch_urban_object_to_db(
    conn: AsyncConnection, urban_object: UrbanObjectPatch, urban_object_id: int
) -> UrbanObjectDTO:
    """Update urban object by only given fields."""

    statement = select(urban_objects_data).where(urban_objects_data.c.urban_object_id == urban_object_id)
    existing_object = (await conn.execute(statement)).mappings().one_or_none()
    if existing_object is None:
        raise EntityNotFoundById(urban_object_id, "urban object")

    values = extract_values_from_model(urban_object, exclude_unset=True)

    if urban_object.physical_object_id is not None:
        if not await check_existence(
            conn, physical_objects_data, conditions={"physical_object_id": urban_object.physical_object_id}
        ):
            raise EntityNotFoundById(urban_object.physical_object_id, "physical_object")

    if urban_object.object_geometry_id is not None:
        if not await check_existence(
            conn, object_geometries_data, conditions={"object_geometry_id": urban_object.object_geometry_id}
        ):
            raise EntityNotFoundById(urban_object.object_geometry_id, "object geometry")

    if urban_object.service_id is not None:
        if not await check_existence(conn, services_data, conditions={"service_id": urban_object.service_id}):
            raise EntityNotFoundById(urban_object.service_id, "service")

    if await check_existence(
        conn,
        urban_objects_data,
        conditions={
            key: values.get(key, getattr(existing_object, key))
            for key in ("physical_object_id", "object_geometry_id", "service_id")
        },
        not_conditions={"urban_object_id": urban_object_id},
    ):
        raise EntityAlreadyExists(
            "urban object",
            *(
                values.get(key, getattr(existing_object, key))
                for key in ("physical_object_id", "object_geometry_id", "service_id")
            ),
        )

    statement = (
        update(urban_objects_data).where(urban_objects_data.c.urban_object_id == urban_object_id).values(**values)
    )
    await conn.execute(statement)
    await conn.commit()

    return (await get_urban_objects_by_ids_from_db(conn, [urban_object_id]))[0]
