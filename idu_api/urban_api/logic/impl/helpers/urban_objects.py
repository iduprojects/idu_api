"""Urban objects handlers logic of getting entities from the database is defined here."""

from geoalchemy2.functions import ST_AsGeoJSON
from sqlalchemy import cast, delete, select
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.common.db.entities import (
    object_geometries_data,
    physical_object_types_dict,
    physical_objects_data,
    service_types_dict,
    services_data,
    territory_types_dict,
    urban_functions_dict,
    urban_objects_data,
)
from idu_api.urban_api.dto import UrbanObjectDTO
from idu_api.urban_api.exceptions.logic.common import EntityNotFoundById


async def get_urban_object_by_id_from_db(conn: AsyncConnection, urban_object_id: int) -> UrbanObjectDTO:
    """Get urban object by urban object id."""

    statement = (
        select(
            urban_objects_data,
            physical_objects_data.c.physical_object_type_id,
            physical_object_types_dict.c.name.label("physical_object_type_name"),
            physical_objects_data.c.name.label("physical_object_name"),
            physical_objects_data.c.properties.label("physical_object_properties"),
            physical_objects_data.c.created_at.label("physical_object_created_at"),
            physical_objects_data.c.updated_at.label("physical_object_updated_at"),
            object_geometries_data.c.territory_id,
            cast(ST_AsGeoJSON(object_geometries_data.c.geometry), JSONB).label("geometry"),
            cast(ST_AsGeoJSON(object_geometries_data.c.centre_point), JSONB).label("centre_point"),
            object_geometries_data.c.created_at.label("object_geometry_created_at"),
            object_geometries_data.c.updated_at.label("object_geometry_updated_at"),
            services_data.c.name.label("service_name"),
            services_data.c.capacity_real,
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
            territory_types_dict.c.territory_type_id,
            territory_types_dict.c.name.label("territory_type_name"),
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
                physical_object_types_dict,
                physical_object_types_dict.c.physical_object_type_id == physical_objects_data.c.physical_object_type_id,
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
        )
        .where(urban_objects_data.c.urban_object_id == urban_object_id)
    )

    urban_object = (await conn.execute(statement)).mappings().one_or_none()
    if urban_object is None:
        raise EntityNotFoundById(urban_object_id, "urban object")

    return UrbanObjectDTO(**urban_object)


async def get_urban_object_by_physical_object_id_from_db(
    conn: AsyncConnection,
    physical_object_id: int,
) -> list[UrbanObjectDTO]:
    """Get list of urban objects by physical object id."""

    statement = select(physical_objects_data).where(physical_objects_data.c.physical_object_id == physical_object_id)
    physical_object = (await conn.execute(statement)).mappings().one_or_none()
    if physical_object is None:
        raise EntityNotFoundById(physical_object_id, "physical object")

    statement = select(urban_objects_data.c.urban_object_id).where(
        urban_objects_data.c.physical_object_id == physical_object_id
    )
    urban_objects = (await conn.execute(statement)).scalars()

    return [await get_urban_object_by_id_from_db(conn, urban_object_id) for urban_object_id in urban_objects]


async def get_urban_object_by_object_geometry_id_from_db(
    conn: AsyncConnection,
    object_geometry_id: int,
) -> list[UrbanObjectDTO]:
    """Get list of urban objects by object geometry id."""

    statement = select(object_geometries_data).where(object_geometries_data.c.object_geometry_id == object_geometry_id)
    object_geometry = (await conn.execute(statement)).mappings().one_or_none()
    if object_geometry is None:
        raise EntityNotFoundById(object_geometry_id, "object geometry")

    statement = select(urban_objects_data.c.urban_object_id).where(
        urban_objects_data.c.object_geometry_id == object_geometry_id
    )
    urban_objects = (await conn.execute(statement)).scalars()

    return [await get_urban_object_by_id_from_db(conn, urban_object_id) for urban_object_id in urban_objects]


async def get_urban_object_by_service_id_from_db(conn: AsyncConnection, service_id: int) -> list[UrbanObjectDTO]:
    """Get list of urban objects by service id."""

    statement = select(services_data).where(services_data.c.service_id == service_id)
    service = (await conn.execute(statement)).mappings().one_or_none()
    if service is None:
        raise EntityNotFoundById(service_id, "service")

    statement = select(urban_objects_data.c.urban_object_id).where(urban_objects_data.c.service_id == service_id)
    urban_objects = (await conn.execute(statement)).scalars()

    return [await get_urban_object_by_id_from_db(conn, urban_object_id) for urban_object_id in urban_objects]


async def delete_urban_object_by_id_from_db(conn: AsyncConnection, urban_object_id: int) -> dict:
    """Get urban object by urban object id."""

    statement = select(urban_objects_data).where(urban_objects_data.c.urban_object_id == urban_object_id)
    urban_object = (await conn.execute(statement)).mappings().one_or_none()
    if urban_object is None:
        raise EntityNotFoundById(urban_object_id, "urban object")

    statement = delete(urban_objects_data).where(urban_objects_data.c.urban_object_id == urban_object_id)
    await conn.execute(statement)
    await conn.commit()

    return {"result": "ok"}
