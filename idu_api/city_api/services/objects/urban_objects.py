from geoalchemy2.functions import ST_AsGeoJSON
from sqlalchemy import select, and_, cast
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncConnection
from sqlalchemy.sql.functions import count

from idu_api.city_api.dto.physical_objects import PhysicalObjectsDTO
from idu_api.city_api.dto.services import CityServiceDTO
from idu_api.city_api.dto.services_count import ServiceCountDTO
from idu_api.common.db.entities import urban_objects_data, physical_objects_data, object_geometries_data, services_data, \
    service_types_dict


async def get_services_types_by_territory_ids(
        conn: AsyncConnection,
        ids: list[int],
        service_type: int | None = None
) -> list[ServiceCountDTO]:
    statement = select(
        service_types_dict.c.service_type_id,
        service_types_dict.c.name,
        service_types_dict.c.code,
        service_types_dict.c.urban_function_id,
        count(services_data.c.service_id).label("count"),
    ).select_from(
        urban_objects_data.join(
            physical_objects_data, urban_objects_data.c.physical_object_id == physical_objects_data.c.physical_object_id
        ).join(
            object_geometries_data,
            object_geometries_data.c.object_geometry_id == urban_objects_data.c.object_geometry_id
        ).join(
            services_data, services_data.c.service_id == urban_objects_data.c.service_id
        ).join(
            service_types_dict, services_data.c.service_type_id == service_types_dict.c.service_type_id
        )
    )
    if service_type:
        statement = statement.where(and_(
            urban_objects_data.c.service_id != None,
            object_geometries_data.c.territory_id.in_(ids),
            services_data.c.service_type_id == service_type
        ))
    else:
        statement = statement.where(and_(
            urban_objects_data.c.service_id != None,
            object_geometries_data.c.territory_id.in_(ids)
        ))

    statement = statement.group_by(
        service_types_dict.c.service_type_id,
        services_data.c.service_id
    )
    result = (await conn.execute(statement)).mappings().all()

    return [ServiceCountDTO(**elem) for elem in result]


async def get_services_by_territory_ids(
        conn: AsyncConnection,
        ids: list[int],
        service_type: int | None = None
) -> list[CityServiceDTO]:
    statement = select(
        services_data.c.service_id.label("id"),
        services_data.c.name,
        services_data.c.capacity_real.label("capacity"),
        services_data.c.properties,
        cast(ST_AsGeoJSON(object_geometries_data.c.geometry), JSONB).label("geometry"),
        cast(ST_AsGeoJSON(object_geometries_data.c.centre_point), JSONB).label("centre_point"),
    ).select_from(
        urban_objects_data.join(
            physical_objects_data, urban_objects_data.c.physical_object_id == physical_objects_data.c.physical_object_id
        ).join(
            object_geometries_data,
            object_geometries_data.c.object_geometry_id == urban_objects_data.c.object_geometry_id
        ).join(
            services_data, services_data.c.service_id == urban_objects_data.c.service_id
        )
    )
    if service_type:
        statement = statement.where(and_(
            urban_objects_data.c.service_id != None,
            object_geometries_data.c.territory_id.in_(ids),
            services_data.c.service_type_id == service_type
        ))
    else:
        statement = statement.where(and_(
            urban_objects_data.c.service_id != None,
            object_geometries_data.c.territory_id.in_(ids)
        ))

    result = (await conn.execute(statement)).mappings().all()

    return [CityServiceDTO(**elem) for elem in result]


async def get_physical_objects_by_territory_ids(
        conn: AsyncConnection,
        ids: list[int],
) -> list[PhysicalObjectsDTO]:
    statement = select(
        physical_objects_data.c.physical_object_id,
        physical_objects_data.c.name,
        physical_objects_data.c.properties,
        cast(ST_AsGeoJSON(object_geometries_data.c.geometry), JSONB).label("geometry"),
        cast(ST_AsGeoJSON(object_geometries_data.c.centre_point), JSONB).label("centre_point"),
    ).select_from(
        urban_objects_data.join(
            physical_objects_data, urban_objects_data.c.physical_object_id == physical_objects_data.c.physical_object_id
        ).join(
            object_geometries_data,
            object_geometries_data.c.object_geometry_id == urban_objects_data.c.object_geometry_id
        )
    )
    statement = statement.where(and_(
        urban_objects_data.c.service_id == None,
        object_geometries_data.c.territory_id.in_(ids)
    ))

    result = (await conn.execute(statement)).mappings().all()

    return [PhysicalObjectsDTO(**elem) for elem in result]
