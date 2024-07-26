from geoalchemy2.functions import ST_AsGeoJSON
from sqlalchemy import select, and_, cast
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.city_api.dto.services import CityServiceDTO
from idu_api.common.db.entities import urban_objects_data, physical_objects_data, object_geometries_data, services_data


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
            object_geometries_data, object_geometries_data.c.object_geometry_id == urban_objects_data.c.object_geometry_id
        ).join(
            services_data, services_data.c.service_id == urban_objects_data.c.service_id
        )
    )
    if service_type:
        statement = statement.where(and_(
            object_geometries_data.c.territory_id.in_(ids),
            services_data.c.service_type_id == service_type
        ))
    else:
        statement = statement.where(object_geometries_data.c.territory_id.in_(ids))

    result = (await conn.execute(statement)).mappings().all()

    return [CityServiceDTO(**elem) for elem in result]
