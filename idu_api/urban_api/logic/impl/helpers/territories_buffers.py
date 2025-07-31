"""Territories buffers internal logic is defined here."""

from geoalchemy2.functions import ST_AsEWKB
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.common.db.entities import (
    buffer_types_dict,
    buffers_data,
    object_geometries_data,
    physical_object_types_dict,
    physical_objects_data,
    service_types_dict,
    services_data,
    territories_data,
    urban_objects_data,
)
from idu_api.urban_api.dto import BufferDTO
from idu_api.urban_api.exceptions.logic.common import EntityNotFoundById
from idu_api.urban_api.logic.impl.helpers.utils import check_existence, include_child_territories_cte
from idu_api.urban_api.utils.query_filters import CustomFilter, EqFilter, apply_filters


async def get_buffers_by_territory_id_from_db(
    conn: AsyncConnection,
    territory_id: int,
    include_child_territories: bool,
    cities_only: bool,
    buffer_type_id: int | None,
    physical_object_type_id: int | None,
    service_type_id: int | None,
) -> list[BufferDTO]:
    """Get buffers by territory identifier."""

    if not await check_existence(conn, territories_data, conditions={"territory_id": territory_id}):
        raise EntityNotFoundById(territory_id, "territory")

    statement = (
        select(
            buffer_types_dict.c.buffer_type_id,
            buffer_types_dict.c.name.label("buffer_type_name"),
            urban_objects_data.c.urban_object_id,
            physical_objects_data.c.physical_object_id,
            physical_objects_data.c.name.label("physical_object_name"),
            physical_object_types_dict.c.physical_object_type_id,
            physical_object_types_dict.c.name.label("physical_object_type_name"),
            object_geometries_data.c.object_geometry_id,
            territories_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
            services_data.c.service_id,
            services_data.c.name.label("service_name"),
            service_types_dict.c.service_type_id,
            service_types_dict.c.name.label("service_type_name"),
            ST_AsEWKB(buffers_data.c.geometry).label("geometry"),
            buffers_data.c.is_custom,
        )
        .select_from(
            buffers_data.join(buffer_types_dict, buffer_types_dict.c.buffer_type_id == buffers_data.c.buffer_type_id)
            .join(urban_objects_data, urban_objects_data.c.urban_object_id == buffers_data.c.urban_object_id)
            .join(
                physical_objects_data,
                physical_objects_data.c.physical_object_id == urban_objects_data.c.physical_object_id,
            )
            .join(
                physical_object_types_dict,
                physical_object_types_dict.c.physical_object_type_id == physical_objects_data.c.physical_object_type_id,
            )
            .join(
                object_geometries_data,
                object_geometries_data.c.object_geometry_id == urban_objects_data.c.object_geometry_id,
            )
            .join(territories_data, territories_data.c.territory_id == object_geometries_data.c.territory_id)
            .outerjoin(services_data, services_data.c.service_id == urban_objects_data.c.service_id)
            .outerjoin(service_types_dict, service_types_dict.c.service_type_id == services_data.c.service_type_id)
        )
        .distinct()
    )

    if include_child_territories:
        territories_cte = include_child_territories_cte(territory_id, cities_only)
        territory_filter = CustomFilter(
            lambda q: q.where(territories_data.c.territory_id.in_(select(territories_cte.c.territory_id)))
        )
    else:
        territory_filter = EqFilter(territories_data, "territory_id", territory_id)

    statement = apply_filters(
        statement,
        territory_filter,
        EqFilter(buffer_types_dict, "buffer_type_id", buffer_type_id),
        EqFilter(physical_object_types_dict, "physical_object_type_id", physical_object_type_id),
        EqFilter(service_types_dict, "service_type_id", service_type_id),
    )

    result = (await conn.execute(statement)).mappings().all()

    return [BufferDTO(**buffer) for buffer in result]
