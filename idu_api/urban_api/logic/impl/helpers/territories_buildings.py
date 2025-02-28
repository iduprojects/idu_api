"""Territories buildings internal logic is defined here."""

from geoalchemy2.functions import ST_AsEWKB
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.common.db.entities import (
    buildings_data,
    object_geometries_data,
    physical_object_types_dict,
    physical_objects_data,
    territories_data,
    urban_objects_data,
)
from idu_api.urban_api.dto import BuildingWithGeometryDTO, PageDTO
from idu_api.urban_api.exceptions.logic.common import EntityNotFoundById
from idu_api.urban_api.logic.impl.helpers.utils import check_existence, include_child_territories_cte
from idu_api.urban_api.utils.pagination import paginate_dto


async def get_buildings_with_geometry_by_territory_id_from_db(
    conn: AsyncConnection,
    territory_id: int,
    include_child_territories: bool,
    cities_only: bool,
) -> PageDTO[BuildingWithGeometryDTO]:
    """Get living buildings with geometry by territory identifier."""

    if not await check_existence(conn, territories_data, conditions={"territory_id": territory_id}):
        raise EntityNotFoundById(territory_id, "territory")

    statement = (
        select(
            buildings_data,
            physical_objects_data.c.name.label("physical_object_name"),
            physical_objects_data.c.properties.label("physical_object_properties"),
            physical_object_types_dict.c.physical_object_type_id,
            physical_object_types_dict.c.name.label("physical_object_type_name"),
            object_geometries_data.c.object_geometry_id,
            object_geometries_data.c.address,
            object_geometries_data.c.osm_id,
            ST_AsEWKB(object_geometries_data.c.geometry).label("geometry"),
            ST_AsEWKB(object_geometries_data.c.centre_point).label("centre_point"),
        )
        .select_from(
            buildings_data.join(
                physical_objects_data,
                physical_objects_data.c.physical_object_id == buildings_data.c.physical_object_id,
            )
            .join(
                physical_object_types_dict,
                physical_objects_data.c.physical_object_type_id == physical_object_types_dict.c.physical_object_type_id,
            )
            .join(
                urban_objects_data,
                physical_objects_data.c.physical_object_id == urban_objects_data.c.physical_object_id,
            )
            .join(
                object_geometries_data,
                urban_objects_data.c.object_geometry_id == object_geometries_data.c.object_geometry_id,
            )
        )
        .distinct()
        .order_by(buildings_data.c.building_id)
    )

    if include_child_territories:
        territories_cte = include_child_territories_cte(territory_id, cities_only)
        statement = statement.where(object_geometries_data.c.territory_id.in_(select(territories_cte.c.territory_id)))
    else:
        statement = statement.where(object_geometries_data.c.territory_id == territory_id)

    return await paginate_dto(conn, statement, transformer=lambda x: [BuildingWithGeometryDTO(**item) for item in x])
