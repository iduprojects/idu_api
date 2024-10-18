"""Territories buildings internal logic is defined here."""

from geoalchemy2.functions import ST_AsGeoJSON
from sqlalchemy import cast, select
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.common.db.entities import (
    living_buildings_data,
    object_geometries_data,
    physical_object_types_dict,
    physical_objects_data,
    territories_data,
    urban_objects_data,
)
from idu_api.urban_api.dto import LivingBuildingsWithGeometryDTO, PageDTO
from idu_api.urban_api.exceptions.logic.common import EntityNotFoundById
from idu_api.urban_api.utils.pagination import paginate_dto


async def get_living_buildings_with_geometry_by_territory_id_from_db(
    conn: AsyncConnection,
    territory_id: int,
) -> PageDTO[LivingBuildingsWithGeometryDTO]:
    """Get living buildings with geometry by territory id."""

    statement = select(territories_data).where(territories_data.c.territory_id == territory_id)
    territory = (await conn.execute(statement)).one_or_none()
    if territory is None:
        raise EntityNotFoundById(territory_id, "territory")

    territories_cte = (
        select(territories_data.c.territory_id)
        .where(territories_data.c.territory_id == territory_id)
        .cte(recursive=True)
    )

    territories_cte = territories_cte.union_all(
        select(territories_data.c.territory_id).where(territories_data.c.parent_id == territories_cte.c.territory_id)
    )

    statement = (
        select(
            living_buildings_data.c.living_building_id,
            living_buildings_data.c.residents_number,
            living_buildings_data.c.living_area,
            living_buildings_data.c.properties,
            physical_objects_data.c.physical_object_id,
            physical_objects_data.c.name.label("physical_object_name"),
            physical_objects_data.c.properties.label("physical_object_properties"),
            physical_objects_data.c.created_at.label("physical_object_created_at"),
            physical_objects_data.c.updated_at.label("physical_object_updated_at"),
            physical_object_types_dict.c.physical_object_type_id,
            physical_object_types_dict.c.name.label("physical_object_type_name"),
            object_geometries_data.c.address.label("physical_object_address"),
            object_geometries_data.c.osm_id.label("object_geometry_osm_id"),
            cast(ST_AsGeoJSON(object_geometries_data.c.geometry), JSONB).label("geometry"),
            cast(ST_AsGeoJSON(object_geometries_data.c.centre_point), JSONB).label("centre_point"),
        )
        .select_from(
            living_buildings_data.join(
                physical_objects_data,
                physical_objects_data.c.physical_object_id == living_buildings_data.c.physical_object_id,
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
        .where(object_geometries_data.c.territory_id.in_(select(territories_cte.c.territory_id)))
        .order_by(living_buildings_data.c.living_building_id)
    )

    return await paginate_dto(
        conn, statement, transformer=lambda x: [LivingBuildingsWithGeometryDTO(**item) for item in x]
    )
