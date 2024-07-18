"""Territories buildings internal logic is defined here."""

from geoalchemy2.functions import ST_AsGeoJSON
from sqlalchemy import cast, func, select
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
from idu_api.urban_api.dto import LivingBuildingsWithGeometryDTO
from idu_api.urban_api.exceptions.logic.common import EntityNotFoundById


async def get_living_buildings_with_geometry_by_territory_id_from_db(
    conn: AsyncConnection,
    territory_id: int,
) -> list[LivingBuildingsWithGeometryDTO]:
    """Get living buildings with geometry by territory id."""

    statement = select(territories_data).where(territories_data.c.territory_id == territory_id)
    territory = (await conn.execute(statement)).one_or_none()
    if territory is None:
        raise EntityNotFoundById(territory_id, "territory")

    subquery = (
        select(
            urban_objects_data.c.physical_object_id,
            func.max(urban_objects_data.c.object_geometry_id).label("object_geometry_id"),
        )
        .group_by(urban_objects_data.c.physical_object_id)
        .subquery()
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
            physical_object_types_dict.c.physical_object_type_id,
            physical_object_types_dict.c.name.label("physical_object_type_name"),
            object_geometries_data.c.address.label("physical_object_address"),
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
                subquery,
                physical_objects_data.c.physical_object_id == subquery.c.physical_object_id,
            )
            .join(
                object_geometries_data,
                subquery.c.object_geometry_id == object_geometries_data.c.object_geometry_id,
            )
        )
        .where(object_geometries_data.c.territory_id == territory_id)
        .distinct()
    )

    result = (await conn.execute(statement)).mappings().all()

    return [LivingBuildingsWithGeometryDTO(**living_building) for living_building in result]
