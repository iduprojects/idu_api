"""Physical objects entities internal logic is defined here."""

from typing import Callable

from geoalchemy2 import Geography, Geometry
from geoalchemy2.functions import ST_GeomFromText
from shapely.geometry import LineString, MultiPolygon, Point, Polygon
from sqlalchemy import cast, func, select, text
from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.common.db.entities import (
    object_geometries_data,
    physical_object_types_dict,
    physical_objects_data,
    territories_data,
    urban_objects_data,
)
from idu_api.urban_api.dto import PhysicalObjectDataDTO
from idu_api.urban_api.exceptions.logic.common import EntitiesNotFoundByIds

func: Callable
Geom = Point | Polygon | MultiPolygon | LineString


async def get_physical_objects_by_ids(conn: AsyncConnection, ids: list[int]) -> list[PhysicalObjectDataDTO]:
    """Get physical objects by list of ids."""
    statement = (
        select(
            physical_objects_data,
            physical_object_types_dict.c.name.label("physical_object_type_name"),
        )
        .select_from(
            physical_objects_data.join(
                physical_object_types_dict,
                physical_objects_data.c.physical_object_type_id == physical_object_types_dict.c.physical_object_type_id,
            )
        )
        .where(physical_objects_data.c.physical_object_id.in_(ids))
    )

    results = (await conn.execute(statement)).mappings().all()
    if not list(results):
        raise EntitiesNotFoundByIds("physical_object")

    return [PhysicalObjectDataDTO(**physical_object) for physical_object in results]


async def get_physical_objects_around(
    conn: AsyncConnection, geometry: Geom, physical_object_type_id: int, buffer_meters: int
) -> list[PhysicalObjectDataDTO]:
    """Get physical objects which are in buffer area of the given geometry."""
    buffered_geometry_cte = select(
        cast(
            func.ST_Buffer(cast(ST_GeomFromText(str(geometry.wkt), text("4326")), Geography(srid=4326)), buffer_meters),
            Geometry(srid=4326),
        ).label("geometry"),
    ).cte("buffered_geometry_cte")

    fine_territories_cte = (
        select(territories_data.c.territory_id.label("territory_id"))
        .where(
            func.ST_Intersects(territories_data.c.geometry, select(buffered_geometry_cte.c.geometry).scalar_subquery())
            | func.ST_Covers(territories_data.c.geometry, select(buffered_geometry_cte.c.geometry).scalar_subquery())
            | func.ST_CoveredBy(territories_data.c.geometry, select(buffered_geometry_cte.c.geometry).scalar_subquery())
        )
        .cte("fine_territories_cte")
    )

    statement = (
        (
            select(physical_objects_data.c.physical_object_id)
            .select_from(physical_objects_data)
            .join(
                urban_objects_data,
                urban_objects_data.c.physical_object_id == physical_objects_data.c.physical_object_id,
            )
            .join(
                object_geometries_data,
                urban_objects_data.c.object_geometry_id == object_geometries_data.c.object_geometry_id,
            )
        )
        .where(
            physical_objects_data.c.physical_object_type_id == physical_object_type_id,
            object_geometries_data.c.territory_id.in_(select(fine_territories_cte.c.territory_id).scalar_subquery()),
            func.ST_Intersects(
                object_geometries_data.c.geometry, select(buffered_geometry_cte.c.geometry).scalar_subquery()
            )
            | func.ST_Covers(
                object_geometries_data.c.geometry, select(buffered_geometry_cte.c.geometry).scalar_subquery()
            )
            | func.ST_CoveredBy(
                object_geometries_data.c.geometry, select(buffered_geometry_cte.c.geometry).scalar_subquery()
            ),
        )
        .distinct()
    )

    ids = (await conn.execute(statement)).scalars().all()

    if len(ids) == 0:
        return []

    return await get_physical_objects_by_ids(conn, ids)
