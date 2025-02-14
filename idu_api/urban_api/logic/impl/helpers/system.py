"""System methods internal logic is defined here."""

import structlog
from geoalchemy2.functions import ST_AsEWKB, ST_GeomFromWKB, ST_IsValid, ST_MakeValid
from shapely import geometry as shapely_geom
from shapely.wkb import dumps as wkb_dumps
from shapely.wkb import loads as wkb_loads
from sqlalchemy.ext.asyncio import AsyncConnection
from sqlalchemy.sql import case, column, select, text, values

from idu_api.urban_api.exceptions.logic.common import TooManyObjectsError
from idu_api.urban_api.logic.impl.helpers.utils import OBJECTS_NUMBER_LIMIT

Geom = (
    shapely_geom.Point
    | shapely_geom.Polygon
    | shapely_geom.MultiPolygon
    | shapely_geom.LineString
    | shapely_geom.MultiLineString
)


async def fix_geometry_by_postgis(conn: AsyncConnection, geom: Geom, logger: structlog.stdlib.BoundLogger) -> Geom:
    """Fix geometry by PostGIS methods using binary (WKB/EWKB) representation for efficiency."""

    geom_wkb = wkb_dumps(geom)
    geom_expr = ST_GeomFromWKB(geom_wkb, text("4326"))

    validity_case = case((ST_IsValid(geom_expr), geom_expr), else_=ST_MakeValid(geom_expr))
    statement = select(ST_AsEWKB(validity_case))

    result = (await conn.execute(statement)).scalar_one_or_none()
    if result is None:
        await logger.aerror("PostGIS returned NULL for geometry", geometry=geom_wkb)
        raise ValueError("Failed to fix geometry")

    fixed_geom = wkb_loads(result)

    return fixed_geom


async def fix_geojson_by_postgis(
    conn: AsyncConnection, geoms: list[Geom], logger: structlog.stdlib.BoundLogger
) -> list[Geom]:
    """Fix list of geometries by postgis methods."""
    if len(geoms) > OBJECTS_NUMBER_LIMIT:
        raise TooManyObjectsError(len(geoms), OBJECTS_NUMBER_LIMIT)

    geoms_wkb = [wkb_dumps(geom) for geom in geoms]
    values_clause = values(column("geom_wkb"), name="v").data([(wkb_val,) for wkb_val in geoms_wkb])
    geom_expr = ST_GeomFromWKB(values_clause.c.geom_wkb, text("4326"))

    validity_case = case((ST_IsValid(geom_expr), geom_expr), else_=ST_MakeValid(geom_expr))
    statement = select(ST_AsEWKB(validity_case)).select_from(values_clause)

    result = (await conn.execute(statement)).scalars().all()

    fixed_geoms = [wkb_loads(fixed_wkb) for fixed_wkb in result]

    if len(fixed_geoms) < len(geoms) or None in fixed_geoms:
        await logger.aerror("error on fixing geometries")
        raise ValueError("Failed to fix geometry")

    return fixed_geoms
