"""System methods internal logic is defined here."""

import structlog
from geoalchemy2.functions import (
    ST_AsEWKB,
    ST_Buffer,
    ST_CollectionExtract,
    ST_GeomFromWKB,
    ST_IsEmpty,
    ST_IsValid,
    ST_MakeValid,
)
from shapely import geometry as shapely_geom
from shapely.wkb import dumps as wkb_dumps
from shapely.wkb import loads as wkb_loads
from sqlalchemy.ext.asyncio import AsyncConnection
from sqlalchemy.sql import case, column, select, text, values
from sqlalchemy.sql.functions import coalesce

from idu_api.urban_api.logic.impl.helpers.utils import OBJECTS_NUMBER_TO_INSERT_LIMIT

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
    conn: AsyncConnection,
    geoms: list[Geom],
    logger: structlog.stdlib.BoundLogger,
    show_progress: bool = False,
) -> list[Geom]:
    """Fix list of geometries by postgis methods, processing them in batches."""
    fixed_geoms = []
    batches = [
        geoms[i : i + OBJECTS_NUMBER_TO_INSERT_LIMIT] for i in range(0, len(geoms), OBJECTS_NUMBER_TO_INSERT_LIMIT)
    ]
    iterator = enumerate(batches)
    if show_progress:
        from tqdm import tqdm

        iterator = tqdm(iterator, total=len(batches), desc="Fixing geometries")

    for batch_idx, batch in iterator:
        try:
            batch_fixed = await process_geometries_batch(conn, batch, batch_idx, logger)
        except Exception as e:
            await logger.aerror("Batch processing failed", batch=batch_idx, error=str(e))
            batch_fixed = batch  # fallback
        fixed_geoms.extend(batch_fixed)

    return fixed_geoms


async def process_geometries_batch(
    conn: AsyncConnection,
    batch: list[Geom],
    batch_idx: int,
    logger: structlog.stdlib.BoundLogger,
) -> list[Geom]:
    """Process a batch of geometries."""
    batch_size = len(batch)
    geoms_wkb = [wkb_dumps(geom) for geom in batch]
    values_clause = values(column("geom_wkb"), name="v").data([(wkb_val,) for wkb_val in geoms_wkb])

    geom_expr = ST_GeomFromWKB(values_clause.c.geom_wkb, text("4326"))
    buffered_geom = ST_Buffer(geom_expr, 0)
    geom_case = case((ST_IsEmpty(buffered_geom), geom_expr), else_=buffered_geom)
    validity_case = case((ST_IsValid(geom_case), geom_case), else_=ST_MakeValid(geom_case))
    processed_geom = coalesce(
        ST_CollectionExtract(validity_case, 3),
        ST_CollectionExtract(validity_case, 2),
        ST_CollectionExtract(validity_case, 1),
        validity_case,
    )

    statement = select(ST_AsEWKB(processed_geom)).select_from(values_clause)
    result = (await conn.execute(statement)).scalars().all()

    batch_fixed = []
    for i, wkb_data in enumerate(result):
        try:
            geom = wkb_loads(wkb_data) if wkb_data else None
            if not geom or geom.is_empty:
                await logger.awarning(
                    "empty geometry after processing",
                    batch=batch_idx,
                    index_in_batch=f"{i}/{batch_size}",
                )
                geom = batch[i]  # fallback
            if geom.geom_type == "GeometryCollection":
                await logger.awarning(
                    "GeometryCollection after processing",
                    batch=batch_idx,
                    index_in_batch=f"{i}/{batch_size}",
                )
            batch_fixed.append(geom)
        except Exception as e:
            await logger.aerror(
                "WKB load failed",
                error=str(e),
                batch=batch_idx,
                index_in_batch=f"{i}/{batch_size}",
            )
            batch_fixed.append(batch[i])  # fallback
    return batch_fixed
