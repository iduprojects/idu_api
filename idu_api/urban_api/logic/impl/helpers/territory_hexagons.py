"""Territories hexagons internal logic is defined here."""

import asyncio

from geoalchemy2.functions import ST_AsGeoJSON, ST_GeomFromText
from sqlalchemy import cast, delete, insert, select, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.common.db.entities import hexagons_data, territories_data
from idu_api.urban_api.dto import HexagonDTO
from idu_api.urban_api.exceptions.logic.common import EntityNotFoundById
from idu_api.urban_api.logic.impl.helpers.territory_objects import check_territory_existence
from idu_api.urban_api.schemas import HexagonPost

OBJECTS_NUMBER_TO_INSERT_LIMIT: int = 7_000


async def get_hexagons_by_ids(conn: AsyncConnection, hexagon_ids: list[int]) -> list[HexagonDTO]:
    """Get hexagons by given ids."""

    statement = (
        select(
            hexagons_data.c.hexagon_id,
            hexagons_data.c.territory_id,
            cast(ST_AsGeoJSON(hexagons_data.c.geometry), JSONB).label("geometry"),
            cast(ST_AsGeoJSON(hexagons_data.c.centre_point), JSONB).label("centre_point"),
            hexagons_data.c.properties,
            territories_data.c.name.label("territory_name"),
        )
        .select_from(
            hexagons_data.join(territories_data, hexagons_data.c.territory_id == territories_data.c.territory_id)
        )
        .where(hexagons_data.c.hexagon_id.in_(hexagon_ids))
    )

    hexagons = (await conn.execute(statement)).mappings().all()

    return [HexagonDTO(**hexagon) for hexagon in hexagons]


async def get_hexagons_by_territory_id_from_db(conn: AsyncConnection, territory_id: int) -> list[HexagonDTO]:
    """Get hexagons for a given territory."""

    territory_exists = await check_territory_existence(conn, territory_id)
    if not territory_exists:
        raise EntityNotFoundById(territory_id, "territory")

    statement = (
        select(
            hexagons_data.c.hexagon_id,
            hexagons_data.c.territory_id,
            cast(ST_AsGeoJSON(hexagons_data.c.geometry), JSONB).label("geometry"),
            cast(ST_AsGeoJSON(hexagons_data.c.centre_point), JSONB).label("centre_point"),
            hexagons_data.c.properties,
            territories_data.c.name.label("territory_name"),
        )
        .select_from(
            hexagons_data.join(territories_data, hexagons_data.c.territory_id == territories_data.c.territory_id)
        )
        .where(hexagons_data.c.territory_id == territory_id)
    )

    hexagons = (await conn.execute(statement)).mappings().all()

    return [HexagonDTO(**hexagon) for hexagon in hexagons]


async def add_hexagons_by_territory_id_to_db(
    conn: AsyncConnection, territory_id: int, hexagons: list[HexagonPost]
) -> list[HexagonDTO]:
    """Create hexagons for a given territory using asyncio.gather."""

    territory_exists = await check_territory_existence(conn, territory_id)
    if not territory_exists:
        raise EntityNotFoundById(territory_id, "territory")

    async def insert_batch(batch: list[HexagonPost]):
        insert_values = [
            {
                "territory_id": territory_id,
                "geometry": ST_GeomFromText(str(hexagon.geometry.as_shapely_geometry()), text("4326")),
                "centre_point": ST_GeomFromText(str(hexagon.centre_point.as_shapely_geometry()), text("4326")),
                "properties": hexagon.properties,
            }
            for hexagon in batch
        ]

        statement = insert(hexagons_data).values(insert_values).returning(hexagons_data.c.hexagon_id)
        return (await conn.execute(statement)).scalars().all()

    tasks = []
    for batch_start in range(0, len(hexagons), OBJECTS_NUMBER_TO_INSERT_LIMIT):
        batch = hexagons[batch_start : batch_start + OBJECTS_NUMBER_TO_INSERT_LIMIT]
        tasks.append(insert_batch(batch))

    results = await asyncio.gather(*tasks)

    hexagon_ids = [hexagon_id for batch_ids in results for hexagon_id in batch_ids]

    await conn.commit()

    return await get_hexagons_by_ids(conn, hexagon_ids)


async def delete_hexagons_by_territory_id_from_db(conn: AsyncConnection, territory_id: int) -> dict:
    """Delete hexagons for a given territory."""

    territory_exists = await check_territory_existence(conn, territory_id)
    if not territory_exists:
        raise EntityNotFoundById(territory_id, "territory")

    statement = delete(hexagons_data).where(hexagons_data.c.territory_id == territory_id)

    await conn.execute(statement)
    await conn.commit()

    return {"result": "ok"}
