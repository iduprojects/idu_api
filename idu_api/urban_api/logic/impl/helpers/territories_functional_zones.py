"""Territories functional zones internal logic is defined here."""

from geoalchemy2.functions import ST_AsGeoJSON
from sqlalchemy import cast, delete, select
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.common.db.entities import functional_zone_types_dict, functional_zones_data, territories_data
from idu_api.urban_api.dto import FunctionalZoneDataDTO, FunctionalZoneSourceDTO
from idu_api.urban_api.exceptions.logic.common import EntityNotFoundById
from idu_api.urban_api.logic.impl.helpers.territory_objects import check_territory_existence


async def get_functional_zones_sources_by_territory_id_from_db(
    conn: AsyncConnection, territory_id: int
) -> list[FunctionalZoneSourceDTO]:
    """Get list of pairs year + source for functional zones for given territory and its children."""

    territory_exists = await check_territory_existence(conn, territory_id)
    if not territory_exists:
        raise EntityNotFoundById(territory_id, "territory")

    territories_cte = (
        select(territories_data.c.territory_id)
        .where(territories_data.c.territory_id == territory_id)
        .cte(recursive=True)
    )
    territories_cte = territories_cte.union_all(
        select(territories_data.c.territory_id).join(
            territories_cte, territories_data.c.parent_id == territories_cte.c.territory_id
        )
    )

    statement = (
        select(functional_zones_data.c.year, functional_zones_data.c.source)
        .where(functional_zones_data.c.territory_id.in_(select(territories_cte)))
        .distinct()
    )
    result = (await conn.execute(statement)).mappings().all()

    return [FunctionalZoneSourceDTO(**res) for res in result]


async def get_functional_zones_by_territory_id_from_db(
    conn: AsyncConnection,
    territory_id: int,
    year: int,
    source: str,
    functional_zone_type_id: int | None,
    include_child_territories: bool,
    cities_only: bool,
) -> list[FunctionalZoneDataDTO]:
    """Get functional zones with geometry by territory id."""

    territory_exists = await check_territory_existence(conn, territory_id)
    if not territory_exists:
        raise EntityNotFoundById(territory_id, "territory")

    statement = (
        select(
            functional_zones_data.c.functional_zone_id,
            functional_zones_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
            functional_zones_data.c.functional_zone_type_id,
            functional_zone_types_dict.c.name.label("functional_zone_type_name"),
            functional_zone_types_dict.c.zone_nickname.label("functional_zone_type_nickname"),
            functional_zones_data.c.name,
            cast(ST_AsGeoJSON(functional_zones_data.c.geometry), JSONB).label("geometry"),
            functional_zones_data.c.year,
            functional_zones_data.c.source,
            functional_zones_data.c.properties,
            functional_zones_data.c.created_at,
            functional_zones_data.c.updated_at,
        )
        .select_from(
            functional_zones_data.join(
                territories_data,
                territories_data.c.territory_id == functional_zones_data.c.territory_id,
            ).join(
                functional_zone_types_dict,
                functional_zone_types_dict.c.functional_zone_type_id == functional_zones_data.c.functional_zone_type_id,
            )
        )
        .where(functional_zones_data.c.year == year, functional_zones_data.c.source == source)
    )

    if include_child_territories:
        territories_cte = (
            select(territories_data.c.territory_id, territories_data.c.is_city)
            .where(territories_data.c.territory_id == territory_id)
            .cte(recursive=True)
        )
        territories_cte = territories_cte.union_all(
            select(territories_data.c.territory_id, territories_data.c.is_city).where(
                territories_data.c.parent_id == territories_cte.c.territory_id
            )
        )

        if cities_only:
            territories_cte = select(territories_cte.c.territory_id).where(territories_cte.c.is_city.is_(cities_only))

        statement = statement.where(functional_zones_data.c.territory_id.in_(select(territories_cte.c.territory_id)))
    else:
        statement = statement.where(functional_zones_data.c.territory_id == territory_id)

    if functional_zone_type_id is not None:
        statement = statement.where(functional_zones_data.c.functional_zone_type_id == functional_zone_type_id)

    result = (await conn.execute(statement)).mappings().all()

    return [FunctionalZoneDataDTO(**zone) for zone in result]


async def delete_all_functional_zones_for_territory_from_db(conn: AsyncConnection, territory_id: int) -> dict:
    """Delete all functional zones for given territory and its children."""

    territory_exists = await check_territory_existence(conn, territory_id)
    if not territory_exists:
        raise EntityNotFoundById(territory_id, "territory")

    territories_cte = (
        select(territories_data.c.territory_id)
        .where(territories_data.c.territory_id == territory_id)
        .cte(recursive=True)
    )

    territories_cte = territories_cte.union_all(
        select(territories_data.c.territory_id).join(
            territories_cte, territories_data.c.parent_id == territories_cte.c.territory_id
        )
    )

    statement = delete(functional_zones_data).where(functional_zones_data.c.territory_id.in_(select(territories_cte)))
    await conn.execute(statement)
    await conn.commit()

    return {"result": "ok"}
