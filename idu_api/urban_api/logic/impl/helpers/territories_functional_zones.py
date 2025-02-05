"""Territories functional zones internal logic is defined here."""

from geoalchemy2.functions import ST_AsGeoJSON
from sqlalchemy import cast, delete, select
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.common.db.entities import functional_zone_types_dict, functional_zones_data, territories_data
from idu_api.urban_api.dto import FunctionalZoneDTO, FunctionalZoneSourceDTO
from idu_api.urban_api.exceptions.logic.common import EntityNotFoundById
from idu_api.urban_api.logic.impl.helpers.utils import DECIMAL_PLACES, check_existence, include_child_territories_cte


async def get_functional_zones_sources_by_territory_id_from_db(
    conn: AsyncConnection, territory_id: int, include_child_territories: bool, cities_only: bool
) -> list[FunctionalZoneSourceDTO]:
    """Get list of pairs year + source for functional zones for given territory and its children (optional)."""

    if not await check_existence(conn, territories_data, conditions={"territory_id": territory_id}):
        raise EntityNotFoundById(territory_id, "territory")

    statement = select(functional_zones_data.c.year, functional_zones_data.c.source).distinct()

    if include_child_territories:
        territories_cte = include_child_territories_cte(territory_id, cities_only)
        statement = statement.where(functional_zones_data.c.territory_id.in_(select(territories_cte.c.territory_id)))
    else:
        statement = statement.where(functional_zones_data.c.territory_id == territory_id)

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
) -> list[FunctionalZoneDTO]:
    """Get functional zones with geometry by territory id."""

    if not await check_existence(conn, territories_data, conditions={"territory_id": territory_id}):
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
            cast(ST_AsGeoJSON(functional_zones_data.c.geometry, DECIMAL_PLACES), JSONB).label("geometry"),
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
        territories_cte = include_child_territories_cte(territory_id, cities_only)
        statement = statement.where(functional_zones_data.c.territory_id.in_(select(territories_cte.c.territory_id)))
    else:
        statement = statement.where(functional_zones_data.c.territory_id == territory_id)

    if functional_zone_type_id is not None:
        statement = statement.where(functional_zones_data.c.functional_zone_type_id == functional_zone_type_id)

    result = (await conn.execute(statement)).mappings().all()

    return [FunctionalZoneDTO(**zone) for zone in result]


async def delete_all_functional_zones_for_territory_from_db(
    conn: AsyncConnection, territory_id: int, include_child_territories: bool, cities_only: bool
) -> dict:
    """Delete all functional zones for given territory and its children."""

    if not await check_existence(conn, territories_data, conditions={"territory_id": territory_id}):
        raise EntityNotFoundById(territory_id, "territory")

    statement = delete(functional_zones_data)

    if include_child_territories:
        territories_cte = include_child_territories_cte(territory_id, cities_only)
        statement = statement.where(functional_zones_data.c.territory_id.in_(select(territories_cte.c.territory_id)))
    else:
        statement = statement.where(functional_zones_data.c.territory_id == territory_id)

    await conn.execute(statement)
    await conn.commit()

    return {"status": "ok"}
