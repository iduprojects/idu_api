"""Projects functional zones internal logic is defined here."""

from geoalchemy2.functions import ST_AsGeoJSON, ST_Intersection, ST_Intersects
from sqlalchemy import cast, delete, insert, select, update
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.common.db.entities import (
    functional_zone_types_dict,
    functional_zones_data,
    projects_functional_zones,
    scenarios_data,
    territories_data,
)
from idu_api.urban_api.dto import FunctionalZoneDTO, FunctionalZoneSourceDTO, ScenarioFunctionalZoneDTO
from idu_api.urban_api.exceptions.logic.common import EntitiesNotFoundByIds, EntityNotFoundById, TooManyObjectsError
from idu_api.urban_api.logic.impl.helpers.projects_scenarios import check_scenario
from idu_api.urban_api.logic.impl.helpers.utils import (
    DECIMAL_PLACES,
    OBJECTS_NUMBER_LIMIT,
    check_existence,
    extract_values_from_model,
    get_all_context_territories,
)
from idu_api.urban_api.schemas import (
    ScenarioFunctionalZonePatch,
    ScenarioFunctionalZonePost,
    ScenarioFunctionalZonePut,
)


async def get_functional_zones_sources_by_scenario_id_from_db(
    conn: AsyncConnection,
    scenario_id: int,
    user_id: str,
) -> list[FunctionalZoneSourceDTO]:
    """Get list of pairs year + source for functional zones for given scenario."""

    await check_scenario(conn, scenario_id, user_id)

    statement = (
        select(projects_functional_zones.c.year, projects_functional_zones.c.source)
        .where(projects_functional_zones.c.scenario_id == scenario_id)
        .distinct()
    )
    result = (await conn.execute(statement)).mappings().all()

    return [FunctionalZoneSourceDTO(**source) for source in result]


async def get_functional_zones_by_scenario_id_from_db(
    conn: AsyncConnection,
    scenario_id: int,
    year: int,
    source: str,
    functional_zone_type_id: int | None,
    user_id: str,
) -> list[ScenarioFunctionalZoneDTO]:
    """Get list of functional zone objects by scenario identifier."""

    await check_scenario(conn, scenario_id, user_id)

    statement = (
        select(
            projects_functional_zones.c.profile_id.label("functional_zone_id"),
            projects_functional_zones.c.scenario_id,
            scenarios_data.c.name.label("scenario_name"),
            projects_functional_zones.c.functional_zone_type_id,
            functional_zone_types_dict.c.name.label("functional_zone_type_name"),
            functional_zone_types_dict.c.zone_nickname.label("functional_zone_type_nickname"),
            projects_functional_zones.c.name,
            cast(ST_AsGeoJSON(projects_functional_zones.c.geometry, DECIMAL_PLACES), JSONB).label("geometry"),
            projects_functional_zones.c.year,
            projects_functional_zones.c.source,
            projects_functional_zones.c.properties,
            projects_functional_zones.c.created_at,
            projects_functional_zones.c.updated_at,
        )
        .select_from(
            projects_functional_zones.join(
                scenarios_data,
                scenarios_data.c.scenario_id == projects_functional_zones.c.scenario_id,
            ).join(
                functional_zone_types_dict,
                functional_zone_types_dict.c.functional_zone_type_id
                == projects_functional_zones.c.functional_zone_type_id,
            )
        )
        .where(
            projects_functional_zones.c.scenario_id == scenario_id,
            projects_functional_zones.c.year == year,
            projects_functional_zones.c.source == source,
        )
    )

    if functional_zone_type_id is not None:
        statement = statement.where(projects_functional_zones.c.functional_zone_type_id == functional_zone_type_id)

    result = (await conn.execute(statement)).mappings().all()

    return [ScenarioFunctionalZoneDTO(**profile) for profile in result]


async def get_context_functional_zones_sources_from_db(
    conn: AsyncConnection,
    project_id: int,
    user_id: str,
) -> list[FunctionalZoneSourceDTO]:
    """Get list of pairs year + source for functional zones for 'context' of the project territory."""

    context = await get_all_context_territories(conn, project_id, user_id)

    statement = (
        select(functional_zones_data.c.year, functional_zones_data.c.source)
        .where(
            functional_zones_data.c.territory_id.in_(select(context["territories"].c.territory_id)),
            ST_Intersects(functional_zones_data.c.geometry, context["geometry"]),
        )
        .distinct()
    )
    result = (await conn.execute(statement)).mappings().all()

    return [FunctionalZoneSourceDTO(**res) for res in result]


async def get_context_functional_zones_from_db(
    conn: AsyncConnection,
    project_id: int,
    year: int,
    source: str,
    functional_zone_type_id: int | None,
    user_id: str,
) -> list[FunctionalZoneDTO]:
    """Get list of functional zone objects for 'context' of the project territory."""

    context = await get_all_context_territories(conn, project_id, user_id)

    statement = (
        select(
            functional_zones_data.c.functional_zone_id,
            functional_zones_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
            functional_zones_data.c.functional_zone_type_id,
            functional_zone_types_dict.c.name.label("functional_zone_type_name"),
            functional_zone_types_dict.c.zone_nickname.label("functional_zone_type_nickname"),
            functional_zones_data.c.name,
            cast(
                ST_AsGeoJSON(ST_Intersection(functional_zones_data.c.geometry, context["geometry"]), DECIMAL_PLACES),
                JSONB,
            ).label("geometry"),
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
        .where(
            functional_zones_data.c.year == year,
            functional_zones_data.c.source == source,
            functional_zones_data.c.territory_id.in_(select(context["territories"].c.territory_id)),
            ST_Intersects(functional_zones_data.c.geometry, context["geometry"]),
        )
    )

    if functional_zone_type_id is not None:
        statement = statement.where(functional_zones_data.c.functional_zone_type_id == functional_zone_type_id)

    result = (await conn.execute(statement)).mappings().all()

    return [FunctionalZoneDTO(**zone) for zone in result]


async def get_functional_zone_by_ids(conn: AsyncConnection, ids: list[int]) -> list[ScenarioFunctionalZoneDTO]:
    """Get functional zone by identifier."""

    if len(ids) > OBJECTS_NUMBER_LIMIT:
        raise TooManyObjectsError(len(ids), OBJECTS_NUMBER_LIMIT)

    statement = (
        select(
            projects_functional_zones.c.profile_id.label("functional_zone_id"),
            projects_functional_zones.c.scenario_id,
            scenarios_data.c.name.label("scenario_name"),
            projects_functional_zones.c.functional_zone_type_id,
            functional_zone_types_dict.c.name.label("functional_zone_type_name"),
            functional_zone_types_dict.c.zone_nickname.label("functional_zone_type_nickname"),
            projects_functional_zones.c.name,
            cast(ST_AsGeoJSON(projects_functional_zones.c.geometry, DECIMAL_PLACES), JSONB).label("geometry"),
            projects_functional_zones.c.year,
            projects_functional_zones.c.source,
            projects_functional_zones.c.properties,
            projects_functional_zones.c.created_at,
            projects_functional_zones.c.updated_at,
        )
        .select_from(
            projects_functional_zones.join(
                scenarios_data,
                scenarios_data.c.scenario_id == projects_functional_zones.c.scenario_id,
            ).join(
                functional_zone_types_dict,
                functional_zone_types_dict.c.functional_zone_type_id
                == projects_functional_zones.c.functional_zone_type_id,
            )
        )
        .where(projects_functional_zones.c.profile_id.in_(ids))
    )

    result = (await conn.execute(statement)).mappings().all()
    if len(result) < len(ids):
        raise EntitiesNotFoundByIds("scenario functional zone")

    return [ScenarioFunctionalZoneDTO(**zone) for zone in result]


async def add_scenario_functional_zones_to_db(
    conn: AsyncConnection, functional_zones: list[ScenarioFunctionalZonePost], scenario_id: int, user_id: str
) -> list[ScenarioFunctionalZoneDTO]:
    """Add list of scenario functional zone objects."""

    await check_scenario(conn, scenario_id, user_id, to_edit=True)

    statement = delete(projects_functional_zones).where(projects_functional_zones.c.scenario_id == scenario_id)
    await conn.execute(statement)

    functional_zone_type_ids = {functional_zone.functional_zone_type_id for functional_zone in functional_zones}
    statement = select(functional_zone_types_dict.c.functional_zone_type_id).where(
        functional_zone_types_dict.c.functional_zone_type_id.in_(functional_zone_type_ids)
    )
    functional_zone_types = (await conn.execute(statement)).scalars().all()
    if len(functional_zone_types) < len(functional_zone_type_ids):
        raise EntitiesNotFoundByIds("functional zone type")

    insert_values = [
        {"scenario_id": scenario_id, **extract_values_from_model(functional_zone)}
        for functional_zone in functional_zones
    ]

    statement = (
        insert(projects_functional_zones).values(insert_values).returning(projects_functional_zones.c.profile_id)
    )
    functional_zone_ids = (await conn.execute(statement)).scalars().all()

    await conn.commit()

    return await get_functional_zone_by_ids(conn, functional_zone_ids)


async def put_scenario_functional_zone_to_db(
    conn: AsyncConnection,
    functional_zone: ScenarioFunctionalZonePut,
    scenario_id: int,
    functional_zone_id: int,
    user_id: str,
) -> ScenarioFunctionalZoneDTO:
    """Update scenario functional zone by all its attributes."""

    await check_scenario(conn, scenario_id, user_id, to_edit=True)

    if not await check_existence(conn, projects_functional_zones, conditions={"profile_id": functional_zone_id}):
        raise EntityNotFoundById(functional_zone_id, "scenario functional zone")

    if not await check_existence(
        conn,
        functional_zone_types_dict,
        conditions={"functional_zone_type_id": functional_zone.functional_zone_type_id},
    ):
        raise EntityNotFoundById(functional_zone.functional_zone_type_id, "functional zone type")

    values = extract_values_from_model(functional_zone, to_update=True)
    statement = (
        update(projects_functional_zones)
        .where(projects_functional_zones.c.profile_id == functional_zone_id)
        .values(**values)
    )

    await conn.execute(statement)
    await conn.commit()

    return (await get_functional_zone_by_ids(conn, [functional_zone_id]))[0]


async def patch_scenario_functional_zone_to_db(
    conn: AsyncConnection,
    functional_zone: ScenarioFunctionalZonePatch,
    scenario_id: int,
    functional_zone_id: int,
    user_id: str,
) -> ScenarioFunctionalZoneDTO:
    """Update scenario functional zone by only given attributes."""

    await check_scenario(conn, scenario_id, user_id, to_edit=True)

    if not await check_existence(conn, projects_functional_zones, conditions={"profile_id": functional_zone_id}):
        raise EntityNotFoundById(functional_zone_id, "scenario functional zone")

    if functional_zone.functional_zone_type_id is not None:
        if not await check_existence(
            conn,
            functional_zone_types_dict,
            conditions={"functional_zone_type_id": functional_zone.functional_zone_type_id},
        ):
            raise EntityNotFoundById(functional_zone.functional_zone_type_id, "functional zone type")

    values = extract_values_from_model(functional_zone, exclude_unset=True, to_update=True)

    statement = (
        update(projects_functional_zones)
        .where(projects_functional_zones.c.profile_id == functional_zone_id)
        .values(**values)
    )

    await conn.execute(statement)
    await conn.commit()

    return (await get_functional_zone_by_ids(conn, [functional_zone_id]))[0]


async def delete_functional_zones_by_scenario_id_from_db(conn: AsyncConnection, scenario_id: int, user_id: str) -> dict:
    """Delete functional zones by scenario identifier."""

    await check_scenario(conn, scenario_id, user_id, to_edit=True)

    statement = delete(projects_functional_zones).where(projects_functional_zones.c.scenario_id == scenario_id)
    await conn.execute(statement)
    await conn.commit()

    return {"status": "ok"}
