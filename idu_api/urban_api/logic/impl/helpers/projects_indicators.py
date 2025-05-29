"""Projects indicators values internal logic is defined here."""

import os
from typing import Any

import aiohttp
import structlog
from geoalchemy2.functions import ST_AsEWKB
from otteroad import KafkaProducerClient
from otteroad.models import RegionalScenarioIndicatorsUpdated, ScenarioIndicatorsUpdated
from sqlalchemy import delete, insert, select, update
from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.common.db.entities import (
    hexagons_data,
    indicators_dict,
    indicators_groups_data,
    measurement_units_dict,
    projects_indicators_data,
    scenarios_data,
    territories_data,
)
from idu_api.urban_api.config import UrbanAPIConfig
from idu_api.urban_api.dto import (
    HexagonWithIndicatorsDTO,
    ScenarioIndicatorValueDTO,
    ShortScenarioIndicatorValueDTO,
    UserDTO,
)
from idu_api.urban_api.exceptions.logic.common import EntityAlreadyExists, EntityNotFoundById
from idu_api.urban_api.exceptions.logic.projects import NotAllowedInProjectScenario
from idu_api.urban_api.logic.impl.helpers.projects_scenarios import check_scenario, get_project_by_scenario_id
from idu_api.urban_api.logic.impl.helpers.utils import check_existence, extract_values_from_model
from idu_api.urban_api.schemas import ScenarioIndicatorValuePatch, ScenarioIndicatorValuePost, ScenarioIndicatorValuePut


async def get_scenario_indicator_value_by_id_from_db(
    conn: AsyncConnection, indicator_value_id: int
) -> ScenarioIndicatorValueDTO:
    """Get scenario's indicator value by given indicator value identifier
    if relevant project is public or if you're the project owner."""

    statement = (
        select(
            projects_indicators_data,
            indicators_dict.c.parent_id,
            indicators_dict.c.name_full,
            indicators_dict.c.measurement_unit_id,
            measurement_units_dict.c.name.label("measurement_unit_name"),
            indicators_dict.c.level,
            indicators_dict.c.list_label,
            scenarios_data.c.name.label("scenario_name"),
            territories_data.c.name.label("territory_name"),
        )
        .select_from(
            projects_indicators_data.join(
                scenarios_data, scenarios_data.c.scenario_id == projects_indicators_data.c.scenario_id
            )
            .join(indicators_dict, indicators_dict.c.indicator_id == projects_indicators_data.c.indicator_id)
            .outerjoin(
                measurement_units_dict,
                measurement_units_dict.c.measurement_unit_id == indicators_dict.c.measurement_unit_id,
            )
            .outerjoin(territories_data, territories_data.c.territory_id == projects_indicators_data.c.territory_id)
        )
        .where(projects_indicators_data.c.indicator_value_id == indicator_value_id)
    )
    result = (await conn.execute(statement)).mappings().one_or_none()

    if result is None:
        raise EntityNotFoundById(indicator_value_id, "indicator value")

    return ScenarioIndicatorValueDTO(**result)


async def get_scenario_indicators_values_by_scenario_id_from_db(
    conn: AsyncConnection,
    scenario_id: int,
    indicator_ids: str | None,
    indicators_group_id: int | None,
    territory_id: int | None,
    hexagon_id: int | None,
    user: UserDTO | None,
) -> list[ScenarioIndicatorValueDTO]:
    """Get scenario's indicators values for given scenario
    if relevant project is public or if you're the project owner."""

    await check_scenario(conn, scenario_id, user)

    statement = (
        select(
            projects_indicators_data,
            indicators_dict.c.parent_id,
            indicators_dict.c.name_full,
            indicators_dict.c.measurement_unit_id,
            measurement_units_dict.c.name.label("measurement_unit_name"),
            indicators_dict.c.level,
            indicators_dict.c.list_label,
            scenarios_data.c.name.label("scenario_name"),
            territories_data.c.name.label("territory_name"),
        )
        .select_from(
            projects_indicators_data.join(
                scenarios_data, scenarios_data.c.scenario_id == projects_indicators_data.c.scenario_id
            )
            .join(indicators_dict, indicators_dict.c.indicator_id == projects_indicators_data.c.indicator_id)
            .outerjoin(
                measurement_units_dict,
                measurement_units_dict.c.measurement_unit_id == indicators_dict.c.measurement_unit_id,
            )
            .outerjoin(territories_data, territories_data.c.territory_id == projects_indicators_data.c.territory_id)
            .outerjoin(
                indicators_groups_data,
                indicators_groups_data.c.indicator_id == indicators_dict.c.indicator_id,
            )
        )
        .where(projects_indicators_data.c.scenario_id == scenario_id)
        .order_by(projects_indicators_data.c.indicator_value_id)
        .distinct()
    )

    if indicators_group_id is not None:
        statement = statement.where(indicators_groups_data.c.indicators_group_id == indicators_group_id)
    if indicator_ids is not None:
        ids = {int(indicator.strip()) for indicator in indicator_ids.split(",")}
        statement = statement.where(projects_indicators_data.c.indicator_id.in_(ids))
    if territory_id is not None:
        statement = statement.where(projects_indicators_data.c.territory_id == territory_id)
    if hexagon_id is not None:
        statement = statement.where(projects_indicators_data.c.hexagon_id == hexagon_id)

    results = (await conn.execute(statement)).mappings().all()

    return [ScenarioIndicatorValueDTO(**result) for result in results]


async def add_scenario_indicator_value_to_db(
    conn: AsyncConnection,
    indicator_value: ScenarioIndicatorValuePost,
    scenario_id: int,
    user: UserDTO,
    kafka_producer: KafkaProducerClient,
) -> ScenarioIndicatorValueDTO:
    """Add a new scenario's indicator value."""

    project = await get_project_by_scenario_id(conn, scenario_id, user, to_edit=True)

    if not await check_existence(conn, indicators_dict, conditions={"indicator_id": indicator_value.indicator_id}):
        raise EntityNotFoundById(indicator_value.indicator_id, "indicator")

    if indicator_value.territory_id is not None:
        if not await check_existence(conn, territories_data, conditions={"territory_id": indicator_value.territory_id}):
            raise EntityNotFoundById(indicator_value.territory_id, "territory")

    if indicator_value.hexagon_id is not None:
        if not await check_existence(conn, hexagons_data, conditions={"hexagon_id": indicator_value.hexagon_id}):
            raise EntityNotFoundById(indicator_value.hexagon_id, "hexagon")

    if await check_existence(
        conn,
        projects_indicators_data,
        conditions={
            "indicator_id": indicator_value.indicator_id,
            "scenario_id": indicator_value.scenario_id,
            "territory_id": indicator_value.territory_id,
            "hexagon_id": indicator_value.hexagon_id,
        },
    ):
        raise EntityAlreadyExists(
            "project indicator value",
            indicator_value.scenario_id,
            indicator_value.indicator_id,
            indicator_value.territory_id,
            indicator_value.hexagon_id,
        )

    statement = (
        insert(projects_indicators_data)
        .values(**indicator_value.model_dump())
        .returning(projects_indicators_data.c.indicator_value_id)
    )
    indicator_value_id = (await conn.execute(statement)).scalar_one()

    new_value = await get_scenario_indicator_value_by_id_from_db(conn, indicator_value_id)

    await conn.commit()

    if project.is_regional and indicator_value.hexagon_id is None:
        event = RegionalScenarioIndicatorsUpdated(
            scenario_id=scenario_id,
            territory_id=project.territory_id,
            indicator_id=indicator_value.indicator_id,
            indicator_value_id=indicator_value_id,
        )
        await kafka_producer.send(event)
    elif indicator_value.hexagon_id is None:
        event = ScenarioIndicatorsUpdated(
            project_id=project.project_id,
            scenario_id=scenario_id,
            indicator_id=indicator_value.indicator_id,
            indicator_value_id=indicator_value_id,
        )
        await kafka_producer.send(event)

    return new_value


async def put_scenario_indicator_value_to_db(
    conn: AsyncConnection,
    indicator_value: ScenarioIndicatorValuePut,
    scenario_id: int,
    user: UserDTO,
    kafka_producer: KafkaProducerClient,
) -> ScenarioIndicatorValueDTO:
    """Update scenario's indicator value by all attributes."""

    project = await get_project_by_scenario_id(conn, scenario_id, user, to_edit=True)

    if not await check_existence(conn, indicators_dict, conditions={"indicator_id": indicator_value.indicator_id}):
        raise EntityNotFoundById(indicator_value.indicator_id, "indicator")

    if indicator_value.territory_id is not None:
        if not await check_existence(conn, territories_data, conditions={"territory_id": indicator_value.territory_id}):
            raise EntityNotFoundById(indicator_value.territory_id, "territory")

    if indicator_value.hexagon_id is not None:
        if not await check_existence(conn, hexagons_data, conditions={"hexagon_id": indicator_value.hexagon_id}):
            raise EntityNotFoundById(indicator_value.hexagon_id, "hexagon")

    if await check_existence(
        conn,
        projects_indicators_data,
        conditions={
            "indicator_id": indicator_value.indicator_id,
            "scenario_id": indicator_value.scenario_id,
            "territory_id": indicator_value.territory_id,
            "hexagon_id": indicator_value.hexagon_id,
        },
    ):
        statement = (
            update(projects_indicators_data)
            .where(
                projects_indicators_data.c.indicator_id == indicator_value.indicator_id,
                projects_indicators_data.c.scenario_id == scenario_id,
                (
                    projects_indicators_data.c.territory_id == indicator_value.territory_id
                    if indicator_value.territory_id is not None
                    else projects_indicators_data.c.territory_id.is_(None)
                ),
                (
                    projects_indicators_data.c.hexagon_id == indicator_value.hexagon_id
                    if indicator_value.hexagon_id is not None
                    else projects_indicators_data.c.hexagon_id.is_(None)
                ),
            )
            .values(**extract_values_from_model(indicator_value, to_update=True))
            .returning(projects_indicators_data.c.indicator_value_id)
        )
        indicator_value_id = (await conn.execute(statement)).scalar_one()
    else:
        statement = (
            insert(projects_indicators_data)
            .values(**extract_values_from_model(indicator_value))
            .returning(projects_indicators_data.c.indicator_value_id)
        )
        indicator_value_id = (await conn.execute(statement)).scalar_one()

    new_value = await get_scenario_indicator_value_by_id_from_db(conn, indicator_value_id)

    await conn.commit()

    if project.is_regional and indicator_value.hexagon_id is None:
        event = RegionalScenarioIndicatorsUpdated(
            scenario_id=scenario_id,
            territory_id=project.territory_id,
            indicator_id=indicator_value.indicator_id,
            indicator_value_id=indicator_value_id,
        )
        await kafka_producer.send(event)
    elif indicator_value.hexagon_id is None:
        event = ScenarioIndicatorsUpdated(
            project_id=project.project_id,
            scenario_id=scenario_id,
            indicator_id=indicator_value.indicator_id,
            indicator_value_id=indicator_value_id,
        )
        await kafka_producer.send(event)

    return new_value


async def patch_scenario_indicator_value_to_db(
    conn: AsyncConnection,
    indicator_value: ScenarioIndicatorValuePatch,
    scenario_id: int | None,
    indicator_value_id: int,
    user: UserDTO,
    kafka_producer: KafkaProducerClient,
) -> ScenarioIndicatorValueDTO:
    """Update scenario's indicator value by only given attributes."""

    if scenario_id is None:
        statement = select(projects_indicators_data.c.scenario_id).where(
            projects_indicators_data.c.indicator_value_id == indicator_value_id
        )
        scenario_id = (await conn.execute(statement)).scalar_one_or_none()
        if scenario_id is None:
            raise EntityNotFoundById(indicator_value_id, "indicator value")

    project = await get_project_by_scenario_id(conn, scenario_id, user, to_edit=True)

    if not await check_existence(conn, projects_indicators_data, conditions={"indicator_value_id": indicator_value_id}):
        raise EntityNotFoundById(indicator_value_id, "indicator value")

    values = extract_values_from_model(indicator_value, exclude_unset=True, to_update=True)
    statement = (
        update(projects_indicators_data)
        .where(projects_indicators_data.c.indicator_value_id == indicator_value_id)
        .values(**values)
        .returning(projects_indicators_data)
    )
    updated_indicator = (await conn.execute(statement)).mappings().one()

    new_value = await get_scenario_indicator_value_by_id_from_db(conn, indicator_value_id)

    await conn.commit()

    if project.is_regional and updated_indicator.hexagon_id is None:
        event = RegionalScenarioIndicatorsUpdated(
            scenario_id=scenario_id,
            territory_id=project.territory_id,
            indicator_id=updated_indicator.indicator_id,
            indicator_value_id=indicator_value_id,
        )
        await kafka_producer.send(event)
    elif updated_indicator.hexagon_id is None:
        event = ScenarioIndicatorsUpdated(
            project_id=project.project_id,
            scenario_id=scenario_id,
            indicator_id=updated_indicator.indicator_id,
            indicator_value_id=indicator_value_id,
        )
        await kafka_producer.send(event)

    return new_value


async def delete_scenario_indicators_values_by_scenario_id_from_db(
    conn: AsyncConnection, scenario_id: int, user: UserDTO
) -> dict:
    """Delete all scenario's indicators values for given scenario if you're the project owner."""

    await check_scenario(conn, scenario_id, user, to_edit=True)

    statement = delete(projects_indicators_data).where(projects_indicators_data.c.scenario_id == scenario_id)

    await conn.execute(statement)
    await conn.commit()

    return {"status": "ok"}


async def delete_scenario_indicator_value_by_id_from_db(
    conn: AsyncConnection, scenario_id: int | None, indicator_value_id: int, user: UserDTO
) -> dict:
    """Delete specific scenario's indicator values by indicator value identifier if you're the project owner."""

    if scenario_id is None:
        statement = select(projects_indicators_data.c.scenario_id).where(
            projects_indicators_data.c.indicator_value_id == indicator_value_id
        )
        scenario_id = (await conn.execute(statement)).scalar_one_or_none()
        if scenario_id is None:
            raise EntityNotFoundById(indicator_value_id, "indicator value")

    await check_scenario(conn, scenario_id, user, to_edit=True)

    if not await check_existence(conn, projects_indicators_data, conditions={"indicator_value_id": indicator_value_id}):
        raise EntityNotFoundById(indicator_value_id, "indicator value")

    statement = delete(projects_indicators_data).where(
        projects_indicators_data.c.indicator_value_id == indicator_value_id
    )

    await conn.execute(statement)
    await conn.commit()

    return {"status": "ok"}


async def get_hexagons_with_indicators_by_scenario_id_from_db(
    conn: AsyncConnection,
    scenario_id: int,
    indicator_ids: str | None,
    indicators_group_id: int | None,
    user: UserDTO | None,
) -> list[HexagonWithIndicatorsDTO]:
    """Get scenario's indicators values for given regional scenario with hexagons."""

    project = await get_project_by_scenario_id(conn, scenario_id, user)
    if not project.is_regional:
        raise NotAllowedInProjectScenario()

    statement = (
        select(
            projects_indicators_data.c.value,
            projects_indicators_data.c.comment,
            indicators_dict.c.indicator_id,
            indicators_dict.c.name_full,
            measurement_units_dict.c.name.label("measurement_unit_name"),
            hexagons_data.c.hexagon_id,
            ST_AsEWKB(hexagons_data.c.geometry).label("geometry"),
            ST_AsEWKB(hexagons_data.c.centre_point).label("centre_point"),
        )
        .select_from(
            projects_indicators_data.join(
                hexagons_data,
                hexagons_data.c.hexagon_id == projects_indicators_data.c.hexagon_id,
            )
            .join(indicators_dict, indicators_dict.c.indicator_id == projects_indicators_data.c.indicator_id)
            .outerjoin(
                measurement_units_dict,
                measurement_units_dict.c.measurement_unit_id == indicators_dict.c.measurement_unit_id,
            )
            .outerjoin(
                indicators_groups_data,
                indicators_groups_data.c.indicator_id == indicators_dict.c.indicator_id,
            )
        )
        .where(projects_indicators_data.c.scenario_id == scenario_id)
        .order_by(projects_indicators_data.c.indicator_id.asc())
    )

    if indicators_group_id is not None:
        statement = statement.where(indicators_groups_data.c.indicators_group_id == indicators_group_id)
    if indicator_ids is not None:
        ids = {int(indicator.strip()) for indicator in indicator_ids.split(",")}
        statement = statement.where(projects_indicators_data.c.indicator_id.in_(ids))
    indicators = (await conn.execute(statement)).mappings().all()

    grouped_data = {}
    for row in indicators:
        hexagon_id = row["hexagon_id"]
        if hexagon_id not in grouped_data:
            grouped_data[hexagon_id] = {
                "hexagon_id": hexagon_id,
                "geometry": row["geometry"],
                "centre_point": row["centre_point"],
                "indicators": [],
            }
        grouped_data[hexagon_id]["indicators"].append(
            ShortScenarioIndicatorValueDTO(
                indicator_id=row["indicator_id"],
                name_full=row["name_full"],
                measurement_unit_name=row["measurement_unit_name"],
                value=row["value"],
                comment=row["comment"],
            )
        )

    return [HexagonWithIndicatorsDTO(**result) for result in list(grouped_data.values())]


async def update_all_indicators_values_by_scenario_id_to_db(
    conn: AsyncConnection, scenario_id: int, user: UserDTO, logger: structlog.stdlib.BoundLogger
) -> dict[str, Any]:
    """Update all indicators values for given scenario."""

    project = await get_project_by_scenario_id(conn, scenario_id, user)

    config = UrbanAPIConfig.from_file_or_default(os.getenv("CONFIG_PATH"))

    async with aiohttp.ClientSession() as session:
        params = {"scenario_id": scenario_id, "project_id": project.project_id, "background": "false"}
        try:
            response = await session.put(
                f"{config.external.hextech_api}/hextech/indicators_saving/save_all",
                params=params,
            )
            response.raise_for_status()
        except aiohttp.ClientResponseError as exc:
            await logger.aerror(
                "failed to save indicators",
                status=exc.status,
                message=exc.message,
                url=exc.request_info.url,
                params=params,
            )
            raise
        except aiohttp.ClientConnectorError as exc:
            await logger.aerror("request failed", error=str(exc), params=params)
            raise
        except Exception:
            await logger.aexception("unexpected error occurred")
            raise

    return {"status": "ok"}
