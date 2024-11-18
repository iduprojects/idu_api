"""Projects indicators values internal logic is defined here."""

from datetime import datetime, timezone

from sqlalchemy import delete, insert, select, update
from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.common.db.entities import (
    hexagons_data,
    indicators_dict,
    indicators_groups_data,
    indicators_groups_dict,
    measurement_units_dict,
    projects_data,
    projects_indicators_data,
    scenarios_data,
    territories_data,
)
from idu_api.urban_api.dto import ProjectsIndicatorValueDTO
from idu_api.urban_api.exceptions.logic.common import EntitiesNotFoundByIds, EntityAlreadyExists, EntityNotFoundById
from idu_api.urban_api.exceptions.logic.users import AccessDeniedError
from idu_api.urban_api.schemas import ProjectsIndicatorValuePatch, ProjectsIndicatorValuePost, ProjectsIndicatorValuePut


async def get_project_indicator_value_by_id_from_db(
    conn: AsyncConnection, indicator_value_id: int, user_id: str
) -> ProjectsIndicatorValueDTO:
    """Get project's indicator value by given indicator value identifier
    if relevant project is public or if you're the project owner."""

    statement = (
        select(
            projects_indicators_data,
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

    statement = select(scenarios_data.c.project_id).where(scenarios_data.c.scenario_id == result.scenario_id)
    project_id = (await conn.execute(statement)).scalar_one_or_none()
    if project_id is None:
        raise EntityNotFoundById(result.scenario_id, "scenario")

    statement = select(projects_data).where(projects_data.c.project_id == project_id)
    project = (await conn.execute(statement)).mappings().one_or_none()
    if project is None:
        raise EntityNotFoundById(project_id, "project")
    if project.user_id != user_id and project.public is False:
        raise AccessDeniedError(project_id, "project")

    return ProjectsIndicatorValueDTO(**result)


async def get_projects_indicators_values_by_scenario_id_from_db(
    conn: AsyncConnection,
    scenario_id: int,
    indicator_ids: str | None,
    indicators_group_id: int | None,
    territory_id: int | None,
    hexagon_id: int | None,
    user_id: str,
) -> list[ProjectsIndicatorValueDTO]:
    """Get project's indicators values for given scenario
    if relevant project is public or if you're the project owner."""

    statement = select(scenarios_data.c.project_id).where(scenarios_data.c.scenario_id == scenario_id)
    project_id = (await conn.execute(statement)).scalar_one_or_none()
    if project_id is None:
        raise EntityNotFoundById(scenario_id, "scenario")

    statement = select(projects_data).where(projects_data.c.project_id == project_id)
    project = (await conn.execute(statement)).mappings().one_or_none()
    if project is None:
        raise EntityNotFoundById(project_id, "project")
    if project.user_id != user_id and project.public is False:
        raise AccessDeniedError(project_id, "project")

    statement = (
        select(
            projects_indicators_data,
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
        .order_by(projects_indicators_data.c.indicator_id.asc())
    )

    if indicators_group_id is not None:
        query = select(indicators_groups_dict).where(
            indicators_groups_dict.c.indicators_group_id == indicators_group_id
        )
        indicators_group = (await conn.execute(query)).scalar_one_or_none()
        if indicators_group is None:
            raise EntityNotFoundById(indicators_group_id, "indicators group")
        statement = statement.where(indicators_groups_data.c.indicators_group_id == indicators_group_id)
    if indicator_ids is not None:
        ids = [int(indicator.strip()) for indicator in indicator_ids.split(",")]
        query = select(indicators_dict.c.indicator_id).where(indicators_dict.c.indicator_id.in_(ids))
        indicators = (await conn.execute(query)).scalars()
        if len(ids) > len(list(indicators)):
            raise EntitiesNotFoundByIds("indicator")
        statement = statement.where(projects_indicators_data.c.indicator_id.in_(ids))
    if territory_id is not None:
        statement = statement.where(projects_indicators_data.c.territory_id == territory_id)
    if hexagon_id is not None:
        statement = statement.where(projects_indicators_data.c.hexagon_id == hexagon_id)

    results = (await conn.execute(statement)).mappings().all()

    return [ProjectsIndicatorValueDTO(**result) for result in results]


async def add_project_indicator_value_to_db(
    conn: AsyncConnection, projects_indicator: ProjectsIndicatorValuePost, user_id: str
) -> ProjectsIndicatorValueDTO:
    """Add a new project's indicator value."""

    statement = select(scenarios_data.c.project_id).where(
        scenarios_data.c.scenario_id == projects_indicator.scenario_id
    )
    project_id = (await conn.execute(statement)).scalar_one_or_none()
    if project_id is None:
        raise EntityNotFoundById(projects_indicator.scenario_id, "scenario")

    statement = select(projects_data).where(projects_data.c.project_id == project_id)
    project = (await conn.execute(statement)).mappings().one_or_none()
    if project is None:
        raise EntityNotFoundById(project_id, "project")
    if project.user_id != user_id:
        raise AccessDeniedError(project_id, "project")

    statement = select(indicators_dict).where(indicators_dict.c.indicator_id == projects_indicator.indicator_id)
    indicator = (await conn.execute(statement)).mappings().one_or_none()
    if indicator is None:
        raise EntityNotFoundById(projects_indicator.indicator_id, "indicator")

    if projects_indicator.territory_id is not None:
        statement = select(territories_data).where(territories_data.c.territory_id == projects_indicator.territory_id)
        territory = (await conn.execute(statement)).mappings().one_or_none()
        if territory is None:
            raise EntityNotFoundById(projects_indicator.territory_id, "territory")

    if projects_indicator.hexagon_id is not None:
        statement = select(hexagons_data).where(hexagons_data.c.hexagon_id == projects_indicator.hexagon_id)
        hexagon = (await conn.execute(statement)).mappings().one_or_none()
        if hexagon is None:
            raise EntityNotFoundById(projects_indicator.hexagon_id, "hexagon")

    statement = (
        insert(projects_indicators_data)
        .values(
            scenario_id=projects_indicator.scenario_id,
            indicator_id=projects_indicator.indicator_id,
            territory_id=projects_indicator.territory_id,
            hexagon_id=projects_indicator.hexagon_id,
            value=projects_indicator.value,
            comment=projects_indicator.comment,
            information_source=projects_indicator.information_source,
        )
        .returning(projects_indicators_data.c.indicator_value_id)
    )

    indicator_value_id = (await conn.execute(statement)).scalar()

    await conn.commit()

    return await get_project_indicator_value_by_id_from_db(conn, indicator_value_id, user_id)


async def put_project_indicator_value_to_db(
    conn: AsyncConnection, projects_indicator: ProjectsIndicatorValuePut, indicator_value_id: int, user_id: str
) -> ProjectsIndicatorValueDTO:
    """Put project's indicator value."""

    statement = select(projects_indicators_data.c.scenario_id).where(
        projects_indicators_data.c.indicator_value_id == indicator_value_id
    )
    scenario_id = (await conn.execute(statement)).scalar_one_or_none()
    if scenario_id is None:
        raise EntityNotFoundById(indicator_value_id, "indicator value")

    statement = select(scenarios_data.c.project_id).where(scenarios_data.c.scenario_id == scenario_id)
    project_id = (await conn.execute(statement)).scalar_one_or_none()
    if project_id is None:
        raise EntityNotFoundById(scenario_id, "scenario")

    statement = select(projects_data).where(projects_data.c.project_id == project_id)
    project = (await conn.execute(statement)).mappings().one_or_none()
    if project is None:
        raise EntityNotFoundById(project_id, "project")
    if project.user_id != user_id:
        raise AccessDeniedError(project_id, "project")

    statement = (
        update(projects_indicators_data)
        .where(projects_indicators_data.c.indicator_value_id == indicator_value_id)
        .values(
            value=projects_indicator.value,
            comment=projects_indicator.comment,
            information_source=projects_indicator.information_source,
            updated_at=datetime.now(timezone.utc),
        )
        .returning(projects_indicators_data.c.indicator_value_id)
    )
    indicator_value_id = (await conn.execute(statement)).scalar_one()

    await conn.commit()

    return await get_project_indicator_value_by_id_from_db(conn, indicator_value_id, user_id)


async def patch_project_indicator_value_to_db(
    conn: AsyncConnection, projects_indicator: ProjectsIndicatorValuePatch, indicator_value_id: int, user_id: str
) -> ProjectsIndicatorValueDTO:
    """Patch project's indicator value."""

    statement = select(projects_indicators_data.c.scenario_id).where(
        projects_indicators_data.c.indicator_value_id == indicator_value_id
    )
    scenario_id = (await conn.execute(statement)).scalar_one_or_none()
    if scenario_id is None:
        raise EntityNotFoundById(indicator_value_id, "indicator value")

    statement = select(scenarios_data.c.project_id).where(scenarios_data.c.scenario_id == scenario_id)
    project_id = (await conn.execute(statement)).scalar_one_or_none()
    if project_id is None:
        raise EntityNotFoundById(scenario_id, "scenario")

    statement = select(projects_data).where(projects_data.c.project_id == project_id)
    project = (await conn.execute(statement)).mappings().one_or_none()
    if project is None:
        raise EntityNotFoundById(project_id, "project")
    if project.user_id != user_id:
        raise AccessDeniedError(project_id, "project")

    statement = (
        update(projects_indicators_data)
        .where(projects_indicators_data.c.indicator_value_id == indicator_value_id)
        .values(updated_at=datetime.now(timezone.utc))
        .returning(projects_indicators_data.c.indicator_value_id)
    )

    values_to_update = {}
    for k, v in projects_indicator.model_dump(exclude_unset=True).items():
        values_to_update.update({k: v})

    statement = statement.values(**values_to_update)

    indicator_value_id = (await conn.execute(statement)).scalar_one()

    await conn.commit()

    return await get_project_indicator_value_by_id_from_db(conn, indicator_value_id, user_id)


async def delete_projects_indicators_values_by_scenario_id_from_db(
    conn: AsyncConnection, scenario_id: int, user_id: str
) -> dict:
    """Delete all project's indicators values for given scenario if you're the project owner."""

    statement = select(scenarios_data.c.project_id).where(scenarios_data.c.scenario_id == scenario_id)
    project_id = (await conn.execute(statement)).scalar_one_or_none()
    if project_id is None:
        raise EntityNotFoundById(scenario_id, "scenario")

    statement = select(projects_data).where(projects_data.c.project_id == project_id)
    project = (await conn.execute(statement)).mappings().one_or_none()
    if project is None:
        raise EntityNotFoundById(project_id, "project")
    if project.user_id != user_id:
        raise AccessDeniedError(project_id, "project")

    statement = delete(projects_indicators_data).where(projects_indicators_data.c.scenario_id == scenario_id)

    await conn.execute(statement)
    await conn.commit()

    return {"status": "ok"}


async def delete_project_indicator_value_by_id_from_db(
    conn: AsyncConnection, indicator_value_id: int, user_id: str
) -> dict:
    """Delete specific project's indicator values by indicator value identifier if you're the project owner."""

    statement = select(projects_indicators_data.c.scenario_id).where(
        projects_indicators_data.c.indicator_value_id == indicator_value_id
    )
    scenario_id = (await conn.execute(statement)).scalar_one_or_none()
    if scenario_id is None:
        raise EntityNotFoundById(indicator_value_id, "indicator value")

    statement = select(scenarios_data.c.project_id).where(scenarios_data.c.scenario_id == scenario_id)
    project_id = (await conn.execute(statement)).scalar_one_or_none()
    if project_id is None:
        raise EntityNotFoundById(scenario_id, "scenario")

    statement = select(projects_data).where(projects_data.c.project_id == project_id)
    project = (await conn.execute(statement)).mappings().one_or_none()
    if project is None:
        raise EntityNotFoundById(project_id, "project")
    if project.user_id != user_id:
        raise AccessDeniedError(project_id, "project")

    statement = delete(projects_indicators_data).where(
        projects_indicators_data.c.indicator_value_id == indicator_value_id
    )

    await conn.execute(statement)
    await conn.commit()

    return {"status": "ok"}
