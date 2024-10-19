"""Projects indicators internal logic is defined here."""

from sqlalchemy import and_, delete, insert, select
from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.common.db.entities import (
    projects_data,
    projects_indicators_data,
    scenarios_data,
)
from idu_api.urban_api.dto import ProjectsIndicatorDTO
from idu_api.urban_api.exceptions.logic.common import EntityNotFoundById
from idu_api.urban_api.exceptions.logic.users import AccessDeniedError
from idu_api.urban_api.schemas import ProjectsIndicatorPost


async def get_all_projects_indicators_values_from_db(
    conn: AsyncConnection, scenario_id: int, user_id: str
) -> list[ProjectsIndicatorDTO]:
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

    statement = select(projects_indicators_data).where(projects_indicators_data.c.scenario_id == scenario_id)
    results = (await conn.execute(statement)).mappings().all()

    return [ProjectsIndicatorDTO(**result) for result in results]


async def get_specific_projects_indicator_values_from_db(
    conn: AsyncConnection, scenario_id: int, indicator_id: int, user_id: str
) -> list[ProjectsIndicatorDTO]:
    """Get project's specific indicator values for given scenario
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

    statement = select(projects_indicators_data).where(
        and_(
            projects_indicators_data.c.scenario_id == scenario_id,
            projects_indicators_data.c.indicator_id == indicator_id,
        )
    )
    result = (await conn.execute(statement)).mappings().all()
    if result is None:
        raise EntityNotFoundById(indicator_id, "indicator")

    return [ProjectsIndicatorDTO(**indicator) for indicator in result]


async def add_projects_indicator_value_to_db(
    conn: AsyncConnection, projects_indicator: ProjectsIndicatorPost, user_id: str
) -> ProjectsIndicatorDTO:
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

    statement = insert(projects_indicators_data).values(
        scenario_id=projects_indicator.scenario_id,
        indicator_id=projects_indicator.indicator_id,
        date_type=projects_indicator.date_type,
        date_value=projects_indicator.date_value,
        value=projects_indicator.value,
        value_type=projects_indicator.value_type,
        information_source=projects_indicator.information_source,
    )

    await conn.execute(statement)

    statement = select(projects_indicators_data).where(
        and_(
            projects_indicators_data.c.scenario_id == projects_indicator.scenario_id,
            projects_indicators_data.c.indicator_id == projects_indicator.indicator_id,
            projects_indicators_data.c.date_type == projects_indicator.date_type,
            projects_indicators_data.c.date_value == projects_indicator.date_value,
            projects_indicators_data.c.value_type == projects_indicator.value_type,
            projects_indicators_data.c.information_source == projects_indicator.information_source,
        )
    )
    result = (await conn.execute(statement)).mappings().one_or_none()

    await conn.commit()

    return ProjectsIndicatorDTO(**result)


async def delete_all_projects_indicators_values_from_db(conn: AsyncConnection, scenario_id: int, user_id: str) -> dict:
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


async def delete_specific_projects_indicator_values_from_db(
    conn: AsyncConnection, scenario_id: int, indicator_id: int, user_id: str
) -> dict:
    """Delete specific project's indicator values for given scenario if you're the project owner."""

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
        and_(
            projects_indicators_data.c.scenario_id == scenario_id,
            projects_indicators_data.c.indicator_id == indicator_id,
        )
    )

    await conn.execute(statement)
    await conn.commit()

    return {"status": "ok"}
