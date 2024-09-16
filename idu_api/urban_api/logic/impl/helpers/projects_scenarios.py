"""Projects scenarios internal logic is defined here."""

from sqlalchemy import delete, insert, select, update
from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.common.db.entities import projects_data, scenarios_data, target_profiles_dict
from idu_api.urban_api.dto import ScenarioDTO
from idu_api.urban_api.exceptions.logic.common import EntityNotFoundById
from idu_api.urban_api.exceptions.logic.users import AccessDeniedError
from idu_api.urban_api.schemas import ScenariosPatch, ScenariosPost, ScenariosPut


async def get_scenarios_by_project_id_from_db(
    conn: AsyncConnection, project_id: int, user_id: str
) -> list[ScenarioDTO]:
    """Get list of scenario objects by project id."""

    statement = select(projects_data).where(projects_data.c.project_id == project_id)
    result = (await conn.execute(statement)).mappings().one_or_none()
    if result is None:
        raise EntityNotFoundById(project_id, "project")
    if result.user_id != user_id and result.public is False:
        raise AccessDeniedError(project_id, "project")

    statement = (
        select(
            scenarios_data,
            target_profiles_dict.c.name.label("target_profile_name"),
        )
        .select_from(
            scenarios_data.outerjoin(
                target_profiles_dict,
                target_profiles_dict.c.target_profile_id == scenarios_data.c.target_profile_id,
            )
        )
        .where(scenarios_data.c.project_id == project_id)
    )
    result = (await conn.execute(statement)).mappings().all()

    return [ScenarioDTO(**scenario) for scenario in result]


async def get_scenario_by_id_from_db(conn: AsyncConnection, scenario_id: int, user_id: str) -> ScenarioDTO:
    """Get scenario object by id."""

    statement = (
        select(
            scenarios_data,
            target_profiles_dict.c.name.label("target_profile_name"),
        )
        .select_from(
            scenarios_data.outerjoin(
                target_profiles_dict,
                target_profiles_dict.c.target_profile_id == scenarios_data.c.target_profile_id,
            )
        )
        .where(scenarios_data.c.scenario_id == scenario_id)
    )
    result = (await conn.execute(statement)).mappings().one_or_none()
    if result is None:
        raise EntityNotFoundById(scenario_id, "scenario")

    statement = select(projects_data).where(projects_data.c.project_id == result.project_id)
    project = (await conn.execute(statement)).mappings().one_or_none()
    if project.user_id != user_id and project.public is False:
        raise AccessDeniedError(result.project_id, "project")

    return ScenarioDTO(**result)


async def add_scenario_to_db(conn: AsyncConnection, scenario: ScenariosPost, user_id: str) -> ScenarioDTO:
    """Create a new scenario."""

    statement = select(projects_data).where(projects_data.c.project_id == scenario.project_id)
    result = (await conn.execute(statement)).mappings().one_or_none()
    if result is None:
        raise EntityNotFoundById(scenario.project_id, "project")
    if result.user_id != user_id:
        raise AccessDeniedError(scenario.project_id, "project")

    statement = select(target_profiles_dict).where(
        target_profiles_dict.c.target_profile_id == scenario.target_profile_id
    )
    profile = (await conn.execute(statement)).mappings().one_or_none()
    if profile is None:
        raise EntityNotFoundById(scenario.target_profile_id, "target profile")

    statement = (
        insert(scenarios_data)
        .values(
            project_id=scenario.project_id,
            target_profile_id=scenario.target_profile_id,
            name=scenario.name,
            properties=scenario.properties,
        )
        .returning(scenarios_data.c.scenario_id)
    )
    scenario_id = (await conn.execute(statement)).scalar_one()

    await conn.commit()

    statement = (
        select(
            scenarios_data,
            target_profiles_dict.c.name.label("target_profile_name"),
        )
        .select_from(
            scenarios_data.outerjoin(
                target_profiles_dict,
                target_profiles_dict.c.target_profile_id == scenarios_data.c.target_profile_id,
            )
        )
        .where(scenarios_data.c.scenario_id == scenario_id)
    )
    result = (await conn.execute(statement)).mappings().one()

    return ScenarioDTO(**result)


async def put_scenario_to_db(
    conn: AsyncConnection, scenario: ScenariosPut, scenario_id: int, user_id: str
) -> ScenarioDTO:
    """Put scenario object."""

    statement = select(scenarios_data).where(scenarios_data.c.scenario_id == scenario_id)
    result = (await conn.execute(statement)).mappings().one_or_none()
    if result is None:
        raise EntityNotFoundById(scenario_id, "scenario")

    statement = select(projects_data).where(projects_data.c.project_id == result.project_id)
    project = (await conn.execute(statement)).mappings().one_or_none()
    if project.user_id != user_id:
        raise AccessDeniedError(result.project_id, "project")

    statement = select(target_profiles_dict).where(
        target_profiles_dict.c.target_profile_id == scenario.target_profile_id
    )
    profile = (await conn.execute(statement)).mappings().one_or_none()
    if profile is None:
        raise EntityNotFoundById(scenario.target_profile_id, "target profile")

    statement = (
        update(scenarios_data)
        .where(scenarios_data.c.scenario_id == scenario_id)
        .values(target_profile_id=scenario.target_profile_id, name=scenario.name, properties=scenario.properties)
        .returning(scenarios_data)
    )
    await conn.execute(statement)
    await conn.commit()

    statement = (
        select(
            scenarios_data,
            target_profiles_dict.c.name.label("target_profile_name"),
        )
        .select_from(
            scenarios_data.outerjoin(
                target_profiles_dict,
                target_profiles_dict.c.target_profile_id == scenarios_data.c.target_profile_id,
            )
        )
        .where(scenarios_data.c.scenario_id == scenario_id)
    )
    result = (await conn.execute(statement)).mappings().one()

    return ScenarioDTO(**result)


async def patch_scenario_to_db(
    conn: AsyncConnection, scenario: ScenariosPatch, scenario_id: int, user_id: str
) -> ScenarioDTO:
    """Patch scenario object."""

    statement = select(scenarios_data).where(scenarios_data.c.scenario_id == scenario_id)
    result = (await conn.execute(statement)).mappings().one_or_none()
    if result is None:
        raise EntityNotFoundById(scenario_id, "scenario")

    statement = select(projects_data).where(projects_data.c.project_id == result.project_id)
    project = (await conn.execute(statement)).mappings().one_or_none()
    if project.user_id != user_id:
        raise AccessDeniedError(result.project_id, "project")

    if scenario.target_profile_id is not None:
        statement = select(target_profiles_dict).where(
            target_profiles_dict.c.target_profile_id == scenario.target_profile_id
        )
        profile = (await conn.execute(statement)).mappings().one_or_none()
        if profile is None:
            raise EntityNotFoundById(scenario.target_profile_id, "target profile")

    statement = update(scenarios_data).where(scenarios_data.c.scenario_id == scenario_id).returning(scenarios_data)
    new_values_for_scenario = {}
    for k, v in scenario.model_dump(exclude_unset=True).items():
        new_values_for_scenario.update({k: v})
    statement = statement.values(**new_values_for_scenario)

    await conn.execute(statement)
    await conn.commit()

    statement = (
        select(
            scenarios_data,
            target_profiles_dict.c.name.label("target_profile_name"),
        )
        .select_from(
            scenarios_data.outerjoin(
                target_profiles_dict,
                target_profiles_dict.c.target_profile_id == scenarios_data.c.target_profile_id,
            )
        )
        .where(scenarios_data.c.scenario_id == scenario_id)
    )
    result = (await conn.execute(statement)).mappings().one()

    return ScenarioDTO(**result)


async def delete_scenario_from_db(conn: AsyncConnection, scenario_id: int, user_id: str) -> dict:
    """Delete scenario object."""

    statement = select(scenarios_data).where(scenarios_data.c.scenario_id == scenario_id)
    result = (await conn.execute(statement)).mappings().one_or_none()
    if result is None:
        raise EntityNotFoundById(scenario_id, "scenario")

    statement = select(projects_data).where(projects_data.c.project_id == result.project_id)
    project = (await conn.execute(statement)).mappings().one_or_none()
    if project.user_id != user_id:
        raise AccessDeniedError(result.project_id, "project")

    statement = delete(scenarios_data).where(scenarios_data.c.scenario_id == scenario_id)

    await conn.execute(statement)
    await conn.commit()

    return {"status": "ok"}
