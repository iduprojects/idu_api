"""Projects scenarios internal logic is defined here."""

import asyncio

from sqlalchemy import RowMapping, case, delete, insert, literal, select, update
from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.common.db.entities import (
    functional_zone_types_dict,
    projects_data,
    projects_functional_zones,
    projects_indicators_data,
    projects_object_geometries_data,
    projects_physical_objects_data,
    projects_services_data,
    projects_urban_objects_data,
    scenarios_data,
    territories_data,
)
from idu_api.urban_api.dto import ScenarioDTO, UserDTO
from idu_api.urban_api.exceptions.logic.common import (
    EntitiesNotFoundByIds,
    EntityNotFoundById,
)
from idu_api.urban_api.exceptions.logic.projects import NotAllowedInRegionalScenario
from idu_api.urban_api.exceptions.logic.users import AccessDeniedError
from idu_api.urban_api.logic.impl.helpers.projects_objects import (
    check_project,
    copy_geometries,
    copy_physical_objects,
    copy_services,
)
from idu_api.urban_api.logic.impl.helpers.utils import check_existence, extract_values_from_model
from idu_api.urban_api.schemas import (
    ScenarioPatch,
    ScenarioPost,
    ScenarioPut,
)


async def check_scenario(
    conn: AsyncConnection,
    scenario_id: int,
    user: UserDTO | None,
    to_edit: bool = False,
    allow_regional: bool = True,
) -> None:
    """Check scenario existence and user access."""

    statement = (
        select(projects_data.c.project_id, projects_data.c.user_id, projects_data.c.public, projects_data.c.is_regional)
        .select_from(scenarios_data.join(projects_data, projects_data.c.project_id == scenarios_data.c.project_id))
        .where(scenarios_data.c.scenario_id == scenario_id)
    )
    scenario = (await conn.execute(statement)).mappings().one_or_none()
    if scenario is None:
        raise EntityNotFoundById(scenario_id, "scenario")
    if user is None:
        if not scenario.public:
            raise AccessDeniedError(scenario.project_id, "project")
    elif scenario.user_id != user.id and (not scenario.public or to_edit) and not user.is_superuser:
        raise AccessDeniedError(scenario.project_id, "project")
    if scenario.is_regional and not allow_regional:
        raise NotAllowedInRegionalScenario()


async def get_project_by_scenario_id(
    conn: AsyncConnection,
    scenario_id: int,
    user: UserDTO | None,
    to_edit: bool = False,
    allow_regional: bool = True,
) -> RowMapping:
    """Get project with checking access"""

    statement = (
        select(projects_data)
        .select_from(scenarios_data.join(projects_data, projects_data.c.project_id == scenarios_data.c.project_id))
        .where(scenarios_data.c.scenario_id == scenario_id)
    )
    project = (await conn.execute(statement)).mappings().one_or_none()
    if project is None:
        raise EntityNotFoundById(scenario_id, "scenario")
    if user is None:
        if not project.public:
            raise AccessDeniedError(project.project_id, "project")
    elif project.user_id != user.id and (not project.public or to_edit) and not user.is_superuser:
        raise AccessDeniedError(project.project_id, "project")
    if project.is_regional and not allow_regional:
        raise NotAllowedInRegionalScenario()

    return project


async def get_scenarios_from_db(
    conn: AsyncConnection,
    parent_id: int | None,
    project_id: int | None,
    territory_id: int | None,
    is_based: bool,
    only_own: bool,
    user: UserDTO | None,
) -> list[ScenarioDTO]:
    """Get list of scenario objects."""

    where_clause = scenarios_data.c.parent_id.is_(None)
    if parent_id is not None:
        await check_scenario(conn, parent_id, user)
        where_clause = scenarios_data.c.parent_id == parent_id

    scenarios_data_parents = scenarios_data.alias("scenarios_data_parents")
    statement = (
        select(
            scenarios_data,
            scenarios_data_parents.c.name.label("parent_name"),
            projects_data.c.name.label("project_name"),
            projects_data.c.user_id.label("project_user_id"),
            territories_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
            functional_zone_types_dict.c.name.label("functional_zone_type_name"),
            functional_zone_types_dict.c.zone_nickname.label("functional_zone_type_nickname"),
            functional_zone_types_dict.c.description.label("functional_zone_type_description"),
        )
        .select_from(
            scenarios_data.join(projects_data, projects_data.c.project_id == scenarios_data.c.project_id)
            .join(territories_data, territories_data.c.territory_id == projects_data.c.territory_id)
            .outerjoin(
                functional_zone_types_dict,
                functional_zone_types_dict.c.functional_zone_type_id == scenarios_data.c.functional_zone_type_id,
            )
            .outerjoin(
                scenarios_data_parents,
                scenarios_data.c.parent_id == scenarios_data_parents.c.scenario_id,
            )
        )
        .where(where_clause)
    )

    if project_id is not None:
        await check_project(conn, project_id, user)
        statement = statement.where(scenarios_data.c.project_id == project_id)

    if territory_id is not None:
        statement = statement.where(projects_data.c.territory_id == territory_id)

    if is_based:
        statement = statement.where(scenarios_data.c.is_based.is_(True))

    if only_own and parent_id is not None:
        statement = statement.where(projects_data.c.user_id == user.id)
    elif only_own and parent_id is None:
        statement = statement.where((projects_data.c.user_id == user.id) | scenarios_data.c.is_based.is_(True))
    elif user is not None:
        statement = statement.where(
            (projects_data.c.user_id == user.id) | (projects_data.c.public.is_(True) if not user.is_superuser else True)
        )
    else:
        statement = statement.where(projects_data.c.public.is_(True))

    result = (await conn.execute(statement)).mappings().all()

    return [ScenarioDTO(**scenario) for scenario in result]


async def get_scenarios_by_project_id_from_db(
    conn: AsyncConnection, project_id: int, user: UserDTO | None
) -> list[ScenarioDTO]:
    """Get list of scenario objects by project id."""

    await check_project(conn, project_id, user)

    scenarios_data_parents = scenarios_data.alias("scenarios_data_parents")
    statement = (
        select(
            scenarios_data,
            scenarios_data_parents.c.name.label("parent_name"),
            projects_data.c.name.label("project_name"),
            projects_data.c.user_id.label("project_user_id"),
            territories_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
            functional_zone_types_dict.c.name.label("functional_zone_type_name"),
            functional_zone_types_dict.c.zone_nickname.label("functional_zone_type_nickname"),
            functional_zone_types_dict.c.description.label("functional_zone_type_description"),
        )
        .select_from(
            scenarios_data.join(projects_data, projects_data.c.project_id == scenarios_data.c.project_id)
            .join(territories_data, territories_data.c.territory_id == projects_data.c.territory_id)
            .outerjoin(
                functional_zone_types_dict,
                functional_zone_types_dict.c.functional_zone_type_id == scenarios_data.c.functional_zone_type_id,
            )
            .outerjoin(
                scenarios_data_parents,
                scenarios_data.c.parent_id == scenarios_data_parents.c.scenario_id,
            )
        )
        .where(scenarios_data.c.project_id == project_id)
    )
    result = (await conn.execute(statement)).mappings().all()

    return [ScenarioDTO(**scenario) for scenario in result]


async def get_scenario_by_id_from_db(conn: AsyncConnection, scenario_id: int, user: UserDTO | None) -> ScenarioDTO:
    """Get scenario object by id."""

    scenarios_data_parents = scenarios_data.alias("scenarios_data_parents")
    statement = (
        select(
            scenarios_data,
            scenarios_data_parents.c.name.label("parent_name"),
            projects_data.c.name.label("project_name"),
            projects_data.c.user_id.label("project_user_id"),
            territories_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
            functional_zone_types_dict.c.name.label("functional_zone_type_name"),
            functional_zone_types_dict.c.zone_nickname.label("functional_zone_type_nickname"),
            functional_zone_types_dict.c.description.label("functional_zone_type_description"),
        )
        .select_from(
            scenarios_data.join(projects_data, projects_data.c.project_id == scenarios_data.c.project_id)
            .join(territories_data, territories_data.c.territory_id == projects_data.c.territory_id)
            .outerjoin(
                functional_zone_types_dict,
                functional_zone_types_dict.c.functional_zone_type_id == scenarios_data.c.functional_zone_type_id,
            )
            .outerjoin(
                scenarios_data_parents,
                scenarios_data.c.parent_id == scenarios_data_parents.c.scenario_id,
            )
        )
        .where(scenarios_data.c.scenario_id == scenario_id)
    )
    result = (await conn.execute(statement)).mappings().one_or_none()
    if result is None:
        raise EntityNotFoundById(scenario_id, "scenario")

    statement = select(projects_data).where(projects_data.c.project_id == result.project_id)
    project = (await conn.execute(statement)).mappings().one_or_none()
    if user is None:
        if not project.public:
            raise AccessDeniedError(result.project_id, "project")
    elif project.user_id != user.id and not project.public and not user.is_superuser:
        raise AccessDeniedError(result.project_id, "project")

    return ScenarioDTO(**result)


async def copy_scenario_to_db(
    conn: AsyncConnection, scenario: ScenarioPost, scenario_id: int, user: UserDTO
) -> ScenarioDTO:
    """Copy an existing scenario and all its related entities to a new one."""

    await _validate_input(conn, scenario, user)
    _, parent_id = await _validate_source_scenario(conn, scenario_id, user)

    new_scenario_id = await _create_scenario(conn, scenario, parent_id)

    await _copy_urban_objects(conn, scenario_id, new_scenario_id)
    await _copy_functional_zones_and_indicators(conn, scenario_id, new_scenario_id)

    await conn.commit()
    return await get_scenario_by_id_from_db(conn, new_scenario_id, user)


async def add_new_scenario_to_db(conn: AsyncConnection, scenario: ScenarioPost, user: UserDTO) -> ScenarioDTO:
    """Create a new scenario from base scenario."""
    project = (
        (await conn.execute(select(projects_data).where(projects_data.c.project_id == scenario.project_id)))
        .mappings()
        .one_or_none()
    )
    if project is None:
        raise EntityNotFoundById(scenario.project_id, "project")

    if project.is_regional:
        base_scenario_id = (
            await conn.execute(
                select(scenarios_data.c.scenario_id)
                .select_from(
                    scenarios_data.join(projects_data, projects_data.c.project_id == scenarios_data.c.project_id)
                )
                .where(
                    projects_data.c.is_regional.is_(True),
                    scenarios_data.c.is_based.is_(True),
                    projects_data.c.territory_id == project.territory_id,
                )
            )
        ).scalar_one()
    else:
        regional_scenarios = scenarios_data.alias("regional_scenarios")
        base_scenario_id = (
            await conn.execute(
                select(scenarios_data.c.scenario_id)
                .select_from(
                    scenarios_data.join(
                        regional_scenarios, regional_scenarios.c.scenario_id == scenarios_data.c.parent_id
                    )
                )
                .where(
                    scenarios_data.c.project_id == project.project_id,
                    scenarios_data.c.is_based.is_(True),
                    regional_scenarios.c.is_based.is_(True),
                )
            )
        ).scalar_one()

    return await copy_scenario_to_db(conn, scenario, base_scenario_id, user)


async def put_scenario_to_db(
    conn: AsyncConnection, scenario: ScenarioPut, scenario_id: int, user: UserDTO
) -> ScenarioDTO:
    """Update scenario object - all attributes."""

    statement = (
        select(scenarios_data, projects_data.c.project_id, projects_data.c.user_id)
        .select_from(scenarios_data.join(projects_data, projects_data.c.project_id == scenarios_data.c.project_id))
        .where(scenarios_data.c.scenario_id == scenario_id)
    )
    requested_scenario = (await conn.execute(statement)).mappings().one_or_none()
    if requested_scenario is None:
        raise EntityNotFoundById(scenario_id, "scenario")
    if requested_scenario.user_id != user.id and not user.is_superuser:
        raise AccessDeniedError(requested_scenario.project_id, "project")

    if scenario.functional_zone_type_id is not None:
        if not await check_existence(
            conn, functional_zone_types_dict, conditions={"functional_zone_type_id": scenario.functional_zone_type_id}
        ):
            raise EntityNotFoundById(scenario.functional_zone_type_id, "functional zone type")

    if requested_scenario.is_based and not scenario.is_based:
        raise ValueError(
            "If you want to create new base scenario, change the one that should become the base, not the current one"
        )

    if not requested_scenario.is_based and scenario.is_based:
        statement = select(scenarios_data.c.scenario_id).where(
            scenarios_data.c.project_id == requested_scenario.project_id,
            scenarios_data.c.is_based.is_(True),
            scenarios_data.c.parent_id == requested_scenario.c.parent_id,
        )
        based_scenario_id = (await conn.execute(statement)).scalar_one_or_none()
        statement = (
            update(scenarios_data).where(scenarios_data.c.scenario_id == based_scenario_id).values(is_based=False)
        )
        await conn.execute(statement)

    values = extract_values_from_model(scenario, to_update=True)

    statement = update(scenarios_data).where(scenarios_data.c.scenario_id == scenario_id).values(**values)
    await conn.execute(statement)
    await conn.commit()

    return await get_scenario_by_id_from_db(conn, scenario_id, user)


async def patch_scenario_to_db(
    conn: AsyncConnection, scenario: ScenarioPatch, scenario_id: int, user: UserDTO
) -> ScenarioDTO:
    """Update scenario object - only given fields."""

    statement = (
        select(scenarios_data, projects_data.c.project_id, projects_data.c.user_id)
        .select_from(scenarios_data.join(projects_data, projects_data.c.project_id == scenarios_data.c.project_id))
        .where(scenarios_data.c.scenario_id == scenario_id)
    )
    requested_scenario = (await conn.execute(statement)).mappings().one_or_none()
    if requested_scenario is None:
        raise EntityNotFoundById(scenario_id, "scenario")
    if requested_scenario.user_id != user.id and not user.is_superuser:
        raise AccessDeniedError(requested_scenario.project_id, "project")

    if scenario.functional_zone_type_id is not None:
        if not await check_existence(
            conn, functional_zone_types_dict, conditions={"functional_zone_type_id": scenario.functional_zone_type_id}
        ):
            raise EntityNotFoundById(scenario.functional_zone_type_id, "functional zone type")

    if scenario.is_based is not None and requested_scenario.is_based and not scenario.is_based:
        raise ValueError(
            "If you want to create new base scenario, change the one that should become the base, not the current one"
        )

    if scenario.is_based is not None and not requested_scenario.is_based and scenario.is_based:
        statement = select(scenarios_data.c.scenario_id).where(
            scenarios_data.c.project_id == requested_scenario.project_id,
            scenarios_data.c.is_based.is_(True),
            scenarios_data.c.parent_id == requested_scenario.c.parent_id,
        )
        based_scenario_id = (await conn.execute(statement)).scalar_one_or_none()
        statement = (
            update(scenarios_data).where(scenarios_data.c.scenario_id == based_scenario_id).values(is_based=False)
        )
        await conn.execute(statement)

    values = extract_values_from_model(scenario, exclude_unset=True, to_update=True)

    statement = update(scenarios_data).where(scenarios_data.c.scenario_id == scenario_id).values(**values)

    await conn.execute(statement)
    await conn.commit()

    return await get_scenario_by_id_from_db(conn, scenario_id, user)


async def delete_scenario_from_db(conn: AsyncConnection, scenario_id: int, user: UserDTO) -> dict:
    """Delete scenario object by identifier."""

    await check_scenario(conn, scenario_id, user, to_edit=True)

    urban_objects = (
        (
            await conn.execute(
                select(
                    projects_urban_objects_data.c.object_geometry_id,
                    projects_urban_objects_data.c.physical_object_id,
                    projects_urban_objects_data.c.service_id,
                ).where(projects_urban_objects_data.c.scenario_id == scenario_id)
            )
        )
        .mappings()
        .all()
    )

    geometry_ids = {obj.object_geometry_id for obj in urban_objects if obj.object_geometry_id is not None}
    physical_ids = {obj.physical_object_id for obj in urban_objects if obj.physical_object_id is not None}
    service_ids = {obj.service_id for obj in urban_objects if obj.service_id is not None}

    if geometry_ids:
        delete_geometry_statement = delete(projects_object_geometries_data).where(
            projects_object_geometries_data.c.object_geometry_id.in_(geometry_ids)
        )
        await conn.execute(delete_geometry_statement)

    if physical_ids:
        delete_physical_statement = delete(projects_physical_objects_data).where(
            projects_physical_objects_data.c.physical_object_id.in_(physical_ids)
        )
        await conn.execute(delete_physical_statement)

    if service_ids:
        delete_service_statement = delete(projects_services_data).where(
            projects_services_data.c.service_id.in_(service_ids)
        )
        await conn.execute(delete_service_statement)

    statement = delete(scenarios_data).where(scenarios_data.c.scenario_id == scenario_id)
    await conn.execute(statement)

    await conn.commit()

    return {"status": "ok"}


####################################################################################
#                            Helper functions                                      #
####################################################################################


async def _validate_input(conn: AsyncConnection, scenario: ScenarioPost, user: UserDTO):
    """Validate project existence and ownership, and optional functional zone type."""
    if scenario.functional_zone_type_id is not None:
        exists = await check_existence(
            conn, functional_zone_types_dict, conditions={"functional_zone_type_id": scenario.functional_zone_type_id}
        )
        if not exists:
            raise EntityNotFoundById(scenario.functional_zone_type_id, "functional zone type")

    project = (
        (await conn.execute(select(projects_data).where(projects_data.c.project_id == scenario.project_id)))
        .mappings()
        .one_or_none()
    )
    if project is None:
        raise EntityNotFoundById(scenario.project_id, "project")
    if project.user_id != user.id and not user.is_superuser:
        raise AccessDeniedError(scenario.project_id, "project")


async def _validate_source_scenario(conn: AsyncConnection, scenario_id: int, user: UserDTO) -> tuple[RowMapping, int]:
    """Validate the original scenario and determine if a regional parent is required."""
    statement = (
        select(projects_data)
        .select_from(scenarios_data.join(projects_data, projects_data.c.project_id == scenarios_data.c.project_id))
        .where(scenarios_data.c.scenario_id == scenario_id)
    )
    project = (await conn.execute(statement)).mappings().one_or_none()
    if project is None:
        raise EntityNotFoundById(scenario_id, "scenario")
    if project.user_id != user.id and not project.public and not user.is_superuser:
        raise AccessDeniedError(project.project_id, "project")

    parent_id = None
    if not project.is_regional:
        parent_id = (
            await conn.execute(
                select(scenarios_data.c.scenario_id)
                .select_from(scenarios_data.join(projects_data))
                .where(
                    projects_data.c.territory_id == project.territory_id,
                    projects_data.c.is_regional.is_(True),
                    scenarios_data.c.is_based.is_(True),
                )
            )
        ).scalar_one_or_none()
        if parent_id is None:
            raise EntitiesNotFoundByIds("parent regional scenario")

    return project, parent_id


async def _create_scenario(conn: AsyncConnection, scenario: ScenarioPost, parent_id: int | None) -> int:
    """Insert a new scenario into the database."""
    return (
        await conn.execute(
            insert(scenarios_data)
            .values(**scenario.model_dump(), parent_id=parent_id, is_based=False)
            .returning(scenarios_data.c.scenario_id)
        )
    ).scalar_one()


async def _copy_urban_objects(conn: AsyncConnection, old_id: int, new_id: int):
    """Copy urban objects from old scenario to new scenario, remapping associated resources."""
    old_objs_cte = (
        select(
            projects_urban_objects_data.c.public_urban_object_id,
            projects_urban_objects_data.c.object_geometry_id,
            projects_urban_objects_data.c.physical_object_id,
            projects_urban_objects_data.c.service_id,
            projects_urban_objects_data.c.public_object_geometry_id,
            projects_urban_objects_data.c.public_physical_object_id,
            projects_urban_objects_data.c.public_service_id,
        ).where(projects_urban_objects_data.c.scenario_id == old_id)
    ).cte(name="old_urban_objects")

    results = (await conn.execute(select(old_objs_cte))).mappings().all()

    geom_ids = {r.object_geometry_id for r in results if r.object_geometry_id}
    phys_ids = {r.physical_object_id for r in results if r.physical_object_id}
    svc_ids = {r.service_id for r in results if r.service_id}

    geom_map, phys_map, svc_map = await asyncio.gather(
        copy_geometries(conn, sorted(list(geom_ids))),
        copy_physical_objects(conn, sorted(list(phys_ids))),
        copy_services(conn, sorted(list(svc_ids))),
    )

    # Copy public urban objects
    await conn.execute(
        insert(projects_urban_objects_data).from_select(
            ["scenario_id", "public_urban_object_id"],
            select(literal(new_id).label("scenario_id"), old_objs_cte.c.public_urban_object_id).where(
                old_objs_cte.c.public_urban_object_id.isnot(None)
            ),
        )
    )

    # Copy internal urban objects with mapping
    def build_case(col, mapping, default=None):
        return (
            case(*[(col == k, literal(v)) for k, v in mapping.items()], else_=literal(default))
            if mapping
            else literal(default)
        )

    await conn.execute(
        insert(projects_urban_objects_data).from_select(
            [
                "scenario_id",
                "object_geometry_id",
                "physical_object_id",
                "service_id",
                "public_object_geometry_id",
                "public_physical_object_id",
                "public_service_id",
            ],
            select(
                literal(new_id).label("scenario_id"),
                build_case(old_objs_cte.c.object_geometry_id, geom_map),
                build_case(old_objs_cte.c.physical_object_id, phys_map),
                build_case(old_objs_cte.c.service_id, svc_map),
                old_objs_cte.c.public_object_geometry_id,
                old_objs_cte.c.public_physical_object_id,
                old_objs_cte.c.public_service_id,
            ).where(old_objs_cte.c.public_urban_object_id.is_(None)),
        )
    )


async def _copy_functional_zones_and_indicators(conn: AsyncConnection, old_id: int, new_id: int):
    """Copy functional zones and indicator values from old scenario."""
    await asyncio.gather(
        conn.execute(
            insert(projects_functional_zones).from_select(
                [
                    projects_functional_zones.c.scenario_id,
                    projects_functional_zones.c.name,
                    projects_functional_zones.c.functional_zone_type_id,
                    projects_functional_zones.c.geometry,
                    projects_functional_zones.c.year,
                    projects_functional_zones.c.source,
                    projects_functional_zones.c.properties,
                ],
                select(
                    literal(new_id).label("scenario_id"),
                    projects_functional_zones.c.name,
                    projects_functional_zones.c.functional_zone_type_id,
                    projects_functional_zones.c.geometry,
                    projects_functional_zones.c.year,
                    projects_functional_zones.c.source,
                    projects_functional_zones.c.properties,
                ).where(projects_functional_zones.c.scenario_id == old_id),
            )
        ),
        conn.execute(
            insert(projects_indicators_data).from_select(
                [
                    projects_indicators_data.c.scenario_id,
                    projects_indicators_data.c.indicator_id,
                    projects_indicators_data.c.territory_id,
                    projects_indicators_data.c.hexagon_id,
                    projects_indicators_data.c.value,
                    projects_indicators_data.c.comment,
                    projects_indicators_data.c.information_source,
                    projects_indicators_data.c.properties,
                ],
                select(
                    literal(new_id).label("scenario_id"),
                    projects_indicators_data.c.indicator_id,
                    projects_indicators_data.c.territory_id,
                    projects_indicators_data.c.hexagon_id,
                    projects_indicators_data.c.value,
                    projects_indicators_data.c.comment,
                    projects_indicators_data.c.information_source,
                    projects_indicators_data.c.properties,
                ).where(projects_indicators_data.c.scenario_id == old_id),
            )
        ),
    )
