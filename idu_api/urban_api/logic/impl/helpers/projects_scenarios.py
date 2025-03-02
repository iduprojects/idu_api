"""Projects scenarios internal logic is defined here."""

import asyncio

from sqlalchemy import RowMapping, delete, insert, literal, select, update
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
from idu_api.urban_api.exceptions.logic.users import AccessDeniedError
from idu_api.urban_api.logic.impl.helpers.projects_objects import check_project
from idu_api.urban_api.logic.impl.helpers.utils import check_existence, extract_values_from_model
from idu_api.urban_api.schemas import (
    ScenarioPatch,
    ScenarioPost,
    ScenarioPut,
)


async def check_scenario(conn: AsyncConnection, scenario_id: int, user: UserDTO | None, to_edit: bool = False) -> None:
    """Check scenario existence and user access."""

    statement = (
        select(projects_data.c.project_id, projects_data.c.user_id, projects_data.c.public)
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


async def get_project_by_scenario_id(
    conn: AsyncConnection, scenario_id: int, user: UserDTO | None, to_edit: bool = False
) -> RowMapping:
    """Get project"""

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

    return project


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
    """Create a new scenario from another scenario (copy) by its identifier."""

    if not await check_existence(conn, projects_data, conditions={"project_id": scenario.project_id}):
        raise EntityNotFoundById(scenario.project_id, "project")

    if scenario.functional_zone_type_id is not None:
        if not await check_existence(
            conn, functional_zone_types_dict, conditions={"functional_zone_type_id": scenario.functional_zone_type_id}
        ):
            raise EntityNotFoundById(scenario.functional_zone_type_id, "functional zone type")

    project = await get_project_by_scenario_id(conn, scenario_id, user, to_edit=True)

    # fixme: use the real parent scenario identifier from the user
    #  instead of using the basic regional scenario by default
    parent_scenario_id = (
        await conn.execute(
            select(scenarios_data.c.scenario_id)
            .select_from(scenarios_data.join(projects_data, projects_data.c.project_id == scenarios_data.c.project_id))
            .where(
                projects_data.c.territory_id == project.territory_id,
                projects_data.c.is_regional.is_(True),
                scenarios_data.c.is_based.is_(True),
            )
        )
    ).scalar_one_or_none()
    if parent_scenario_id is None:
        raise EntitiesNotFoundByIds("parent regional scenario")

    new_scenario_id = (
        await conn.execute(
            insert(scenarios_data)
            .values(**scenario.model_dump(), parent_id=parent_scenario_id, is_based=False)
            .returning(scenarios_data.c.scenario_id)
        )
    ).scalar_one()

    # copy urban objects
    statement = select(
        projects_urban_objects_data.c.public_urban_object_id,
        projects_urban_objects_data.c.object_geometry_id,
        projects_urban_objects_data.c.physical_object_id,
        projects_urban_objects_data.c.service_id,
        projects_urban_objects_data.c.public_object_geometry_id,
        projects_urban_objects_data.c.public_physical_object_id,
        projects_urban_objects_data.c.public_service_id,
    ).where(projects_urban_objects_data.c.scenario_id == scenario_id)
    old_urban_objects = (await conn.execute(statement)).mappings().all()

    geometry_ids = set(obj.object_geometry_id for obj in old_urban_objects if obj.object_geometry_id is not None)
    physical_ids = set(obj.physical_object_id for obj in old_urban_objects if obj.physical_object_id is not None)
    service_ids = set(obj.service_id for obj in old_urban_objects if obj.service_id is not None)

    geometry_mapping, physical_mapping, service_mapping = await asyncio.gather(
        copy_geometries(conn, sorted(list(geometry_ids))),
        copy_physical_objects(conn, sorted(list(physical_ids))),
        copy_services(conn, sorted(list(service_ids))),
    )

    new_objects = []
    for obj in old_urban_objects:
        if obj.public_urban_object_id is not None:
            new_obj = {
                "scenario_id": new_scenario_id,
                "object_geometry_id": None,
                "physical_object_id": None,
                "service_id": None,
                "public_object_geometry_id": None,
                "public_physical_object_id": None,
                "public_service_id": None,
                "public_urban_object_id": obj.public_urban_object_id,
            }
        else:
            new_obj = {
                "scenario_id": new_scenario_id,
                "object_geometry_id": (
                    geometry_mapping.get(obj.object_geometry_id) if obj.public_object_geometry_id is None else None
                ),
                "physical_object_id": (
                    physical_mapping.get(obj.physical_object_id) if obj.public_physical_object_id is None else None
                ),
                "service_id": service_mapping.get(obj.service_id) if obj.public_service_id is None else None,
                "public_object_geometry_id": obj.public_object_geometry_id,
                "public_physical_object_id": obj.public_physical_object_id,
                "public_service_id": obj.public_service_id,
                "public_urban_object_id": None,
            }
        new_objects.append(new_obj)
    if new_objects:
        await conn.execute(insert(projects_urban_objects_data).values(new_objects))

    # copy functional zones and indicators values
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
                    literal(new_scenario_id).label("scenario_id"),
                    projects_functional_zones.c.name,
                    projects_functional_zones.c.functional_zone_type_id,
                    projects_functional_zones.c.geometry,
                    projects_functional_zones.c.year,
                    projects_functional_zones.c.source,
                    projects_functional_zones.c.properties,
                ).where(projects_functional_zones.c.scenario_id == scenario_id),
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
                    literal(new_scenario_id).label("scenario_id"),
                    projects_indicators_data.c.indicator_id,
                    projects_indicators_data.c.territory_id,
                    projects_indicators_data.c.hexagon_id,
                    projects_indicators_data.c.value,
                    projects_indicators_data.c.comment,
                    projects_indicators_data.c.information_source,
                    projects_indicators_data.c.properties,
                ).where(projects_indicators_data.c.scenario_id == scenario_id),
            )
        ),
    )

    await conn.commit()

    return await get_scenario_by_id_from_db(conn, new_scenario_id, user)


async def add_new_scenario_to_db(conn: AsyncConnection, scenario: ScenarioPost, user: UserDTO) -> ScenarioDTO:
    """Create a new scenario from base scenario."""

    base_scenario_id = (
        await conn.execute(
            select(scenarios_data.c.scenario_id)
            .select_from(scenarios_data.join(projects_data, projects_data.c.project_id == scenarios_data.c.project_id))
            .where(
                projects_data.c.project_id == scenario.project_id,
                scenarios_data.c.is_based.is_(True),
            )
        )
    ).scalar_one_or_none()
    if base_scenario_id is None:
        raise EntityNotFoundById(scenario.project_id, "project")

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
            scenarios_data.c.project_id == requested_scenario.project_id, scenarios_data.c.is_based.is_(True)
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
            scenarios_data.c.project_id == requested_scenario.project_id, scenarios_data.c.is_based.is_(True)
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


async def copy_geometries(conn, geometry_ids: list[int]) -> dict[int, int]:
    if not geometry_ids:
        return {}

    # find all geometries by identifiers
    statement = (
        select(
            projects_object_geometries_data.c.public_object_geometry_id,
            projects_object_geometries_data.c.territory_id,
            projects_object_geometries_data.c.address,
            projects_object_geometries_data.c.osm_id,
            projects_object_geometries_data.c.geometry,
            projects_object_geometries_data.c.centre_point,
        )
        .where(projects_object_geometries_data.c.object_geometry_id.in_(geometry_ids))
        .order_by(projects_object_geometries_data.c.object_geometry_id)
    )
    old_geometries = (await conn.execute(statement)).mappings().all()
    old_geometries = [dict(row) for row in old_geometries]

    # insert copies of old geometries
    new_ids = (
        (
            await conn.execute(
                insert(projects_object_geometries_data)
                .values(old_geometries)
                .returning(projects_object_geometries_data.c.object_geometry_id)
            )
        )
        .scalars()
        .all()
    )

    # mapping old and new objects geometries
    return dict(zip(geometry_ids, new_ids))


async def copy_physical_objects(conn, physical_ids: list[int]) -> dict[int, int]:
    if not physical_ids:
        return {}

    # find all physical objects by identifiers
    statement = (
        select(
            projects_physical_objects_data.c.public_physical_object_id,
            projects_physical_objects_data.c.physical_object_type_id,
            projects_physical_objects_data.c.name,
            projects_physical_objects_data.c.properties,
        )
        .where(projects_physical_objects_data.c.physical_object_id.in_(physical_ids))
        .order_by(projects_physical_objects_data.c.physical_object_id)
    )
    old_physical_objects = (await conn.execute(statement)).mappings().all()
    old_physical_objects = [dict(row) for row in old_physical_objects]

    # insert copies of old physical objects
    new_ids = (
        (
            await conn.execute(
                insert(projects_physical_objects_data)
                .values(old_physical_objects)
                .returning(projects_physical_objects_data.c.physical_object_id)
            )
        )
        .scalars()
        .all()
    )

    # mapping old and new physical objects
    return dict(zip(physical_ids, new_ids))


async def copy_services(conn, service_ids: list[int]) -> dict[int, int]:
    if not service_ids:
        return {}

    # find all services by identifiers
    statement = (
        select(
            projects_services_data.c.public_service_id,
            projects_services_data.c.service_type_id,
            projects_services_data.c.name,
            projects_services_data.c.capacity,
            projects_services_data.c.is_capacity_real,
            projects_services_data.c.properties,
        )
        .where(projects_services_data.c.service_id.in_(service_ids))
        .order_by(projects_services_data.c.service_id)
    )
    old_services = (await conn.execute(statement)).mappings().all()
    old_services = [dict(row) for row in old_services]

    # insert copies of old services
    new_ids = (
        (
            await conn.execute(
                insert(projects_services_data).values(old_services).returning(projects_services_data.c.service_id)
            )
        )
        .scalars()
        .all()
    )

    # mapping old and new services
    return dict(zip(service_ids, new_ids))
