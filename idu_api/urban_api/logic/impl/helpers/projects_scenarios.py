"""Projects scenarios internal logic is defined here."""

from geoalchemy2.functions import ST_GeomFromText
from sqlalchemy import and_, delete, insert, select, text, update
from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.common.db.entities import (
    physical_object_types_dict,
    projects_data,
    projects_object_geometries_data,
    projects_physical_objects_data,
    projects_services_data,
    projects_urban_objects_data,
    scenarios_data,
    service_types_dict,
    target_profiles_dict,
    territories_data,
    territory_types_dict,
)
from idu_api.urban_api.dto import ScenarioDTO, ScenarioUrbanObjectDTO
from idu_api.urban_api.exceptions.logic.common import EntityAlreadyExists, EntityNotFoundById, EntityNotFoundByParams
from idu_api.urban_api.exceptions.logic.users import AccessDeniedError
from idu_api.urban_api.logic.impl.helpers.projects_scenarios_urban_objects import (
    get_scenario_urban_object_by_id_from_db,
)
from idu_api.urban_api.schemas import (
    PhysicalObjectsDataPost,
    PhysicalObjectWithGeometryPost,
    ScenariosPatch,
    ScenariosPost,
    ScenariosPut,
    ServicesDataPost,
)


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


async def add_physical_object_to_scenario_in_db(
    conn: AsyncConnection, scenario_id: int, physical_object: PhysicalObjectWithGeometryPost, user_id: str
) -> ScenarioUrbanObjectDTO:
    """Add physical object to scenario."""

    statement = select(scenarios_data).where(scenarios_data.c.scenario_id == scenario_id)
    result = (await conn.execute(statement)).mappings().one_or_none()
    if result is None:
        raise EntityNotFoundById(scenario_id, "scenario")

    statement = select(projects_data).where(projects_data.c.project_id == result.project_id)
    project = (await conn.execute(statement)).mappings().one_or_none()
    if project.user_id != user_id:
        raise AccessDeniedError(result.project_id, "project")

    statement = select(territories_data).where(territories_data.c.territory_id == physical_object.territory_id)
    territory = (await conn.execute(statement)).one_or_none()
    if territory is None:
        raise EntityNotFoundById(physical_object.territory_id, "territory")

    statement = select(physical_object_types_dict).where(
        physical_object_types_dict.c.physical_object_type_id == physical_object.physical_object_type_id
    )
    physical_object_type = (await conn.execute(statement)).one_or_none()
    if physical_object_type is None:
        raise EntityNotFoundById(physical_object.physical_object_type_id, "physical object type")

    statement = (
        insert(projects_physical_objects_data)
        .values(
            physical_object_type_id=physical_object.physical_object_type_id,
            name=physical_object.name,
            properties=physical_object.properties,
        )
        .returning(projects_physical_objects_data.c.physical_object_id)
    )
    physical_object_id = (await conn.execute(statement)).scalar_one()

    statement = (
        insert(projects_object_geometries_data)
        .values(
            territory_id=physical_object.territory_id,
            geometry=ST_GeomFromText(str(physical_object.geometry.as_shapely_geometry()), text("4326")),
            centre_point=ST_GeomFromText(str(physical_object.centre_point.as_shapely_geometry()), text("4326")),
            address=physical_object.address,
        )
        .returning(projects_object_geometries_data.c.object_geometry_id)
    )
    object_geometry_id = (await conn.execute(statement)).scalar_one()

    statement = (
        insert(projects_urban_objects_data)
        .values(physical_object_id=physical_object_id, object_geometry_id=object_geometry_id, scenario_id=scenario_id)
        .returning(projects_urban_objects_data.c.urban_object_id)
    )

    scenario_urban_object_id = (await conn.execute(statement)).scalar_one_or_none()
    await conn.commit()

    return await get_scenario_urban_object_by_id_from_db(conn, scenario_urban_object_id)


async def add_existing_physical_object_to_scenario_in_db(
    conn: AsyncConnection,
    scenario_id: int,
    object_geometry_id: int,
    physical_object: PhysicalObjectsDataPost,
    user_id: str,
) -> ScenarioUrbanObjectDTO:
    """Add existing physical object to scenario."""

    statement = select(scenarios_data).where(scenarios_data.c.scenario_id == scenario_id)
    result = (await conn.execute(statement)).mappings().one_or_none()
    if result is None:
        raise EntityNotFoundById(scenario_id, "scenario")

    statement = select(projects_data).where(projects_data.c.project_id == result.project_id)
    project = (await conn.execute(statement)).mappings().one_or_none()
    if project.user_id != user_id:
        raise AccessDeniedError(result.project_id, "project")

    statement = select(projects_object_geometries_data).where(
        projects_object_geometries_data.c.object_geometry_id == object_geometry_id
    )
    object_geometry = (await conn.execute(statement)).one_or_none()
    if object_geometry is None:
        raise EntityNotFoundById(object_geometry_id, "object geometry")

    statement = select(physical_object_types_dict).where(
        physical_object_types_dict.c.physical_object_type_id == physical_object.physical_object_type_id
    )
    physical_object_type = (await conn.execute(statement)).one_or_none()
    if physical_object_type is None:
        raise EntityNotFoundById(physical_object.physical_object_type_id, "physical object type")

    statement = (
        insert(projects_physical_objects_data)
        .values(
            physical_object_type_id=physical_object.physical_object_type_id,
            name=physical_object.name,
            properties=physical_object.properties,
        )
        .returning(projects_physical_objects_data.c.physical_object_id)
    )
    physical_object_id = (await conn.execute(statement)).scalar_one()

    statement = (
        insert(projects_urban_objects_data)
        .values(physical_object_id=physical_object_id, object_geometry_id=object_geometry_id, scenario_id=scenario_id)
        .returning(projects_urban_objects_data.c.urban_object_id)
    )

    scenario_urban_object_id = (await conn.execute(statement)).scalar_one_or_none()
    await conn.commit()

    return await get_scenario_urban_object_by_id_from_db(conn, scenario_urban_object_id)


async def add_service_to_scenario_in_db(
    conn: AsyncConnection, scenario_id: int, service: ServicesDataPost, user_id: str
) -> ScenarioUrbanObjectDTO:
    """Add service object to scenario."""

    statement = select(scenarios_data).where(scenarios_data.c.scenario_id == scenario_id)
    result = (await conn.execute(statement)).mappings().one_or_none()
    if result is None:
        raise EntityNotFoundById(scenario_id, "scenario")

    statement = select(projects_data).where(projects_data.c.project_id == result.project_id)
    project = (await conn.execute(statement)).mappings().one_or_none()
    if project.user_id != user_id:
        raise AccessDeniedError(result.project_id, "project")

    statement = select(projects_urban_objects_data).where(
        and_(
            projects_urban_objects_data.c.physical_object_id == service.physical_object_id,
            projects_urban_objects_data.c.object_geometry_id == service.object_geometry_id,
        )
    )
    urban_objects = (await conn.execute(statement)).mappings().all()
    if not list(urban_objects):
        raise EntityNotFoundByParams("urban object", service.physical_object_id, service.object_geometry_id)

    statement = select(service_types_dict).where(service_types_dict.c.service_type_id == service.service_type_id)
    service_type = (await conn.execute(statement)).one_or_none()
    if service_type is None:
        raise EntityNotFoundById(service.service_type_id, "service type")

    if service.territory_type_id is not None:
        statement = select(territory_types_dict).where(
            territory_types_dict.c.territory_type_id == service.territory_type_id
        )
        territory_type = (await conn.execute(statement)).one_or_none()
        if territory_type is None:
            raise EntityNotFoundById(service.territory_type_id, "territory type")

    statement = (
        insert(projects_services_data)
        .values(
            service_type_id=service.service_type_id,
            territory_type_id=service.territory_type_id,
            name=service.name,
            capacity_real=service.capacity_real,
            properties=service.properties,
        )
        .returning(projects_services_data.c.service_id)
    )
    service_id = (await conn.execute(statement)).scalar_one()

    flag = False
    for urban_object in urban_objects:
        if urban_object.service_id is None:
            statement = (
                update(projects_urban_objects_data)
                .where(projects_urban_objects_data.c.urban_object_id == urban_object.urban_object_id)
                .values(service_id=service_id, scenario_id=scenario_id)
            ).returning(projects_urban_objects_data.c.urban_object_id)
            flag = True
            break

    if not flag:
        statement = (
            insert(projects_urban_objects_data)
            .values(
                service_id=service_id,
                physical_object_id=service.physical_object_id,
                object_geometry_id=service.object_geometry_id,
                scenario_id=scenario_id,
            )
            .returning(projects_urban_objects_data.c.urban_object_id)
        )

    urban_object_id = (await conn.execute(statement)).scalar_one()
    await conn.commit()

    return await get_scenario_urban_object_by_id_from_db(conn, urban_object_id)


async def add_existing_service_to_scenario_in_db(
    conn: AsyncConnection,
    scenario_id: int,
    service_id: int,
    physical_object_id: int,
    object_geometry_id: int,
    user_id: str,
) -> ScenarioUrbanObjectDTO:
    """Add existing service object to scenario."""

    statement = select(scenarios_data).where(scenarios_data.c.scenario_id == scenario_id)
    result = (await conn.execute(statement)).mappings().one_or_none()
    if result is None:
        raise EntityNotFoundById(scenario_id, "scenario")

    statement = select(projects_data).where(projects_data.c.project_id == result.project_id)
    project = (await conn.execute(statement)).mappings().one_or_none()
    if project.user_id != user_id:
        raise AccessDeniedError(result.project_id, "project")

    statement = select(projects_urban_objects_data).where(
        and_(
            projects_urban_objects_data.c.physical_object_id == physical_object_id,
            projects_urban_objects_data.c.object_geometry_id == object_geometry_id,
        )
    )
    urban_objects = (await conn.execute(statement)).mappings().all()
    if not list(urban_objects):
        raise EntityNotFoundByParams("urban object", physical_object_id, object_geometry_id)

    statement = select(projects_services_data).where(projects_services_data.c.service_id == service_id)
    service = (await conn.execute(statement)).one_or_none()
    if service is None:
        raise EntityNotFoundById(service_id, "service")

    flag = False
    for urban_object in urban_objects:
        if urban_object.service_id is None:
            statement = (
                update(projects_urban_objects_data)
                .where(projects_urban_objects_data.c.urban_object_id == urban_object.urban_object_id)
                .values(service_id=service_id, scenario_id=scenario_id)
                .returning(projects_urban_objects_data.c.urban_object_id)
            )
            flag = True
        if urban_object.service_id == service_id:
            raise EntityAlreadyExists("urban object", physical_object_id, object_geometry_id, service_id)

    if not flag:
        statement = (
            insert(projects_urban_objects_data)
            .values(
                service_id=service_id,
                physical_object_id=physical_object_id,
                object_geometry_id=object_geometry_id,
                scenario_id=scenario_id,
            )
            .returning(projects_urban_objects_data.c.urban_object_id)
        )

    urban_object_id = (await conn.execute(statement)).scalar_one()
    await conn.commit()

    return await get_scenario_urban_object_by_id_from_db(conn, urban_object_id)
