"""Projects internal logic is defined here."""

import asyncio
import os
from collections.abc import Callable
from datetime import date, datetime, timezone
from typing import Any, Literal

import aiohttp
import structlog
from geoalchemy2 import Geography, Geometry
from geoalchemy2.functions import (
    ST_Area,
    ST_AsEWKB,
    ST_Buffer,
    ST_Centroid,
    ST_GeometryType,
    ST_GeomFromWKB,
    ST_Intersection,
    ST_Intersects,
    ST_IsEmpty,
    ST_Within,
)
from otteroad import KafkaProducerClient
from otteroad.models import BaseScenarioCreated, ProjectCreated
from sqlalchemy import (
    BaseRow,
    Integer,
    ScalarSelect,
    and_,
    case,
    cast,
    delete,
    func,
    insert,
    literal,
    or_,
    select,
    text,
    update,
)
from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.common.db.entities import (
    functional_zone_types_dict,
    functional_zones_data,
    object_geometries_data,
    physical_object_types_dict,
    physical_objects_data,
    projects_data,
    projects_functional_zones,
    projects_object_geometries_data,
    projects_physical_objects_data,
    projects_services_data,
    projects_territory_data,
    projects_urban_objects_data,
    scenarios_data,
    territories_data,
    territory_types_dict,
    urban_objects_data, projects_phases_data,
)
from idu_api.urban_api.config import UrbanAPIConfig
from idu_api.urban_api.dto import (
    PageDTO,
    ProjectDTO,
    ProjectPhasesDTO,
    ProjectTerritoryDTO,
    ProjectWithTerritoryDTO,
    ScenarioDTO,
    UserDTO,
)
from idu_api.urban_api.exceptions.logic.common import EntityAlreadyExists, EntityNotFoundById, EntityNotFoundByParams
from idu_api.urban_api.exceptions.logic.projects import NotAllowedInProjectScenario, NotAllowedInRegionalProject
from idu_api.urban_api.exceptions.logic.users import AccessDeniedError
from idu_api.urban_api.logic.impl.helpers.utils import SRID, check_existence, extract_values_from_model
from idu_api.urban_api.minio.services import ProjectStorageManager
from idu_api.urban_api.schemas import (
    ProjectPatch,
    ProjectPost,
    ProjectPut, ProjectPhasesPut,
)
from idu_api.urban_api.utils.pagination import paginate_dto

func: Callable


####################################################################################
#                           Main business-logic                                    #
####################################################################################


async def check_project(
    conn: AsyncConnection,
    project_id: int,
    user: UserDTO | None,
    to_edit: bool = False,
    allow_regional: bool = True,
) -> None:
    """Check project existence and user access."""

    statement = select(projects_data).where(projects_data.c.project_id == project_id)
    result = (await conn.execute(statement)).mappings().one_or_none()
    if result is None:
        raise EntityNotFoundById(project_id, "project")
    if user is None:
        if not result.public:
            raise AccessDeniedError(project_id, "project")
    elif result.user_id != user.id and (not result.public or to_edit) and not user.is_superuser:
        raise AccessDeniedError(project_id, "project")
    if result.is_regional and not allow_regional:
        raise NotAllowedInRegionalProject()


async def get_project_by_id_from_db(conn: AsyncConnection, project_id: int, user: UserDTO | None) -> ProjectDTO:
    """Get project object by identifier."""

    regional_scenarios = scenarios_data.alias("regional_scenarios")
    statement = (
        select(
            projects_data,
            territories_data.c.name.label("territory_name"),
            scenarios_data.c.scenario_id,
            scenarios_data.c.name.label("scenario_name"),
        )
        .select_from(
            projects_data.join(territories_data, territories_data.c.territory_id == projects_data.c.territory_id)
            .outerjoin(
                scenarios_data,
                and_(
                    scenarios_data.c.project_id == projects_data.c.project_id,
                    scenarios_data.c.is_based.is_(True),
                    projects_data.c.is_regional.is_(False),
                ),
            )
            .outerjoin(
                regional_scenarios,
                regional_scenarios.c.scenario_id == scenarios_data.c.parent_id,
            )
        )
        .where(
            projects_data.c.project_id == project_id,
            or_(
                projects_data.c.is_regional.is_(True),
                and_(
                    scenarios_data.c.is_based.is_(True),
                    regional_scenarios.c.is_based.is_(True),
                ),
            ),
        )
    )

    result = (await conn.execute(statement)).mappings().one_or_none()

    if result is None:
        raise EntityNotFoundById(project_id, "project")
    if user is None:
        if not result.public:
            raise AccessDeniedError(project_id, "project")
    elif result.user_id != user.id and not result.public and not user.is_superuser:
        raise AccessDeniedError(project_id, "project")

    return ProjectDTO(**result)


async def get_project_territory_by_id_from_db(
    conn: AsyncConnection, project_id: int, user: UserDTO | None
) -> ProjectTerritoryDTO:
    """Get project territory object by project identifier."""

    await check_project(conn, project_id, user, allow_regional=False)

    regional_scenarios = scenarios_data.alias("regional_scenarios")
    statement = (
        select(
            projects_territory_data.c.project_territory_id,
            projects_data.c.project_id,
            projects_data.c.name.label("project_name"),
            projects_data.c.user_id.label("project_user_id"),
            territories_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
            ST_AsEWKB(projects_territory_data.c.geometry).label("geometry"),
            ST_AsEWKB(projects_territory_data.c.centre_point).label("centre_point"),
            projects_territory_data.c.properties,
            scenarios_data.c.scenario_id,
            scenarios_data.c.name.label("scenario_name"),
        )
        .select_from(
            projects_territory_data.join(
                projects_data, projects_data.c.project_id == projects_territory_data.c.project_id
            )
            .join(territories_data, territories_data.c.territory_id == projects_data.c.territory_id)
            .outerjoin(
                scenarios_data,
                scenarios_data.c.project_id == projects_data.c.project_id,
            )
            .outerjoin(
                regional_scenarios,
                regional_scenarios.c.scenario_id == scenarios_data.c.parent_id,
            )
        )
        .where(
            projects_territory_data.c.project_id == project_id,
            scenarios_data.c.is_based.is_(True),
            regional_scenarios.c.is_based.is_(True),
        )
    )

    result = (await conn.execute(statement)).mappings().one_or_none()
    if result is None:
        raise EntityNotFoundById(project_id, "project territory")

    return ProjectTerritoryDTO(**result)


async def get_project_phases_by_id_from_db(conn, project_id: int, user: UserDTO | None) -> ProjectPhasesDTO:
    """Get all info about project's phases by project identifier."""

    await check_project(conn, project_id, user, allow_regional=False)

    statement = (
        select(
            projects_phases_data.c.actual_start_date,
            projects_phases_data.c.planned_start_date,
            projects_phases_data.c.actual_end_date,
            projects_phases_data.c.planned_end_date,
            projects_phases_data.c.investment,
            projects_phases_data.c.pre_design,
            projects_phases_data.c.design,
            projects_phases_data.c.construction,
            projects_phases_data.c.operation,
            projects_phases_data.c.decommission,
        )
        .where(projects_phases_data.c.project_id == project_id)
    )

    result = (await conn.execute(statement)).mappings().one_or_none()

    return ProjectPhasesDTO(**result)


async def put_project_phases_to_db(
    conn: AsyncConnection, project_id: int, project_phases: ProjectPhasesPut, user: UserDTO
) -> ProjectPhasesDTO:
    """Update all info about project's phases by project identifier."""

    await check_project(conn, project_id, user, to_edit=True, allow_regional=False)

    statement = (
        update(projects_phases_data)
        .where(projects_phases_data.c.project_id == project_id)
        .values(**extract_values_from_model(project_phases))
    )
    await conn.execute(statement)
    await conn.commit()

    return await get_project_phases_by_id_from_db(conn, project_id, user)


async def get_all_projects_from_db(conn: AsyncConnection) -> list[ProjectDTO]:
    """Get all available projects."""

    regional_scenarios = scenarios_data.alias("regional_scenarios")
    statement = (
        select(
            projects_data,
            territories_data.c.name.label("territory_name"),
            scenarios_data.c.scenario_id,
            scenarios_data.c.name.label("scenario_name"),
        )
        .select_from(
            projects_data.join(territories_data, territories_data.c.territory_id == projects_data.c.territory_id)
            .outerjoin(
                scenarios_data,
                scenarios_data.c.project_id == projects_data.c.project_id,
            )
            .outerjoin(
                regional_scenarios,
                regional_scenarios.c.scenario_id == scenarios_data.c.parent_id,
            )
        )
        .where(
            projects_data.c.is_regional.is_(False),
            scenarios_data.c.is_based.is_(True),
            regional_scenarios.c.is_based.is_(True),
        )
        .order_by(projects_data.c.project_id)
    )

    result = (await conn.execute(statement)).mappings().all()
    return [ProjectDTO(**item) for item in result]


async def get_projects_from_db(
    conn: AsyncConnection,
    user: UserDTO | None,
    only_own: bool,
    is_regional: bool,
    project_type: Literal["common", "city"] | None,
    territory_id: int | None,
    name: str | None,
    created_at: date | None,
    order_by: Literal["created_at", "updated_at"] | None,
    ordering: Literal["asc", "desc"] | None,
    paginate: bool = False,
) -> list[ProjectDTO] | PageDTO[ProjectDTO]:
    """Get all public and user's projects."""

    regional_scenarios = scenarios_data.alias("regional_scenarios")
    statement = (
        select(
            projects_data,
            territories_data.c.name.label("territory_name"),
            scenarios_data.c.scenario_id,
            scenarios_data.c.name.label("scenario_name"),
        )
        .select_from(
            projects_data.join(territories_data, territories_data.c.territory_id == projects_data.c.territory_id)
            .outerjoin(
                scenarios_data,
                and_(
                    scenarios_data.c.project_id == projects_data.c.project_id,
                    scenarios_data.c.is_based.is_(True),
                    projects_data.c.is_regional.is_(False),
                ),
            )
            .outerjoin(
                regional_scenarios,
                regional_scenarios.c.scenario_id == scenarios_data.c.parent_id,
            )
        )
        .where(
            projects_data.c.is_regional.is_(is_regional),
            or_(
                projects_data.c.is_regional.is_(True),
                and_(
                    scenarios_data.c.is_based.is_(True),
                    regional_scenarios.c.is_based.is_(True),
                ),
            ),
        )
    )

    if only_own:
        statement = statement.where(projects_data.c.user_id == user.id)
    elif user is not None:
        statement = statement.where(
            (projects_data.c.user_id == user.id) | (projects_data.c.public.is_(True) if not user.is_superuser else True)
        )
    else:
        statement = statement.where(projects_data.c.public.is_(True))

    if project_type == "common":
        statement = statement.where(projects_data.c.is_city.is_(False))
    elif project_type == "city":
        statement = statement.where(projects_data.c.is_city.is_(True))

    if territory_id is not None:
        statement = statement.where(projects_data.c.territory_id == territory_id)
    if name is not None:
        statement = statement.where(projects_data.c.name.ilike(f"%{name}%"))
    if created_at is not None:
        statement = statement.where(func.date(projects_data.c.created_at) >= created_at)

    if order_by is not None:
        order = projects_data.c.created_at if order_by == "created_at" else projects_data.c.updated_at
        if ordering == "desc":
            order = order.desc()
        statement = statement.order_by(order)
    else:
        if ordering == "desc":
            statement = statement.order_by(projects_data.c.project_id.desc())
        else:
            statement = statement.order_by(projects_data.c.project_id)

    if paginate:
        return await paginate_dto(conn, statement, transformer=lambda x: [ProjectDTO(**item) for item in x])

    result = (await conn.execute(statement)).mappings().all()

    return [ProjectDTO(**project) for project in result]


async def get_projects_territories_from_db(
    conn: AsyncConnection,
    user: UserDTO | None,
    only_own: bool,
    project_type: Literal["common", "city"] | None,
    territory_id: int | None,
) -> list[ProjectWithTerritoryDTO]:
    """Get all public and user's project territories."""

    regional_scenarios = scenarios_data.alias("regional_scenarios")
    statement = (
        select(
            projects_data,
            territories_data.c.name.label("territory_name"),
            ST_AsEWKB(projects_territory_data.c.geometry).label("geometry"),
            ST_AsEWKB(projects_territory_data.c.centre_point).label("centre_point"),
            scenarios_data.c.scenario_id,
            scenarios_data.c.name.label("scenario_name"),
        )
        .select_from(
            projects_data.join(territories_data, territories_data.c.territory_id == projects_data.c.territory_id)
            .join(scenarios_data, scenarios_data.c.project_id == projects_data.c.project_id)
            .join(regional_scenarios, regional_scenarios.c.scenario_id == scenarios_data.c.parent_id)
            .join(projects_territory_data, projects_territory_data.c.project_id == projects_data.c.project_id)
        )
        .where(
            scenarios_data.c.is_based.is_(True),
            regional_scenarios.c.is_based.is_(True),
            projects_data.c.is_regional.is_(False),
        )
    )

    if only_own:
        statement = statement.where(projects_data.c.user_id == user.id)
    elif user is not None:
        statement = statement.where(
            (projects_data.c.user_id == user.id) | (projects_data.c.public.is_(True) if not user.is_superuser else True)
        )
    else:
        statement = statement.where(projects_data.c.public.is_(True))

    if project_type == "common":
        statement = statement.where(projects_data.c.is_city.is_(False))
    elif project_type == "city":
        statement = statement.where(projects_data.c.is_city.is_(True))

    if territory_id is not None:
        statement = statement.where(projects_data.c.territory_id == territory_id)

    result = (await conn.execute(statement)).mappings().all()

    return [ProjectWithTerritoryDTO(**project) for project in result]


async def add_project_to_db(
    conn: AsyncConnection,
    project: ProjectPost,
    user: UserDTO,
    kafka_producer: KafkaProducerClient,
    project_storage_manager: ProjectStorageManager,
    logger: structlog.stdlib.BoundLogger,
) -> ProjectDTO:
    """Create project object."""
    if project.is_regional:
        project_id = await insert_project(conn, project, user)
        await conn.commit()
        return await get_project_by_id_from_db(conn, project_id, user)

    given_geometry = select(
        ST_GeomFromWKB(project.territory.geometry.as_shapely_geometry().wkb, text(str(SRID))).label("geometry")
    ).scalar_subquery()

    await add_context_territories(conn, project, given_geometry)

    project_id = await insert_project(conn, project, user)

    project_territory = extract_values_from_model(project.territory)
    await conn.execute(insert(projects_territory_data).values(**project_territory, project_id=project_id))

    scenario_id = await create_project_base_scenario(conn, project_id, project.territory_id)

    id_mapping = await insert_intersecting_geometries(conn, given_geometry)

    await insert_urban_objects(conn, scenario_id, id_mapping)

    await insert_functional_zones(conn, scenario_id, given_geometry)

    await save_indicators(project_id, scenario_id, logger)

    new_project = await get_project_by_id_from_db(conn, project_id, user)

    event = ProjectCreated(project_id=project_id, base_scenario_id=scenario_id, territory_id=project.territory_id)
    await kafka_producer.send(event)

    await project_storage_manager.init_project(project_id, logger)

    await conn.commit()

    return new_project


async def create_base_scenario_to_db(
    conn: AsyncConnection,
    project_id: int,
    scenario_id: int,
    kafka_producer: KafkaProducerClient,
    logger: structlog.stdlib.BoundLogger,
) -> ScenarioDTO:
    """Create base scenario object for given project from specified regional scenario."""

    statement = (
        select(projects_data, projects_territory_data.c.geometry)
        .select_from(
            projects_data.outerjoin(
                projects_territory_data,
                projects_territory_data.c.project_id == projects_data.c.project_id,
            )
        )
        .where(projects_data.c.project_id == project_id)
    )
    project = (await conn.execute(statement)).mappings().one_or_none()
    if project is None:
        raise EntityNotFoundById(project_id, "project")
    if project.is_regional:
        raise NotAllowedInRegionalProject()

    statement = (
        select(scenarios_data, projects_data.c.is_regional)
        .select_from(scenarios_data.join(projects_data, projects_data.c.project_id == scenarios_data.c.project_id))
        .where(scenarios_data.c.scenario_id == scenario_id)
    )
    scenario = (await conn.execute(statement)).mappings().one_or_none()
    if scenario is None:
        raise EntityNotFoundById(scenario_id, "scenario")
    if not scenario.is_regional:
        raise NotAllowedInProjectScenario()

    if await check_existence(
        conn,
        scenarios_data,
        conditions={"project_id": project_id, "parent_id": scenario_id, "is_based": True},
    ):
        raise EntityAlreadyExists("base scenario", project_id, scenario_id)

    base_scenario_id = await create_project_base_scenario(conn, project_id, project.territory_id, parent_id=scenario_id)

    await copy_urban_objects_from_regional_scenario(conn, scenario_id, project.geometry, base_scenario_id)

    id_mapping = await insert_intersecting_geometries(conn, project.geometry)

    await insert_urban_objects(conn, base_scenario_id, id_mapping)

    await insert_functional_zones(conn, base_scenario_id, project.geometry)

    await save_indicators(project_id, base_scenario_id, logger)

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
        .where(scenarios_data.c.scenario_id == base_scenario_id)
    )
    result = (await conn.execute(statement)).mappings().one_or_none()

    event = BaseScenarioCreated(
        project_id=project_id,
        base_scenario_id=base_scenario_id,
        regional_scenario_id=scenario_id,
    )
    await kafka_producer.send(event)

    await conn.commit()

    return ScenarioDTO(**result)


async def put_project_to_db(conn: AsyncConnection, project: ProjectPut, project_id: int, user: UserDTO) -> ProjectDTO:
    """Update project object by all its attributes."""

    await check_project(conn, project_id, user, to_edit=True)

    statement = (
        update(projects_data)
        .where(projects_data.c.project_id == project_id)
        .values(**extract_values_from_model(project, to_update=True))
    )

    await conn.execute(statement)
    await conn.commit()

    return await get_project_by_id_from_db(conn, project_id, user)


async def patch_project_to_db(
    conn: AsyncConnection, project: ProjectPatch, project_id: int, user: UserDTO
) -> ProjectDTO:
    """Patch project object."""

    await check_project(conn, project_id, user, to_edit=True)

    statement_for_project = (
        update(projects_data)
        .where(projects_data.c.project_id == project_id)
        .values(**project.model_dump(exclude_unset=True), updated_at=datetime.now(timezone.utc))
        .returning(projects_data)
    )

    await conn.execute(statement_for_project)
    await conn.commit()

    return await get_project_by_id_from_db(conn, project_id, user)


async def delete_project_from_db(
    conn: AsyncConnection,
    project_id: int,
    project_storage_manager: ProjectStorageManager,
    user: UserDTO,
    logger: structlog.stdlib.BoundLogger,
) -> dict:
    """Delete project object."""

    await check_project(conn, project_id, user, to_edit=True)

    urban_objects = (
        (
            await conn.execute(
                select(
                    projects_urban_objects_data.c.object_geometry_id,
                    projects_urban_objects_data.c.physical_object_id,
                    projects_urban_objects_data.c.service_id,
                )
                .select_from(
                    projects_urban_objects_data.join(
                        scenarios_data,
                        scenarios_data.c.scenario_id == projects_urban_objects_data.c.scenario_id,
                    )
                )
                .where(scenarios_data.c.project_id == project_id)
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

    statement_for_project = delete(projects_data).where(projects_data.c.project_id == project_id)

    await conn.execute(statement_for_project)
    await project_storage_manager.delete_project(project_id, logger)

    await conn.commit()

    return {"status": "ok"}


####################################################################################
#                            Helper functions                                      #
####################################################################################


async def insert_project(conn: AsyncConnection, project: ProjectPost, user: UserDTO) -> int:
    statement = (
        insert(projects_data)
        .values(**project.model_dump(exclude={"territory"}), user_id=user.id)
        .returning(projects_data.c.project_id)
    )
    return (await conn.execute(statement)).scalar_one()


async def add_context_territories(
    conn: AsyncConnection,
    project: ProjectPost,
    geometry: ScalarSelect[Any] | BaseRow,
) -> None:
    """
    Enrich project with contextual territorial data:
    - 'territories': parents of intersecting city territories,
    - 'districts': parents of those parent territories,
    - 'context': parents of city territories in a 3km buffer zone.
    """

    # Ensure the project's base territory exists
    if not await check_existence(conn, territories_data, conditions={"territory_id": project.territory_id}):
        raise EntityNotFoundById(project.territory_id, "territory")

    # Prepare geometry buffer (3 km)
    buffer_meters = 3000
    buffered_geometry = select(
        cast(ST_Buffer(cast(geometry, Geography(srid=SRID)), buffer_meters), Geometry(srid=SRID)).label("geometry")
    ).scalar_subquery()

    region_level = 2

    # Regions intersecting buffered geometry
    intersecting_regions_query = (
        select(territories_data.c.territory_id, territory_types_dict.c.name.label("type_name"))
        .select_from(
            territories_data.join(
                territory_types_dict, territory_types_dict.c.territory_type_id == territories_data.c.territory_type_id
            )
        )
        .where(territories_data.c.level == region_level, ST_Intersects(territories_data.c.geometry, buffered_geometry))
    )

    regions = (await conn.execute(intersecting_regions_query)).mappings().all()

    regular_regions = [r.territory_id for r in regions if r.type_name != "Город федерального значения"]
    federal_cities = [r.territory_id for r in regions if r.type_name == "Город федерального значения"]

    territories, districts, context = [], [], []

    if regular_regions:
        t, d, c = await _extract_regular_regions_context(conn, geometry, buffered_geometry, regular_regions)
        territories += t
        districts += d
        context += c

    if federal_cities:
        t, d, c = await _extract_federal_cities_context(conn, geometry, buffered_geometry, federal_cities)
        territories += t
        districts += d
        context += c

    project.properties.update(
        {
            "territories": territories,
            "districts": districts,
            "context": context,
        }
    )


async def _extract_regular_regions_context(
    conn: AsyncConnection,
    geometry: ScalarSelect[Any],
    buffered_geometry: ScalarSelect[Any],
    region_ids: list[int],
) -> tuple[list[str], list[str], list[int]]:
    """
    Extract context territorial data from regular regions:
    - 'territories': parents of intersecting city territories,
    - 'districts': parents of those parent territories,
    - 'context': parents of city territories in a 3km buffer zone.
    """

    # Recursive CTE for hierarchy under regions
    base_cte = (
        select(
            territories_data.c.territory_id,
            territories_data.c.parent_id,
            territories_data.c.name,
            territories_data.c.is_city,
            territories_data.c.level,
            territories_data.c.geometry,
        )
        .where(territories_data.c.parent_id.in_(region_ids))
        .cte(name="territories_cte", recursive=True)
    )
    recursive_cte = select(
        territories_data.c.territory_id,
        territories_data.c.parent_id,
        territories_data.c.name,
        territories_data.c.is_city,
        territories_data.c.level,
        territories_data.c.geometry,
    ).where(territories_data.c.parent_id == base_cte.c.territory_id)
    territories_cte = base_cte.union_all(recursive_cte)

    # Cities below regions
    city_children = (
        select(territories_cte.c.parent_id)
        .where(territories_cte.c.level > 3, territories_cte.c.is_city.is_(True))
        .cte(name="city_children")
    )

    # Parent territories of city territories
    parent_territory_ids = select(city_children.c.parent_id).distinct()

    # Districts (parents of those parent territories)
    district_ids = (
        select(territories_cte.c.parent_id)
        .where(
            territories_cte.c.territory_id.in_(parent_territory_ids),
        )
        .distinct()
    )

    # Final selections
    territories_query = select(territories_cte.c.name).where(
        territories_cte.c.territory_id.in_(parent_territory_ids),
        ST_Intersects(territories_cte.c.geometry, geometry),
        territories_cte.c.is_city.is_(False),
    )
    districts_query = select(territories_cte.c.name).where(
        territories_cte.c.territory_id.in_(district_ids),
        ST_Intersects(territories_cte.c.geometry, geometry),
        territories_cte.c.is_city.is_(False),
    )
    context_query = select(territories_cte.c.territory_id).where(
        territories_cte.c.territory_id.in_(parent_territory_ids),
        ST_Intersects(territories_cte.c.geometry, buffered_geometry),
        territories_cte.c.is_city.is_(False),
    )

    # Execute all queries concurrently
    t_res, d_res, c_res = await asyncio.gather(
        conn.execute(territories_query), conn.execute(districts_query), conn.execute(context_query)
    )

    t_res = list(t_res.scalars())
    d_res = list(d_res.scalars())
    c_res = list(c_res.scalars())

    # Fallback: if no districts — treat territories as districts
    if not d_res:
        d_res, t_res = t_res, []

    return t_res, d_res, c_res


async def _extract_federal_cities_context(
    conn: AsyncConnection,
    geometry: ScalarSelect[Any],
    buffered_geometry: ScalarSelect[Any],
    region_ids: list[int],
) -> tuple[list[str], list[str], list[int]]:
    """
    Extract context territorial data from subject regions:
    - 'territories': territories with type name `Муниципальное образование`,
    - 'districts': territories with type name `Район`,
    - 'context': territories with type name `Муниципальное образование` in a 3km buffer zone.
    """
    base_cte = (
        select(
            territories_data.c.territory_id,
            territories_data.c.territory_type_id,
            territories_data.c.parent_id,
            territories_data.c.name,
            territories_data.c.geometry,
        )
        .where(territories_data.c.parent_id.in_(region_ids))
        .cte(name="territories_cte", recursive=True)
    )
    recursive_cte = select(
        territories_data.c.territory_id,
        territories_data.c.territory_type_id,
        territories_data.c.parent_id,
        territories_data.c.name,
        territories_data.c.geometry,
    ).where(territories_data.c.parent_id == base_cte.c.territory_id)
    territories_cte = base_cte.union_all(recursive_cte)

    base_query = (
        select(territories_cte.c.name)
        .select_from(
            territories_cte.join(
                territory_types_dict, territory_types_dict.c.territory_type_id == territories_cte.c.territory_type_id
            )
        )
        .where(ST_Intersects(territories_cte.c.geometry, geometry))
    )
    territories_query = base_query.where(territory_types_dict.c.name == "Муниципальное образование")
    districts_query = base_query.where(territory_types_dict.c.name == "Район")
    context_query = (
        select(territories_cte.c.territory_id)
        .select_from(
            territories_cte.join(
                territory_types_dict, territory_types_dict.c.territory_type_id == territories_cte.c.territory_type_id
            )
        )
        .where(
            territory_types_dict.c.name == "Муниципальное образование",
            ST_Intersects(territories_cte.c.geometry, buffered_geometry),
        )
    )

    t_res, d_res, c_res = await asyncio.gather(
        conn.execute(territories_query),
        conn.execute(districts_query),
        conn.execute(context_query),
    )

    return list(t_res.scalars()), list(d_res.scalars()), list(c_res.scalars())


async def create_project_base_scenario(
    conn: AsyncConnection,
    project_id: int,
    territory_id: int,
    parent_id: int | None = None,
) -> int:
    """
    Create a base scenario for a given project.

    If `parent_id` is not provided, the function will attempt to find a regional base scenario
    associated with the same territory. If no such scenario exists, an error will be raised.

    Args:
        conn (AsyncConnection): Active database connection.
        project_id (int): ID of the current project.
        territory_id (int): ID of the territory the scenario is related to.
        parent_id (int | None): Optional ID of the parent (regional) scenario.

    Returns:
        int: ID of the newly created scenario.

    Raises:
        EntityNotFoundByParams: If no regional base scenario is found when expected.
    """

    # If no parent scenario ID is explicitly provided, try to find the appropriate one.
    if parent_id is None:
        # Look for an existing regional base scenario for the same territory.
        parent_id = (
            await conn.execute(
                select(scenarios_data.c.scenario_id)
                .select_from(
                    scenarios_data.join(projects_data, projects_data.c.project_id == scenarios_data.c.project_id)
                )
                .where(
                    projects_data.c.territory_id == territory_id,
                    projects_data.c.is_regional.is_(True),  # Regional projects only.
                    scenarios_data.c.is_based.is_(True),  # Must be a base scenario.
                )
            )
        ).scalar_one_or_none()

        # If no parent scenario is found, raise an informative error.
        if parent_id is None:
            raise EntityNotFoundByParams("parent regional scenario", territory_id)

    # Insert a new base scenario for the user project with the found or given parent.
    scenario_id = (
        await conn.execute(
            insert(scenarios_data)
            .values(
                project_id=project_id,
                functional_zone_type_id=None,  # Base scenario doesn't have a predefined profile type.
                name="Исходный пользовательский сценарий",  # Default name (in Russian).
                is_based=True,
                parent_id=parent_id,
            )
            .returning(scenarios_data.c.scenario_id)
        )
    ).scalar_one()

    # Return the ID of the newly created scenario.
    return scenario_id


async def copy_urban_objects_from_regional_scenario(
    conn: AsyncConnection, scenario_id: int, geometry: ScalarSelect[Any] | BaseRow, new_scenario_id: int
):
    regional_urban_objects_cte = (
        select(
            projects_urban_objects_data.c.public_urban_object_id,
            projects_urban_objects_data.c.object_geometry_id,
            projects_urban_objects_data.c.physical_object_id,
            projects_urban_objects_data.c.service_id,
            projects_urban_objects_data.c.public_object_geometry_id,
            projects_urban_objects_data.c.public_physical_object_id,
            projects_urban_objects_data.c.public_service_id,
        )
        .select_from(
            projects_urban_objects_data.outerjoin(
                projects_object_geometries_data,
                projects_object_geometries_data.c.object_geometry_id
                == projects_urban_objects_data.c.object_geometry_id,
            ).outerjoin(
                object_geometries_data,
                object_geometries_data.c.object_geometry_id == projects_urban_objects_data.c.public_object_geometry_id,
            )
        )
        .where(
            projects_urban_objects_data.c.scenario_id == scenario_id,
            ST_Intersects(projects_object_geometries_data.c.geometry, geometry)
            | ST_Intersects(object_geometries_data.c.geometry, geometry),
        )
    ).cte(name="regional_urban_objects")

    results = (await conn.execute(select(regional_urban_objects_cte))).mappings().all()

    geom_ids = {r.object_geometry_id for r in results if r.object_geometry_id}
    public_geom_ids = {r.public_object_geometry_id for r in results if r.public_object_geometry_id}

    phys_ids = {r.physical_object_id for r in results if r.physical_object_id}
    svc_ids = {r.service_id for r in results if r.service_id}

    geom_map = await copy_geometries(conn, sorted(list(geom_ids)), geometry)
    public_geom_map = await copy_geometries(conn, sorted(list(public_geom_ids)), geometry, is_from_public=True)
    phys_map, svc_map = await asyncio.gather(
        copy_physical_objects(conn, sorted(list(phys_ids))),
        copy_services(conn, sorted(list(svc_ids))),
    )

    def build_case(col, mapping, default=None):
        return (
            case(*[(col == k, literal(v)) for k, v in mapping.items()], else_=literal(default))
            if mapping
            else literal(default)
        )

    await conn.execute(
        insert(projects_urban_objects_data).from_select(
            ["scenario_id", "public_urban_object_id"],
            select(
                literal(new_scenario_id).label("scenario_id"), regional_urban_objects_cte.c.public_urban_object_id
            ).where(regional_urban_objects_cte.c.public_urban_object_id.isnot(None)),
        )
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
                literal(new_scenario_id).label("scenario_id"),
                case(
                    (
                        regional_urban_objects_cte.c.public_object_geometry_id.isnot(None),
                        cast(
                            build_case(regional_urban_objects_cte.c.public_object_geometry_id, public_geom_map), Integer
                        ),
                    ),
                    else_=cast(build_case(regional_urban_objects_cte.c.object_geometry_id, geom_map), Integer),
                ),
                build_case(regional_urban_objects_cte.c.physical_object_id, phys_map),
                build_case(regional_urban_objects_cte.c.service_id, svc_map),
                literal(None),
                regional_urban_objects_cte.c.public_physical_object_id,
                regional_urban_objects_cte.c.public_service_id,
            ).where(regional_urban_objects_cte.c.public_urban_object_id.is_(None)),
        )
    )


async def insert_intersecting_geometries(
    conn: AsyncConnection,
    geometry: ScalarSelect[Any] | BaseRow,
) -> dict[int, int]:
    """
    Identify geometries that intersect the given geometry, crop them,
    and insert the resulting geometries into the `user_projects` schema.

    The function filters geometries that intersect (but are not fully within) the given geometry
    and have an overlapping area greater than a minimum threshold. It excludes building-type objects.
    After inserting the cropped geometries, it returns a mapping from the public object geometry IDs
    to their corresponding new project-specific geometry IDs.

    Args:
        conn (AsyncConnection): Active asynchronous database connection.
        geometry (ScalarSelect[Any]): Geometry used as a mask to crop intersecting geometries.

    Returns:
        dict[int, int]: A mapping of public object geometry IDs to inserted project object geometry IDs.
    """

    area_percent = 0.01  # Minimum intersection threshold as a fraction of original area.

    # Step 1: Identify object geometries that intersect the given geometry
    # but are not fully within it, and have at least 1% overlapping area.
    # Also exclude objects of type "building".
    objects_intersecting_cte = (
        select(object_geometries_data.c.object_geometry_id)
        .select_from(
            object_geometries_data.join(
                urban_objects_data,
                urban_objects_data.c.object_geometry_id == object_geometries_data.c.object_geometry_id,
            )
            .join(
                physical_objects_data,
                physical_objects_data.c.physical_object_id == urban_objects_data.c.physical_object_id,
            )
            .join(
                physical_object_types_dict,
                physical_object_types_dict.c.physical_object_type_id == physical_objects_data.c.physical_object_type_id,
            )
        )
        .where(
            ST_Intersects(object_geometries_data.c.geometry, geometry),
            ~ST_Within(object_geometries_data.c.geometry, geometry),
            ST_Area(ST_Intersection(object_geometries_data.c.geometry, geometry))
            >= area_percent * ST_Area(object_geometries_data.c.geometry),
            ~physical_object_types_dict.c.name.ilike("%здание%"),  # Exclude buildings
        )
        .distinct()
        .cte("objects_intersecting")
    )

    # Step 2: Crop geometries using the given geometry and insert the cropped
    # versions into the `user_projects.object_geometries_data` table.
    insert_stmt = (
        insert(projects_object_geometries_data)
        .from_select(
            [
                projects_object_geometries_data.c.public_object_geometry_id,
                projects_object_geometries_data.c.territory_id,
                projects_object_geometries_data.c.geometry,
                projects_object_geometries_data.c.centre_point,
                projects_object_geometries_data.c.address,
                projects_object_geometries_data.c.osm_id,
            ],
            select(
                object_geometries_data.c.object_geometry_id.label("public_object_geometry_id"),
                object_geometries_data.c.territory_id,
                ST_Intersection(object_geometries_data.c.geometry, geometry).label("geometry"),
                ST_Centroid(ST_Intersection(object_geometries_data.c.geometry, geometry)).label("centre_point"),
                object_geometries_data.c.address,
                object_geometries_data.c.osm_id,
            ).where(object_geometries_data.c.object_geometry_id.in_(select(objects_intersecting_cte))),
        )
        .returning(
            projects_object_geometries_data.c.object_geometry_id,
            projects_object_geometries_data.c.public_object_geometry_id,
        )
    )

    # Step 3: Execute the insertion and build a mapping of public IDs to new project-specific IDs.
    result = await conn.execute(insert_stmt)
    return {row.public_object_geometry_id: row.object_geometry_id for row in result.mappings().all()}


async def insert_urban_objects(conn: AsyncConnection, scenario_id: int, id_mapping: dict[int, int]) -> None:
    """
    Insert cropped urban objects into the `user_projects.urban_objects_data` table for a specific scenario.

    This function does the following:
    1. Selects urban objects related to the provided geometry mapping.
    2. Inserts both public and detailed data using `INSERT FROM SELECT` directly,
       applying the provided geometry ID mapping in a SQL-compliant way.

    Args:
        conn (AsyncConnection): Asynchronous database connection.
        scenario_id (int): The scenario to which the urban objects should be linked.
        id_mapping (dict[int, int]): Mapping from original object_geometry_id to newly inserted project-specific geometry_id.
    """

    if not id_mapping:
        return

    # Step 1: Common base query for selecting matched urban objects and joining mapping
    base_query_cte = (
        select(
            urban_objects_data.c.urban_object_id.label("public_urban_object_id"),
            urban_objects_data.c.physical_object_id.label("public_physical_object_id"),
            urban_objects_data.c.service_id.label("public_service_id"),
            projects_object_geometries_data.c.object_geometry_id.label("object_geometry_id"),
        )
        .select_from(
            urban_objects_data.join(
                projects_object_geometries_data,
                urban_objects_data.c.object_geometry_id == projects_object_geometries_data.c.public_object_geometry_id,
            )
        )
        .where(urban_objects_data.c.object_geometry_id.in_(id_mapping.keys()))
    ).cte(name="base_query")

    # Step 2: Insert basic public urban object references
    insert_public = insert(projects_urban_objects_data).from_select(
        ["public_urban_object_id", "scenario_id"],
        select(
            base_query_cte.c.public_urban_object_id,
            literal(scenario_id).label("scenario_id"),
        ),
    )

    # Step 3: Insert detailed urban object data with mapped geometries
    insert_detailed = insert(projects_urban_objects_data).from_select(
        ["public_physical_object_id", "public_service_id", "object_geometry_id", "scenario_id"],
        select(
            base_query_cte.c.public_physical_object_id,
            base_query_cte.c.public_service_id,
            base_query_cte.c.object_geometry_id,
            literal(scenario_id).label("scenario_id"),
        ),
    )

    # Execute inserts
    await conn.execute(insert_public)
    await conn.execute(insert_detailed)


async def insert_functional_zones(conn: AsyncConnection, scenario_id: int, geometry: ScalarSelect[Any]) -> None:
    """
    Insert functional zones intersecting the given geometry into the specified scenario.

    This function performs the following steps:
    1. Finds functional zones from the `functional_zones_data` table that intersect with the given geometry.
    2. Computes the intersection geometry using ST_Intersection.
    3. Filters out invalid geometries (empty or non-polygonal).
    4. Inserts the resulting intersected zones into the `projects_functional_zones` table for the specified scenario.

    Args:
        conn (AsyncConnection): Asynchronous database connection.
        scenario_id (int): Target scenario ID to associate the functional zones with.
        geometry (ScalarSelect[Any]): A subquery returning the project's geometry to intersect with.
    """

    # Step 1: Define the SELECT statement to fetch intersecting functional zones
    intersecting_zones_select = select(
        literal(scenario_id).label("scenario_id"),
        functional_zones_data.c.functional_zone_type_id,
        ST_Intersection(functional_zones_data.c.geometry, geometry).label("geometry"),
        functional_zones_data.c.year,
        functional_zones_data.c.source,
        functional_zones_data.c.properties,
    ).where(
        # Must intersect with the given geometry
        ST_Intersects(functional_zones_data.c.geometry, geometry),
        # Filter only valid polygonal geometries
        ST_GeometryType(ST_Intersection(functional_zones_data.c.geometry, geometry)).in_(
            ("ST_Polygon", "ST_MultiPolygon")
        ),
        # Ensure geometry is not empty
        ~ST_IsEmpty(ST_Intersection(functional_zones_data.c.geometry, geometry)),
    )

    # Step 2: Insert results into the `user_projects.functional_zones` table using INSERT FROM SELECT
    await conn.execute(
        insert(projects_functional_zones).from_select(
            [
                projects_functional_zones.c.scenario_id,
                projects_functional_zones.c.functional_zone_type_id,
                projects_functional_zones.c.geometry,
                projects_functional_zones.c.year,
                projects_functional_zones.c.source,
                projects_functional_zones.c.properties,
            ],
            intersecting_zones_select,
        )
    )


async def save_indicators(project_id: int, scenario_id: int, logger: structlog.stdlib.BoundLogger) -> None:
    """
    Update all indicators for the specified project via the external Hextech service.

    This function interacts with the Hextech API to save indicators related to a specific project and scenario.
    It handles different exceptions and logs relevant information in case of errors.

    Args:
        project_id (int): The ID of the project for which indicators are being saved.
        scenario_id (int): The ID of the scenario within the project.
        logger (structlog.stdlib.BoundLogger): The logger used to record warning and error logs.
    """

    # Load configuration settings from a file or default environment variables
    config = UrbanAPIConfig.from_file_or_default(os.getenv("CONFIG_PATH"))
    params = {"scenario_id": scenario_id, "project_id": project_id, "background": "true"}

    # Create an asynchronous HTTP client session
    async with aiohttp.ClientSession() as session:
        try:
            response = await session.put(
                f"{config.external.hextech_api}/hextech/indicators_saving/save_all", params=params
            )
            response.raise_for_status()

        # Handle errors related to the response (e.g., 4xx or 5xx errors)
        except aiohttp.ClientResponseError as exc:
            await logger.awarning(
                "Failed to save indicators",
                status=exc.status,
                message=exc.message,
                url=exc.request_info.url,
                params=params,
            )

        # Handle connection errors (e.g., network issues)
        except aiohttp.ClientConnectorError as exc:
            await logger.awarning("Request failed due to connection error", reason=str(exc), params=params)

        # Handle any other unexpected exceptions
        except Exception:  # pylint: disable=broad-exception-caught
            await logger.aexception("Unexpected error occurred while saving indicators")


async def copy_geometries(
    conn,
    geometry_ids: list[int],
    geometry: ScalarSelect[Any] | BaseRow | None = None,
    is_from_public: bool = False,
) -> dict[int, int]:
    if not geometry_ids:
        return {}

    if is_from_public:
        table = object_geometries_data
        id_column = table.c.object_geometry_id
    else:
        table = projects_object_geometries_data
        id_column = table.c.object_geometry_id

    statement = (
        select(
            (
                table.c.public_object_geometry_id
                if not is_from_public
                else table.c.object_geometry_id.label("public_object_geometry_id")
            ),
            table.c.territory_id,
            table.c.address,
            table.c.osm_id,
            ST_Intersection(table.c.geometry, geometry).label("geometry") if geometry else table.c.geometry,
            (
                ST_Centroid(ST_Intersection(table.c.geometry, geometry)).label("centre_point")
                if geometry
                else table.c.centre_point
            ),
        )
        .where(id_column.in_(geometry_ids))
        .order_by(id_column)
    )

    old_geometries = (await conn.execute(statement)).mappings().all()
    old_geometries = [dict(row) for row in old_geometries]

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
