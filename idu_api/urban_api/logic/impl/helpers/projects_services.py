"""Projects services internal logic is defined here."""

from collections import defaultdict
from datetime import datetime, timezone

from geoalchemy2.functions import ST_Intersects, ST_Union, ST_Within
from sqlalchemy import delete, insert, literal, or_, select, update
from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.common.db.entities import (
    object_geometries_data,
    projects_data,
    projects_object_geometries_data,
    projects_services_data,
    projects_territory_data,
    projects_urban_objects_data,
    scenarios_data,
    service_types_dict,
    services_data,
    territories_data,
    territory_types_dict,
    urban_functions_dict,
    urban_objects_data,
)
from idu_api.urban_api.dto import (
    ScenarioServiceDTO,
    ScenarioUrbanObjectDTO,
    ServiceDTO,
)
from idu_api.urban_api.exceptions.logic.common import EntityAlreadyExists, EntityNotFoundById, EntityNotFoundByParams
from idu_api.urban_api.exceptions.logic.users import AccessDeniedError
from idu_api.urban_api.logic.impl.helpers.projects_urban_objects import get_scenario_urban_object_by_id_from_db
from idu_api.urban_api.schemas import ScenarioServicePost, ServicesDataPatch, ServicesDataPut


async def get_services_by_scenario_id(
    conn: AsyncConnection,
    scenario_id: int,
    user_id: str,
    service_type_id: int | None,
    urban_function_id: int | None,
) -> list[ScenarioServiceDTO]:
    """Get services by scenario identifier."""

    statement = select(scenarios_data.c.project_id).where(scenarios_data.c.scenario_id == scenario_id)
    project_id = (await conn.execute(statement)).scalar_one_or_none()
    if project_id is None:
        raise EntityNotFoundById(scenario_id, "scenario")

    statement = select(projects_data).where(projects_data.c.project_id == project_id)
    project = (await conn.execute(statement)).mappings().one_or_none()
    if project.user_id != user_id:
        raise AccessDeniedError(project_id, "project")

    project_geometry = (
        select(projects_territory_data.c.geometry).where(projects_territory_data.c.project_id == project.project_id)
    ).alias("project_geometry")

    # Шаг 1: Получить все public_urban_object_id для данного scenario_id
    public_urban_object_ids = (
        select(projects_urban_objects_data.c.public_urban_object_id)
        .where(projects_urban_objects_data.c.scenario_id == scenario_id)
        .where(projects_urban_objects_data.c.public_urban_object_id.isnot(None))
    ).alias("public_urban_object_ids")

    # Шаг 2: Собрать все записи из public.urban_objects_data по собранным public_urban_object_id
    public_urban_objects_query = (
        select(
            services_data,
            service_types_dict.c.urban_function_id,
            urban_functions_dict.c.name.label("urban_function_name"),
            service_types_dict.c.name.label("service_type_name"),
            service_types_dict.c.capacity_modeled.label("service_type_capacity_modeled"),
            service_types_dict.c.code.label("service_type_code"),
            service_types_dict.c.infrastructure_type,
            service_types_dict.c.properties.label("service_type_properties"),
            territory_types_dict.c.name.label("territory_type_name"),
        )
        .select_from(
            urban_objects_data.join(
                object_geometries_data,
                object_geometries_data.c.object_geometry_id == urban_objects_data.c.object_geometry_id,
            )
            .join(services_data, services_data.c.service_id == urban_objects_data.c.service_id)
            .join(
                service_types_dict,
                service_types_dict.c.service_type_id == services_data.c.service_type_id,
            )
            .outerjoin(
                territory_types_dict,
                territory_types_dict.c.territory_type_id == services_data.c.territory_type_id,
            )
            .join(
                urban_functions_dict,
                urban_functions_dict.c.urban_function_id == service_types_dict.c.urban_function_id,
            )
        )
        .where(
            urban_objects_data.c.urban_object_id.not_in(select(public_urban_object_ids)),
            ST_Within(object_geometries_data.c.geometry, select(project_geometry).scalar_subquery()),
        )
    )

    # Условия фильтрации для public объектов
    if service_type_id is not None:
        public_urban_objects_query = public_urban_objects_query.where(
            services_data.c.service_type_id == service_type_id
        )
    if urban_function_id is not None:
        public_urban_objects_query = public_urban_objects_query.where(
            service_types_dict.c.urban_function_id == urban_function_id
        )

    # Получаем все объекты из public.urban_objects_data
    public_objects = []
    for row in (await conn.execute(public_urban_objects_query)).mappings().all():
        public_objects.append(
            {
                "service_id": row.service_id,
                "service_type_id": row.service_type_id,
                "service_type_name": row.service_type_name,
                "urban_function_id": row.urban_function_id,
                "urban_function_name": row.urban_function_name,
                "service_type_capacity_modeled": row.service_type_capacity_modeled,
                "service_type_code": row.service_type_code,
                "infrastructure_type": row.infrastructure_type,
                "service_type_properties": row.service_type_properties,
                "territory_type_id": row.territory_type_id,
                "territory_type_name": row.territory_type_name,
                "name": row.name,
                "capacity_real": row.capacity_real,
                "properties": row.properties,
                "created_at": row.created_at,
                "updated_at": row.updated_at,
                "is_scenario_object": False,
            }
        )

    # Шаг 3: Собрать все записи из user_projects.urban_objects_data для данного сценария
    scenario_urban_objects_query = (
        select(
            projects_services_data.c.service_id,
            projects_services_data.c.name,
            projects_services_data.c.capacity_real,
            projects_services_data.c.properties,
            projects_services_data.c.created_at,
            projects_services_data.c.updated_at,
            services_data.c.service_id.label("public_service_id"),
            services_data.c.name.label("public_name"),
            services_data.c.capacity_real.label("public_capacity_real"),
            services_data.c.properties.label("public_properties"),
            services_data.c.created_at.label("public_created_at"),
            services_data.c.updated_at.label("public_updated_at"),
            service_types_dict.c.service_type_id,
            service_types_dict.c.urban_function_id,
            urban_functions_dict.c.name.label("urban_function_name"),
            service_types_dict.c.name.label("service_type_name"),
            service_types_dict.c.capacity_modeled.label("service_type_capacity_modeled"),
            service_types_dict.c.code.label("service_type_code"),
            service_types_dict.c.infrastructure_type,
            service_types_dict.c.properties.label("service_type_properties"),
            territory_types_dict.c.territory_type_id,
            territory_types_dict.c.name.label("territory_type_name"),
        )
        .select_from(
            projects_urban_objects_data.outerjoin(
                projects_object_geometries_data,
                projects_object_geometries_data.c.object_geometry_id
                == projects_urban_objects_data.c.object_geometry_id,
            )
            .outerjoin(
                projects_services_data, projects_services_data.c.service_id == projects_urban_objects_data.c.service_id
            )
            .outerjoin(
                object_geometries_data,
                object_geometries_data.c.object_geometry_id == projects_urban_objects_data.c.public_object_geometry_id,
            )
            .outerjoin(services_data, services_data.c.service_id == projects_urban_objects_data.c.public_service_id)
            .outerjoin(
                service_types_dict,
                or_(
                    service_types_dict.c.service_type_id == projects_services_data.c.service_type_id,
                    service_types_dict.c.service_type_id == services_data.c.service_type_id,
                ),
            )
            .outerjoin(
                territory_types_dict,
                or_(
                    territory_types_dict.c.territory_type_id == projects_services_data.c.territory_type_id,
                    territory_types_dict.c.territory_type_id == services_data.c.territory_type_id,
                ),
            )
            .outerjoin(
                urban_functions_dict,
                urban_functions_dict.c.urban_function_id == service_types_dict.c.urban_function_id,
            )
        )
        .where(projects_urban_objects_data.c.scenario_id == scenario_id)
        .where(projects_urban_objects_data.c.public_urban_object_id.is_(None))
    )

    # Условия фильтрации для объектов user_projects
    if service_type_id:
        scenario_urban_objects_query = scenario_urban_objects_query.where(
            (projects_services_data.c.service_type_id == service_type_id)
            | (services_data.c.service_type_id == service_type_id)
        )
    if urban_function_id is not None:
        scenario_urban_objects_query = scenario_urban_objects_query.where(
            service_types_dict.c.urban_function_id == urban_function_id
        )

    # Получаем все объекты из user_projects.urban_objects_data
    scenario_objects = []
    for row in (await conn.execute(scenario_urban_objects_query)).mappings().all():
        is_scenario_service = row.service_id is not None and row.public_service_id is None
        if row.service_id is not None and row.public_service_id is not None:
            scenario_objects.append(
                {
                    "service_id": row.service_id or row.public_service_id,
                    "service_type_id": row.service_type_id,
                    "service_type_name": row.service_type_name,
                    "urban_function_id": row.urban_function_id,
                    "urban_function_name": row.urban_function_name,
                    "service_type_capacity_modeled": row.service_type_capacity_modeled,
                    "service_type_code": row.service_type_code,
                    "infrastructure_type": row.infrastructure_type,
                    "service_type_properties": row.service_type_properties,
                    "territory_type_id": row.territory_type_id,
                    "territory_type_name": row.territory_type_name,
                    "name": row.name if is_scenario_service else row.public_name,
                    "capacity_real": row.capacity_real if is_scenario_service else row.public_capacity_real,
                    "properties": row.properties if is_scenario_service else row.public_properties,
                    "created_at": row.created_at if is_scenario_service else row.public_created_at,
                    "updated_at": row.updated_at if is_scenario_service else row.public_updated_at,
                    "is_scenario_object": is_scenario_service,
                }
            )

    grouped_objects = defaultdict(dict)
    for obj in public_objects + scenario_objects:
        service_id = obj["service_id"]
        is_scenario_geometry = obj["is_scenario_object"]

        # Проверка и добавление сервиса
        existing_entry = grouped_objects.get(service_id)
        if existing_entry is None:
            grouped_objects[service_id].update(obj)
        elif existing_entry.get("is_scenario_object") != is_scenario_geometry:
            grouped_objects[-service_id].update(obj)

    return [ScenarioServiceDTO(**row) for row in grouped_objects.values()]


async def get_context_services_by_scenario_id_from_db(
    conn: AsyncConnection,
    scenario_id: int,
    user_id: str,
    service_type_id: int | None,
    urban_function_id: int | None,
) -> list[ServiceDTO]:
    """Get list of services for 'context' of the project territory."""

    statement = select(scenarios_data.c.project_id).where(scenarios_data.c.scenario_id == scenario_id)
    project_id = (await conn.execute(statement)).scalar_one_or_none()
    if project_id is None:
        raise EntityNotFoundById(scenario_id, "scenario")

    statement = select(projects_data).where(projects_data.c.project_id == project_id)
    project = (await conn.execute(statement)).mappings().one_or_none()
    if project.user_id != user_id:
        raise AccessDeniedError(project_id, "project")

    context_territories = select(
        territories_data.c.territory_id,
        territories_data.c.geometry,
    ).where(territories_data.c.territory_id.in_(project.properties["context"]))
    unified_geometry = select(ST_Union(context_territories.c.geometry)).scalar_subquery()
    all_descendants = (
        select(
            territories_data.c.territory_id,
            territories_data.c.parent_id,
        )
        .where(territories_data.c.territory_id.in_(select(context_territories.c.territory_id)))
        .cte(name="all_descendants", recursive=True)
    )
    all_descendants = all_descendants.union_all(
        select(
            territories_data.c.territory_id,
            territories_data.c.parent_id,
        ).select_from(
            territories_data.join(
                all_descendants,
                territories_data.c.parent_id == all_descendants.c.territory_id,
            )
        )
    )
    all_ancestors = (
        select(
            territories_data.c.territory_id,
            territories_data.c.parent_id,
        )
        .where(territories_data.c.territory_id.in_(select(context_territories.c.territory_id)))
        .cte(name="all_ancestors", recursive=True)
    )
    all_ancestors = all_ancestors.union_all(
        select(
            territories_data.c.territory_id,
            territories_data.c.parent_id,
        ).select_from(
            territories_data.join(
                all_ancestors,
                territories_data.c.territory_id == all_ancestors.c.parent_id,
            )
        )
    )
    all_related_territories = (
        select(all_descendants.c.territory_id).union(select(all_ancestors.c.territory_id)).subquery()
    )

    objects_intersecting = (
        select(object_geometries_data.c.object_geometry_id)
        .where(
            object_geometries_data.c.territory_id.in_(select(all_related_territories)),
            ST_Intersects(object_geometries_data.c.geometry, unified_geometry),
        )
        .subquery()
    )

    # Step 2. Find all the services in `public` schema for `intersecting_territories`
    statement = (
        select(
            services_data,
            service_types_dict.c.urban_function_id,
            urban_functions_dict.c.name.label("urban_function_name"),
            service_types_dict.c.name.label("service_type_name"),
            service_types_dict.c.capacity_modeled.label("service_type_capacity_modeled"),
            service_types_dict.c.code.label("service_type_code"),
            service_types_dict.c.infrastructure_type,
            service_types_dict.c.properties.label("service_type_properties"),
            territory_types_dict.c.name.label("territory_type_name"),
        )
        .select_from(
            urban_objects_data.join(
                object_geometries_data,
                object_geometries_data.c.object_geometry_id == urban_objects_data.c.object_geometry_id,
            )
            .join(services_data, services_data.c.service_id == urban_objects_data.c.service_id)
            .join(
                service_types_dict,
                service_types_dict.c.service_type_id == services_data.c.service_type_id,
            )
            .outerjoin(
                territory_types_dict,
                territory_types_dict.c.territory_type_id == services_data.c.territory_type_id,
            )
            .join(
                urban_functions_dict,
                urban_functions_dict.c.urban_function_id == service_types_dict.c.urban_function_id,
            )
        )
        .where(object_geometries_data.c.object_geometry_id.in_(select(objects_intersecting)))
        .distinct()
    )

    # Условия фильтрации для public объектов
    if service_type_id is not None:
        statement = statement.where(services_data.c.service_type_id == service_type_id)
    if urban_function_id is not None:
        statement = statement.where(service_types_dict.c.urban_function_id == urban_function_id)

    result = (await conn.execute(statement)).mappings().all()

    return [ServiceDTO(**row) for row in result]


async def add_service_to_db(
    conn: AsyncConnection, service: ScenarioServicePost, scenario_id: int, user_id: str
) -> ScenarioUrbanObjectDTO:
    """Create scenario service object."""

    statement = select(scenarios_data.c.project_id).where(scenarios_data.c.scenario_id == scenario_id)
    project_id = (await conn.execute(statement)).scalar_one_or_none()
    if project_id is None:
        raise EntityNotFoundById(scenario_id, "scenario")

    statement = select(projects_data).where(projects_data.c.project_id == project_id)
    project = (await conn.execute(statement)).mappings().one_or_none()
    if project.user_id != user_id:
        raise AccessDeniedError(project_id, "project")

    physical_object_column = (
        projects_urban_objects_data.c.physical_object_id
        if service.is_scenario_physical_object
        else projects_urban_objects_data.c.public_physical_object_id
    )
    geometry_column = (
        projects_urban_objects_data.c.object_geometry_id
        if service.is_scenario_geometry
        else projects_urban_objects_data.c.public_object_geometry_id
    )
    statement = select(projects_urban_objects_data).where(
        physical_object_column == service.physical_object_id,
        geometry_column == service.object_geometry_id,
        projects_urban_objects_data.c.scenario_id == scenario_id,
    )
    urban_objects = (await conn.execute(statement)).mappings().all()
    is_from_public = False
    if not list(urban_objects):
        is_from_public = True
        if not service.is_scenario_physical_object and not service.is_scenario_geometry:
            statement = select(urban_objects_data).where(
                urban_objects_data.c.physical_object_id == service.physical_object_id,
                urban_objects_data.c.object_geometry_id == service.object_geometry_id,
            )
            urban_objects = (await conn.execute(statement)).mappings().all()
        if not list(urban_objects):
            raise EntityNotFoundByParams(
                "urban object", service.physical_object_id, service.object_geometry_id, scenario_id
            )

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
            public_service_id=None,
            service_type_id=service.service_type_id,
            territory_type_id=service.territory_type_id,
            name=service.name,
            capacity_real=service.capacity_real,
            properties=service.properties,
        )
        .returning(services_data.c.service_id)
    )
    service_id = (await conn.execute(statement)).scalar_one()

    if is_from_public:
        statement = (
            insert(projects_urban_objects_data)
            .values(
                scenario_id=scenario_id,
                service_id=service_id,
                public_physical_object_id=service.physical_object_id,
                public_object_geometry_id=service.object_geometry_id,
            )
            .returning(projects_urban_objects_data.c.urban_object_id)
        )
    else:
        flag = False
        for urban_object in urban_objects:
            if urban_object.service_id is None:
                statement = (
                    update(projects_urban_objects_data)
                    .where(projects_urban_objects_data.c.urban_object_id == urban_object.urban_object_id)
                    .values(service_id=service_id)
                    .returning(projects_urban_objects_data.c.urban_object_id)
                )
                flag = True
                break

        if not flag:
            statement = (
                insert(projects_urban_objects_data)
                .values(
                    scenario_id=scenario_id,
                    service_id=service_id,
                    physical_object_id=service.physical_object_id if service.is_scenario_physical_object else None,
                    object_geometry_id=service.object_geometry_id if service.is_scenario_geometry else None,
                    public_physical_object_id=(
                        service.physical_object_id if not service.is_scenario_physical_object else None
                    ),
                    public_object_geometry_id=service.object_geometry_id if not service.is_scenario_geometry else None,
                )
                .returning(projects_urban_objects_data.c.urban_object_id)
            )

    urban_object_id = (await conn.execute(statement)).scalar_one()
    await conn.commit()

    return await get_scenario_urban_object_by_id_from_db(conn, urban_object_id)


async def get_scenario_service_by_id_from_db(conn: AsyncConnection, service_id: int) -> ScenarioServiceDTO:
    """Get scenario service by identifier."""

    statement = (
        select(
            projects_services_data.c.service_id,
            projects_services_data.c.name,
            projects_services_data.c.capacity_real,
            projects_services_data.c.properties,
            projects_services_data.c.created_at,
            projects_services_data.c.updated_at,
            service_types_dict.c.service_type_id,
            service_types_dict.c.urban_function_id,
            urban_functions_dict.c.name.label("urban_function_name"),
            service_types_dict.c.name.label("service_type_name"),
            service_types_dict.c.capacity_modeled.label("service_type_capacity_modeled"),
            service_types_dict.c.code.label("service_type_code"),
            service_types_dict.c.infrastructure_type,
            service_types_dict.c.properties.label("service_type_properties"),
            territory_types_dict.c.territory_type_id,
            territory_types_dict.c.name.label("territory_type_name"),
            literal(True).label("is_scenario_object"),
        )
        .select_from(
            projects_services_data.join(
                service_types_dict,
                service_types_dict.c.service_type_id == projects_services_data.c.service_type_id,
            )
            .outerjoin(
                territory_types_dict,
                territory_types_dict.c.territory_type_id == projects_services_data.c.territory_type_id,
            )
            .join(
                urban_functions_dict,
                urban_functions_dict.c.urban_function_id == service_types_dict.c.urban_function_id,
            )
        )
        .where(projects_services_data.c.service_id == service_id)
        .distinct()
    )
    result = (await conn.execute(statement)).mappings().one_or_none()
    if result is None:
        raise EntityNotFoundById(service_id, "scenario service")

    return ScenarioServiceDTO(**result)


async def put_service_to_db(
    conn: AsyncConnection,
    service: ServicesDataPut,
    scenario_id: int,
    service_id: int,
    is_scenario_object: bool,
    user_id: str,
) -> ScenarioServiceDTO:
    """Update scenario service by all its attributes."""

    statement = select(scenarios_data.c.project_id).where(scenarios_data.c.scenario_id == scenario_id)
    project_id = (await conn.execute(statement)).scalar_one_or_none()
    if project_id is None:
        raise EntityNotFoundById(scenario_id, "scenario")

    statement = select(projects_data).where(projects_data.c.project_id == project_id)
    project = (await conn.execute(statement)).mappings().one_or_none()
    if project.user_id != user_id:
        raise AccessDeniedError(project_id, "project")

    if is_scenario_object:
        statement = select(projects_services_data.c.service_id).where(projects_services_data.c.service_id == service_id)
    else:
        statement = select(services_data.c.service_id).where(services_data.c.service_id == service_id)
    requested_service = (await conn.execute(statement)).scalar_one_or_none()
    if requested_service is None:
        raise EntityNotFoundById(service_id, "service")

    if not is_scenario_object:
        statement = (
            select(projects_services_data.c.service_id)
            .select_from(
                projects_urban_objects_data.join(
                    projects_services_data,
                    projects_services_data.c.service_id == projects_urban_objects_data.c.service_id,
                )
            )
            .where(
                projects_urban_objects_data.c.scenario_id == scenario_id,
                projects_services_data.c.public_service_id == service_id,
            )
        )
        public_service = (await conn.execute(statement)).scalar_one_or_none()
        if public_service is not None:
            raise EntityAlreadyExists("scenario service", service_id)

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

    if is_scenario_object:
        statement = (
            update(projects_services_data)
            .where(projects_services_data.c.service_id == service_id)
            .values(
                service_type_id=service.service_type_id,
                territory_type_id=service.territory_type_id,
                name=service.name,
                capacity_real=service.capacity_real,
                properties=service.properties,
                updated_at=datetime.now(timezone.utc),
            )
            .returning(projects_services_data.c.service_id)
        )
        updated_service_id = (await conn.execute(statement)).scalar_one()
    else:
        statement = (
            insert(projects_services_data)
            .values(
                public_service_id=service_id,
                service_type_id=service.service_type_id,
                territory_type_id=service.territory_type_id,
                name=service.name,
                capacity_real=service.capacity_real,
                properties=service.properties,
                updated_at=datetime.now(timezone.utc),
            )
            .returning(projects_services_data.c.service_id)
        )
        updated_service_id = (await conn.execute(statement)).scalar_one()

        project_geometry = (
            select(projects_territory_data.c.geometry).where(projects_territory_data.c.project_id == project.project_id)
        ).alias("project_geometry")

        public_urban_object_ids = (
            select(projects_urban_objects_data.c.public_urban_object_id.label("urban_object_id"))
            .where(
                projects_urban_objects_data.c.scenario_id == scenario_id,
                projects_urban_objects_data.c.public_urban_object_id.is_not(None),
            )
            .alias("public_urban_object_ids")
        )

        statement = (
            select(urban_objects_data)
            .select_from(
                urban_objects_data.join(
                    object_geometries_data,
                    object_geometries_data.c.object_geometry_id == urban_objects_data.c.object_geometry_id,
                )
            )
            .where(
                urban_objects_data.c.service_id == service_id,
                urban_objects_data.c.urban_object_id.not_in(select(public_urban_object_ids.c.urban_object_id)),
                ST_Within(object_geometries_data.c.geometry, select(project_geometry).scalar_subquery()),
            )
        )
        urban_objects = (await conn.execute(statement)).mappings().all()
        if urban_objects:
            await conn.execute(
                insert(projects_urban_objects_data).values(
                    [
                        {
                            "public_urban_object_id": row.urban_object_id,
                            "scenario_id": scenario_id,
                        }
                        for row in urban_objects
                    ]
                )
            )
            await conn.execute(
                insert(projects_urban_objects_data).values(
                    [
                        {
                            "service_id": updated_service_id,
                            "public_physical_object_id": row.physical_object_id,
                            "public_object_geometry_id": row.object_geometry_id,
                            "scenario_id": scenario_id,
                        }
                        for row in urban_objects
                    ]
                )
            )
        await conn.execute(
            (
                update(projects_urban_objects_data)
                .where(projects_urban_objects_data.c.public_service_id == service_id)
                .values(service_id=updated_service_id, public_service_id=None)
            )
        )

    await conn.commit()

    return await get_scenario_service_by_id_from_db(conn, updated_service_id)


async def patch_service_to_db(
    conn: AsyncConnection,
    service: ServicesDataPatch,
    scenario_id: int,
    service_id: int,
    is_scenario_object: bool,
    user_id: str,
) -> ScenarioServiceDTO:
    """Update scenario service by only given attributes."""

    statement = select(scenarios_data.c.project_id).where(scenarios_data.c.scenario_id == scenario_id)
    project_id = (await conn.execute(statement)).scalar_one_or_none()
    if project_id is None:
        raise EntityNotFoundById(scenario_id, "scenario")

    statement = select(projects_data).where(projects_data.c.project_id == project_id)
    project = (await conn.execute(statement)).mappings().one_or_none()
    if project.user_id != user_id:
        raise AccessDeniedError(project_id, "project")

    if is_scenario_object:
        statement = select(projects_services_data).where(projects_services_data.c.service_id == service_id)
    else:
        statement = select(services_data).where(services_data.c.service_id == service_id)
    requested_service = (await conn.execute(statement)).mappings().one_or_none()
    if requested_service is None:
        raise EntityNotFoundById(service_id, "service")

    if not is_scenario_object:
        statement = (
            select(projects_services_data.c.service_id)
            .select_from(
                projects_urban_objects_data.join(
                    projects_services_data,
                    projects_services_data.c.service_id == projects_urban_objects_data.c.service_id,
                )
            )
            .where(
                projects_urban_objects_data.c.scenario_id == scenario_id,
                projects_services_data.c.public_service_id == service_id,
            )
        )
        public_service = (await conn.execute(statement)).scalar_one_or_none()
        if public_service is not None:
            raise EntityAlreadyExists("scenario service", service_id)

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

    values_to_update = {}
    for k, v in service.model_dump(exclude_unset=True).items():
        values_to_update.update({k: v})

    if is_scenario_object:
        statement = (
            update(projects_services_data)
            .where(projects_services_data.c.service_id == service_id)
            .values(updated_at=datetime.now(timezone.utc), **values_to_update)
            .returning(projects_services_data.c.service_id)
        )
        updated_service_id = (await conn.execute(statement)).scalar_one()
    else:
        statement = (
            insert(projects_services_data)
            .values(
                public_service_id=service_id,
                service_type_id=values_to_update.get("service_type_id", requested_service.service_type_id),
                territory_type_id=values_to_update.get("territory_type_id", requested_service.territory_type_id),
                name=values_to_update.get("name", requested_service.name),
                capacity_real=values_to_update.get("capacity_real", requested_service.capacity_real),
                properties=values_to_update.get("properties", requested_service.properties),
            )
            .returning(projects_services_data.c.service_id)
        )
        updated_service_id = (await conn.execute(statement)).scalar_one()

        project_geometry = (
            select(projects_territory_data.c.geometry).where(projects_territory_data.c.project_id == project.project_id)
        ).alias("project_geometry")

        public_urban_object_ids = (
            select(projects_urban_objects_data.c.public_urban_object_id.label("urban_object_id"))
            .where(
                projects_urban_objects_data.c.scenario_id == scenario_id,
                projects_urban_objects_data.c.public_urban_object_id.is_not(None),
            )
            .alias("public_urban_object_ids")
        )

        statement = (
            select(urban_objects_data)
            .select_from(
                urban_objects_data.join(
                    object_geometries_data,
                    object_geometries_data.c.object_geometry_id == urban_objects_data.c.object_geometry_id,
                )
            )
            .where(
                urban_objects_data.c.service_id == service_id,
                urban_objects_data.c.urban_object_id.not_in(select(public_urban_object_ids.c.urban_object_id)),
                ST_Within(object_geometries_data.c.geometry, select(project_geometry).scalar_subquery()),
            )
        )
        urban_objects = (await conn.execute(statement)).mappings().all()
        if urban_objects:
            await conn.execute(
                insert(projects_urban_objects_data).values(
                    [
                        {
                            "public_urban_object_id": row.urban_object_id,
                            "scenario_id": scenario_id,
                        }
                        for row in urban_objects
                    ]
                )
            )
            await conn.execute(
                insert(projects_urban_objects_data).values(
                    [
                        {
                            "service_id": updated_service_id,
                            "public_physical_object_id": row.physical_object_id,
                            "public_object_geometry_id": row.object_geometry_id,
                            "scenario_id": scenario_id,
                        }
                        for row in urban_objects
                    ]
                )
            )
        await conn.execute(
            (
                update(projects_urban_objects_data)
                .where(projects_urban_objects_data.c.public_service_id == service_id)
                .values(service_id=updated_service_id, public_service_id=None)
            )
        )

    await conn.commit()

    return await get_scenario_service_by_id_from_db(conn, updated_service_id)


async def delete_service_in_db(
    conn: AsyncConnection,
    scenario_id: int,
    service_id: int,
    is_scenario_object: bool,
    user_id: str,
) -> dict:
    """Delete scenario service."""

    statement = select(scenarios_data.c.project_id).where(scenarios_data.c.scenario_id == scenario_id)
    project_id = (await conn.execute(statement)).scalar_one_or_none()
    if project_id is None:
        raise EntityNotFoundById(scenario_id, "scenario")

    statement = select(projects_data).where(projects_data.c.project_id == project_id)
    project = (await conn.execute(statement)).mappings().one_or_none()
    if project.user_id != user_id:
        raise AccessDeniedError(project_id, "project")

    if is_scenario_object:
        statement = select(projects_services_data.c.service_id).where(projects_services_data.c.service_id == service_id)
    else:
        statement = select(services_data.c.service_id).where(services_data.c.service_id == service_id)
    requested_service = (await conn.execute(statement)).scalar_one_or_none()
    if requested_service is None:
        raise EntityNotFoundById(service_id, "service")

    if is_scenario_object:
        statement = delete(projects_services_data).where(projects_services_data.c.service_id == service_id)
        await conn.execute(statement)
    else:
        statement = delete(projects_urban_objects_data).where(
            projects_urban_objects_data.c.public_service_id == service_id
        )
        await conn.execute(statement)

        project_geometry = (
            select(projects_territory_data.c.geometry).where(projects_territory_data.c.project_id == project.project_id)
        ).alias("project_geometry")

        public_urban_object_ids = (
            select(projects_urban_objects_data.c.public_urban_object_id.label("urban_object_id"))
            .where(
                projects_urban_objects_data.c.scenario_id == scenario_id,
                projects_urban_objects_data.c.public_urban_object_id.is_not(None),
            )
            .alias("public_urban_object_ids")
        )

        statement = (
            select(urban_objects_data)
            .select_from(
                urban_objects_data.join(
                    object_geometries_data,
                    object_geometries_data.c.object_geometry_id == urban_objects_data.c.object_geometry_id,
                )
            )
            .where(
                urban_objects_data.c.service_id == service_id,
                urban_objects_data.c.urban_object_id.not_in(select(public_urban_object_ids.c.urban_object_id)),
                ST_Within(object_geometries_data.c.geometry, select(project_geometry).scalar_subquery()),
            )
        )
        urban_objects = (await conn.execute(statement)).mappings().all()
        if urban_objects:
            await conn.execute(
                insert(projects_urban_objects_data).values(
                    [
                        {
                            "public_urban_object_id": row.urban_object_id,
                            "scenario_id": scenario_id,
                        }
                        for row in urban_objects
                    ]
                )
            )
    await conn.commit()

    return {"result": "ok"}
