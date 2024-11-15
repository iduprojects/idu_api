"""Projects services internal logic is defined here."""

from collections import defaultdict

from geoalchemy2 import Geography, Geometry
from geoalchemy2.functions import ST_Buffer, ST_Intersects, ST_Within
from sqlalchemy import cast, or_, select
from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.common.db.entities import (
    object_geometries_data,
    physical_objects_data,
    projects_data,
    projects_object_geometries_data,
    projects_physical_objects_data,
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
    ServiceDTO,
)
from idu_api.urban_api.exceptions.logic.common import EntityNotFoundById
from idu_api.urban_api.exceptions.logic.users import AccessDeniedError


async def get_services_by_scenario_id(
    conn: AsyncConnection,
    scenario_id: int,
    user_id: str,
    service_type_id: int | None,
    urban_function_id: int | None,
) -> list[ScenarioServiceDTO]:
    """Get services by scenario identifier."""

    statement = select(scenarios_data).where(scenarios_data.c.scenario_id == scenario_id)
    scenario = (await conn.execute(statement)).mappings().one_or_none()
    if scenario is None:
        raise EntityNotFoundById(scenario_id, "scenario")

    statement = select(projects_data).where(projects_data.c.project_id == scenario.project_id)
    project = (await conn.execute(statement)).mappings().one_or_none()
    if project.user_id != user_id:
        raise AccessDeniedError(scenario.project_id, "project")

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
        .distinct()
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
        .distinct()
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
                    "service_id": row.service_id if is_scenario_service else row.public_service_id,
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

    return [ScenarioServiceDTO(**row) for row in list(grouped_objects.values())]


async def get_context_services_by_scenario_id_from_db(
    conn: AsyncConnection,
    scenario_id: int,
    user_id: str,
    service_type_id: int | None,
    urban_function_id: int | None,
) -> list[ServiceDTO]:
    """Get list of services for 'context' of the project territory."""

    statement = select(scenarios_data).where(scenarios_data.c.scenario_id == scenario_id)
    scenario = (await conn.execute(statement)).mappings().one_or_none()
    if scenario is None:
        raise EntityNotFoundById(scenario_id, "scenario")

    statement = select(projects_data).where(projects_data.c.project_id == scenario.project_id)
    project = (await conn.execute(statement)).mappings().one_or_none()
    if project.user_id != user_id:
        raise AccessDeniedError(scenario.project_id, "project")

    buffer_meters = 3000
    project_geometry = (
        select(
            cast(
                ST_Buffer(
                    cast(
                        projects_territory_data.c.geometry,
                        Geography(srid=4326),
                    ),
                    buffer_meters,
                ),
                Geometry(srid=4326),
            ).label("geometry")
        )
        .where(projects_territory_data.c.project_id == project.project_id)
        .alias("project_geometry")
    )

    # Step 1. Find all the territories at the `cities_level` - 1 that intersect with the buffered project geometry.
    territories_cte = (
        select(
            territories_data.c.territory_id,
            territories_data.c.parent_id,
            territories_data.c.level,
            territories_data.c.geometry,
            territories_data.c.is_city,
        )
        .where(territories_data.c.territory_id == project.territory_id)
        .cte(recursive=True)
    )
    territories_cte = territories_cte.union_all(
        select(
            territories_data.c.territory_id,
            territories_data.c.parent_id,
            territories_data.c.level,
            territories_data.c.geometry,
            territories_data.c.is_city,
        ).join(
            territories_cte,
            territories_data.c.parent_id == territories_cte.c.territory_id,
        )
    )
    cities_level = (
        select(territories_cte.c.level)
        .where(territories_cte.c.is_city.is_(True))
        .order_by(territories_cte.c.level.desc())
        .limit(1)
        .cte(name="cities_level")
    )
    intersecting_territories = (
        select(territories_cte.c.territory_id)
        .where(
            territories_cte.c.level == (cities_level.c.level - 1),
            ST_Intersects(territories_cte.c.geometry, select(project_geometry).scalar_subquery()),
        )
        .cte(name="intersecting_territories")
    )
    all_intersecting_descendants = (
        select(
            territories_data.c.territory_id,
            territories_data.c.parent_id,
        )
        .where(territories_data.c.territory_id.in_(select(intersecting_territories.c.territory_id)))
        .cte(name="all_intersecting_descendants", recursive=True)
    )
    all_intersecting_descendants = all_intersecting_descendants.union_all(
        select(
            territories_data.c.territory_id,
            territories_data.c.parent_id,
        ).join(
            all_intersecting_descendants,
            territories_data.c.parent_id == all_intersecting_descendants.c.territory_id,
        )
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
        .where(object_geometries_data.c.territory_id.in_(select(all_intersecting_descendants.c.territory_id)))
        .distinct()
    )

    # Условия фильтрации для public объектов
    if service_type_id is not None:
        statement = statement.where(services_data.c.service_type_id == service_type_id)
    if urban_function_id is not None:
        statement = statement.where(service_types_dict.c.urban_function_id == urban_function_id)

    result = (await conn.execute(statement)).mappings().all()

    return [ServiceDTO(**row) for row in result]
