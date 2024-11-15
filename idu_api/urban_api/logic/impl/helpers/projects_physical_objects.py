"""Projects physical objects internal logic is defined here."""

from collections import defaultdict

from geoalchemy2 import Geography, Geometry
from geoalchemy2.functions import ST_Buffer, ST_Intersects, ST_Within
from sqlalchemy import cast, or_, select
from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.common.db.entities import (
    object_geometries_data,
    physical_object_functions_dict,
    physical_object_types_dict,
    physical_objects_data,
    projects_data,
    projects_object_geometries_data,
    projects_physical_objects_data,
    projects_services_data,
    projects_territory_data,
    projects_urban_objects_data,
    scenarios_data,
    territories_data,
    urban_objects_data,
)
from idu_api.urban_api.dto import PhysicalObjectDataDTO, ScenarioPhysicalObjectDTO
from idu_api.urban_api.exceptions.logic.common import EntityNotFoundById
from idu_api.urban_api.exceptions.logic.users import AccessDeniedError


async def get_physical_objects_by_scenario_id(
    conn: AsyncConnection,
    scenario_id: int,
    user_id: str,
    physical_object_type_id: int | None,
    physical_object_function_id: int | None,
) -> list[ScenarioPhysicalObjectDTO]:
    """Get physical objects by scenario identifier."""

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
            physical_objects_data.c.physical_object_id,
            physical_object_types_dict.c.physical_object_type_id,
            physical_object_types_dict.c.name.label("physical_object_type_name"),
            physical_object_functions_dict.c.physical_object_function_id,
            physical_object_functions_dict.c.name.label("physical_object_function_name"),
            physical_objects_data.c.name,
            physical_objects_data.c.properties,
            physical_objects_data.c.created_at,
            physical_objects_data.c.updated_at,
        )
        .select_from(
            urban_objects_data.join(
                physical_objects_data,
                physical_objects_data.c.physical_object_id == urban_objects_data.c.physical_object_id,
            )
            .join(
                object_geometries_data,
                object_geometries_data.c.object_geometry_id == urban_objects_data.c.object_geometry_id,
            )
            .join(
                physical_object_types_dict,
                physical_object_types_dict.c.physical_object_type_id == physical_objects_data.c.physical_object_type_id,
            )
            .join(
                physical_object_functions_dict,
                physical_object_functions_dict.c.physical_object_function_id
                == physical_object_types_dict.c.physical_object_function_id,
            )
        )
        .where(
            urban_objects_data.c.urban_object_id.not_in(select(public_urban_object_ids)),
            ST_Within(object_geometries_data.c.geometry, select(project_geometry).scalar_subquery()),
        )
        .distinct()
    )

    # Условия фильтрации для public объектов
    if physical_object_type_id is not None:
        public_urban_objects_query = public_urban_objects_query.where(
            physical_objects_data.c.physical_object_type_id == physical_object_type_id
        )
    if physical_object_function_id is not None:
        public_urban_objects_query = public_urban_objects_query.where(
            physical_object_types_dict.c.physical_object_function_id == physical_object_function_id
        )

    # Получаем все объекты из public.urban_objects_data
    public_objects = []
    for row in (await conn.execute(public_urban_objects_query)).mappings().all():
        public_objects.append(
            {
                "physical_object_id": row.physical_object_id,
                "physical_object_type_id": row.physical_object_type_id,
                "physical_object_type_name": row.physical_object_type_name,
                "physical_object_function_id": row.physical_object_function_id,
                "physical_object_function_name": row.physical_object_function_name,
                "name": row.name,
                "properties": row.properties,
                "created_at": row.created_at,
                "updated_at": row.updated_at,
                "is_scenario_object": False,
            }
        )

    # Шаг 3: Собрать все записи из user_projects.urban_objects_data для данного сценария
    scenario_urban_objects_query = (
        select(
            projects_physical_objects_data.c.physical_object_id,
            physical_object_types_dict.c.physical_object_type_id,
            physical_object_types_dict.c.name.label("physical_object_type_name"),
            physical_object_functions_dict.c.physical_object_function_id,
            physical_object_functions_dict.c.name.label("physical_object_function_name"),
            projects_physical_objects_data.c.name,
            projects_physical_objects_data.c.properties,
            projects_physical_objects_data.c.created_at,
            projects_physical_objects_data.c.updated_at,
            physical_objects_data.c.physical_object_id.label("public_physical_object_id"),
            physical_objects_data.c.physical_object_type_id.label("public_physical_object_type_id"),
            physical_objects_data.c.name.label("public_name"),
            physical_objects_data.c.properties.label("public_properties"),
            physical_objects_data.c.created_at.label("public_created_at"),
            physical_objects_data.c.updated_at.label("public_updated_at"),
        )
        .select_from(
            projects_urban_objects_data.outerjoin(
                projects_physical_objects_data,
                projects_physical_objects_data.c.physical_object_id == projects_urban_objects_data.c.physical_object_id,
            )
            .outerjoin(
                projects_object_geometries_data,
                projects_object_geometries_data.c.object_geometry_id
                == projects_urban_objects_data.c.object_geometry_id,
            )
            .outerjoin(
                physical_objects_data,
                physical_objects_data.c.physical_object_id == projects_urban_objects_data.c.public_physical_object_id,
            )
            .outerjoin(
                object_geometries_data,
                object_geometries_data.c.object_geometry_id == projects_urban_objects_data.c.public_object_geometry_id,
            )
            .outerjoin(
                physical_object_types_dict,
                or_(
                    physical_object_types_dict.c.physical_object_type_id
                    == projects_physical_objects_data.c.physical_object_type_id,
                    physical_object_types_dict.c.physical_object_type_id
                    == physical_objects_data.c.physical_object_type_id,
                ),
            )
            .outerjoin(
                physical_object_functions_dict,
                physical_object_functions_dict.c.physical_object_function_id
                == physical_object_types_dict.c.physical_object_function_id,
            )
        )
        .where(projects_urban_objects_data.c.scenario_id == scenario_id)
        .where(projects_urban_objects_data.c.public_urban_object_id.is_(None))
        .distinct()
    )

    # Условия фильтрации для объектов user_projects
    if physical_object_type_id:
        scenario_urban_objects_query = scenario_urban_objects_query.where(
            (projects_physical_objects_data.c.physical_object_type_id == physical_object_type_id)
            | (physical_objects_data.c.physical_object_type_id == physical_object_type_id)
        )
    if physical_object_function_id is not None:
        scenario_urban_objects_query = scenario_urban_objects_query.where(
            physical_object_types_dict.c.physical_object_function_id == physical_object_function_id
        )

    # Получаем все объекты из user_projects.urban_objects_data
    scenario_objects = []
    for row in (await conn.execute(scenario_urban_objects_query)).mappings().all():
        is_scenario_physical_object = row.physical_object_id is not None and row.public_physical_object_id is None
        scenario_objects.append(
            {
                "physical_object_id": row.physical_object_id or row.public_physical_object_id,
                "physical_object_type_id": (
                    row.physical_object_type_id if is_scenario_physical_object else row.public_physical_object_type_id
                ),
                "physical_object_type_name": row.physical_object_type_name,
                "physical_object_function_id": row.physical_object_function_id,
                "physical_object_function_name": row.physical_object_function_name,
                "name": row.name if is_scenario_physical_object else row.public_name,
                "properties": row.properties if is_scenario_physical_object else row.public_properties,
                "created_at": row.created_at if is_scenario_physical_object else row.public_created_at,
                "updated_at": row.updated_at if is_scenario_physical_object else row.public_updated_at,
                "is_scenario_object": is_scenario_physical_object,
            }
        )

    grouped_objects = defaultdict(dict)
    for obj in public_objects + scenario_objects:
        physical_object_id = obj["physical_object_id"]
        is_scenario_geometry = obj["is_scenario_object"]

        # Проверка и добавление физ объекта
        existing_entry = grouped_objects.get(physical_object_id)
        if existing_entry is None:
            grouped_objects[physical_object_id].update(obj)
        elif existing_entry.get("is_scenario_object") != is_scenario_geometry:
            grouped_objects[-physical_object_id].update(obj)

    return [ScenarioPhysicalObjectDTO(**row) for row in list(grouped_objects.values())]


async def get_context_physical_objects_by_scenario_id_from_db(
    conn: AsyncConnection,
    scenario_id: int,
    user_id: str,
    physical_object_type_id: int | None,
    physical_object_function_id: int | None,
) -> list[PhysicalObjectDataDTO]:
    """Get list of physical objects for 'context' of the project territory."""

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

    # Step 2. Find all the physical objects in `public` schema for `intersecting_territories`
    statement = (
        select(
            physical_objects_data.c.physical_object_id,
            physical_object_types_dict.c.physical_object_type_id,
            physical_object_types_dict.c.name.label("physical_object_type_name"),
            physical_object_functions_dict.c.physical_object_function_id,
            physical_object_functions_dict.c.name.label("physical_object_function_name"),
            physical_objects_data.c.name,
            physical_objects_data.c.properties,
            physical_objects_data.c.created_at,
            physical_objects_data.c.updated_at,
        )
        .select_from(
            urban_objects_data.join(
                physical_objects_data,
                physical_objects_data.c.physical_object_id == urban_objects_data.c.physical_object_id,
            )
            .join(
                object_geometries_data,
                object_geometries_data.c.object_geometry_id == urban_objects_data.c.object_geometry_id,
            )
            .join(
                physical_object_types_dict,
                physical_object_types_dict.c.physical_object_type_id == physical_objects_data.c.physical_object_type_id,
            )
            .join(
                physical_object_functions_dict,
                physical_object_functions_dict.c.physical_object_function_id
                == physical_object_types_dict.c.physical_object_function_id,
            )
        )
        .where(object_geometries_data.c.territory_id.in_(select(all_intersecting_descendants.c.territory_id)))
        .distinct()
    )

    # Условия фильтрации для public объектов
    if physical_object_type_id is not None:
        statement = statement.where(physical_objects_data.c.physical_object_type_id == physical_object_type_id)
    if physical_object_function_id is not None:
        statement = statement.where(
            physical_object_types_dict.c.physical_object_function_id == physical_object_function_id
        )

    result = (await conn.execute(statement)).mappings().all()

    return [PhysicalObjectDataDTO(**row) for row in result]
