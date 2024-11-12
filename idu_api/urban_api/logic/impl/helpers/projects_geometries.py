"""Projects geometries internal logic is defined here."""

from collections import defaultdict

from geoalchemy2 import Geography, Geometry
from geoalchemy2.functions import ST_AsGeoJSON, ST_Buffer, ST_Within
from sqlalchemy import cast, or_, select
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.common.db.entities import (
    object_geometries_data,
    physical_object_types_dict,
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
    urban_objects_data,
)
from idu_api.urban_api.dto import (
    ScenarioGeometryDTO,
    ScenarioGeometryWithAllObjectsDTO,
    ShortScenarioPhysicalObjectDTO,
    ShortScenarioServiceDTO,
)
from idu_api.urban_api.exceptions.logic.common import EntityNotFoundById
from idu_api.urban_api.exceptions.logic.users import AccessDeniedError


async def get_geometries_by_scenario_id(
    conn: AsyncConnection,
    scenario_id: int,
    user_id: str,
    physical_object_id: int | None,
    service_id: int | None,
    for_context: bool,
) -> list[ScenarioGeometryDTO]:
    """Get geometries by scenario identifier."""

    statement = select(scenarios_data).where(scenarios_data.c.scenario_id == scenario_id)
    scenario = (await conn.execute(statement)).mappings().one_or_none()
    if scenario is None:
        raise EntityNotFoundById(scenario_id, "scenario")

    statement = select(projects_data).where(projects_data.c.project_id == scenario.project_id)
    project = (await conn.execute(statement)).mappings().one_or_none()
    if project.user_id != user_id:
        raise AccessDeniedError(scenario.project_id, "project")

    buffer_meters = 3000

    if for_context:
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
    else:
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
            object_geometries_data.c.object_geometry_id,
            object_geometries_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
            object_geometries_data.c.address,
            object_geometries_data.c.osm_id,
            cast(ST_AsGeoJSON(object_geometries_data.c.geometry), JSONB).label("geometry"),
            cast(ST_AsGeoJSON(object_geometries_data.c.centre_point), JSONB).label("centre_point"),
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
                territories_data,
                territories_data.c.territory_id == object_geometries_data.c.territory_id,
            )
            .outerjoin(services_data, services_data.c.service_id == urban_objects_data.c.service_id)
        )
        .where(
            urban_objects_data.c.urban_object_id.not_in(select(public_urban_object_ids)),
            ST_Within(object_geometries_data.c.geometry, select(project_geometry).scalar_subquery()),
        )
        .distinct()
    )

    # Условия фильтрации для public объектов
    if physical_object_id is not None:
        public_urban_objects_query = public_urban_objects_query.where(
            physical_objects_data.c.physical_object_id == physical_object_id
        )
    if service_id is not None:
        public_urban_objects_query = public_urban_objects_query.where(services_data.c.service_id == service_id)

    # Получаем все объекты из public.urban_objects_data
    public_objects = []
    for row in (await conn.execute(public_urban_objects_query)).mappings().all():
        public_objects.append(
            {
                "object_geometry_id": row.object_geometry_id,
                "territory_id": row.territory_id,
                "territory_name": row.territory_name,
                "geometry": row.geometry,
                "centre_point": row.centre_point,
                "address": row.address,
                "osm_id": row.osm_id,
                "is_scenario_object": False,
            }
        )

    # Шаг 3: Собрать все записи из user_projects.urban_objects_data для данного сценария
    scenario_urban_objects_query = (
        select(
            projects_urban_objects_data.c.object_geometry_id,
            projects_object_geometries_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
            projects_object_geometries_data.c.address,
            projects_object_geometries_data.c.osm_id,
            cast(ST_AsGeoJSON(projects_object_geometries_data.c.geometry), JSONB).label("geometry"),
            cast(ST_AsGeoJSON(projects_object_geometries_data.c.centre_point), JSONB).label("centre_point"),
            object_geometries_data.c.object_geometry_id.label("public_object_geometry_id"),
            object_geometries_data.c.territory_id.label("public_territory_id"),
            object_geometries_data.c.address.label("public_address"),
            object_geometries_data.c.osm_id.label("public_osm_id"),
            cast(ST_AsGeoJSON(object_geometries_data.c.geometry), JSONB).label("public_geometry"),
            cast(ST_AsGeoJSON(object_geometries_data.c.centre_point), JSONB).label("public_centre_point"),
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
                projects_services_data, projects_services_data.c.service_id == projects_urban_objects_data.c.service_id
            )
            .outerjoin(
                physical_objects_data,
                physical_objects_data.c.physical_object_id == projects_urban_objects_data.c.public_physical_object_id,
            )
            .outerjoin(
                object_geometries_data,
                object_geometries_data.c.object_geometry_id == projects_urban_objects_data.c.public_object_geometry_id,
            )
            .outerjoin(services_data, services_data.c.service_id == projects_urban_objects_data.c.public_service_id)
            .outerjoin(
                territories_data,
                or_(
                    territories_data.c.territory_id == object_geometries_data.c.territory_id,
                    territories_data.c.territory_id == projects_object_geometries_data.c.territory_id,
                ),
            )
        )
        .where(projects_urban_objects_data.c.scenario_id == scenario_id)
        .where(projects_urban_objects_data.c.public_urban_object_id.is_(None))
        .distinct()
    )

    # Условия фильтрации для объектов user_projects
    if physical_object_id is not None:
        scenario_urban_objects_query = scenario_urban_objects_query.where(
            physical_objects_data.c.physical_object_id == physical_object_id
        )
    if service_id is not None:
        scenario_urban_objects_query = scenario_urban_objects_query.where(services_data.c.service_id == service_id)

    # Получаем все объекты из user_projects.urban_objects_data
    scenario_objects = []
    for row in (await conn.execute(scenario_urban_objects_query)).mappings().all():
        is_scenario_geometry = row.object_geometry_id is not None and row.public_object_geometry_id is None
        scenario_objects.append(
            {
                "object_geometry_id": row.object_geometry_id or row.public_object_geometry_id,
                "territory_id": row.territory_id if is_scenario_geometry else row.public_territory_id,
                "territory_name": row.territory_name,
                "geometry": row.geometry if is_scenario_geometry else row.public_geometry,
                "centre_point": row.centre_point if is_scenario_geometry else row.public_centre_point,
                "address": row.address if is_scenario_geometry else row.public_address,
                "osm_id": row.osm_id if is_scenario_geometry else row.public_osm_id,
                "is_scenario_object": is_scenario_geometry,
            }
        )

    grouped_objects = defaultdict()
    for obj in public_objects + scenario_objects:
        geometry_id = obj["object_geometry_id"]
        is_scenario_geometry = obj["is_scenario_object"]

        # Проверка и добавление геометрии
        existing_entry = grouped_objects.get(geometry_id)
        if existing_entry is None:
            grouped_objects[geometry_id] = {
                "object_geometry_id": geometry_id,
                "territory_id": obj.get("territory_id"),
                "territory_name": obj.get("territory_name"),
                "geometry": obj.get("geometry"),
                "centre_point": obj.get("centre_point"),
                "address": obj.get("address"),
                "osm_id": obj.get("osm_id"),
                "is_scenario_object": is_scenario_geometry,
            }
        elif existing_entry.get("is_scenario_object") != is_scenario_geometry:
            grouped_objects[-geometry_id] = {
                "object_geometry_id": geometry_id,
                "territory_id": obj.get("territory_id"),
                "territory_name": obj.get("territory_name"),
                "geometry": obj.get("geometry"),
                "centre_point": obj.get("centre_point"),
                "address": obj.get("address"),
                "osm_id": obj.get("osm_id"),
                "is_scenario_object": is_scenario_geometry,
            }

    return [ScenarioGeometryDTO(**row) for row in list(grouped_objects.values())]


async def get_geometries_with_all_objects_by_scenario_id(
    conn: AsyncConnection,
    scenario_id: int,
    user_id: str,
    physical_object_type_id: int | None,
    service_type_id: int | None,
    physical_object_function_id: int | None,
    urban_function_id: int | None,
    for_context: bool,
) -> list[ScenarioGeometryWithAllObjectsDTO]:
    """Get geometries with list of physical objects and services by scenario identifier."""

    statement = select(scenarios_data).where(scenarios_data.c.scenario_id == scenario_id)
    scenario = (await conn.execute(statement)).mappings().one_or_none()
    if scenario is None:
        raise EntityNotFoundById(scenario_id, "scenario")

    statement = select(projects_data).where(projects_data.c.project_id == scenario.project_id)
    project = (await conn.execute(statement)).mappings().one_or_none()
    if project.user_id != user_id:
        raise AccessDeniedError(scenario.project_id, "project")

    buffer_meters = 3000

    if for_context:
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
    else:
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
            physical_objects_data.c.physical_object_type_id,
            physical_objects_data.c.name.label("physical_object_name"),
            object_geometries_data.c.object_geometry_id,
            object_geometries_data.c.territory_id,
            object_geometries_data.c.address,
            object_geometries_data.c.osm_id,
            cast(ST_AsGeoJSON(object_geometries_data.c.geometry), JSONB).label("geometry"),
            cast(ST_AsGeoJSON(object_geometries_data.c.centre_point), JSONB).label("centre_point"),
            services_data.c.service_id,
            services_data.c.service_type_id,
            services_data.c.territory_type_id,
            services_data.c.name.label("service_name"),
            services_data.c.capacity_real,
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
            .outerjoin(services_data, services_data.c.service_id == urban_objects_data.c.service_id)
            .join(
                physical_object_types_dict,
                physical_object_types_dict.c.physical_object_type_id == physical_objects_data.c.physical_object_type_id,
            )
            .outerjoin(
                service_types_dict,
                service_types_dict.c.service_type_id == services_data.c.service_type_id,
            )
        )
        .where(
            urban_objects_data.c.urban_object_id.not_in(select(public_urban_object_ids)),
            ST_Within(object_geometries_data.c.geometry, select(project_geometry).scalar_subquery()),
        )
    )

    # Условия фильтрации для public объектов
    if physical_object_type_id is not None:
        public_urban_objects_query = public_urban_objects_query.where(
            physical_objects_data.c.physical_object_type_id == physical_object_type_id
        )
    if service_type_id is not None:
        public_urban_objects_query = public_urban_objects_query.where(
            services_data.c.service_type_id == service_type_id
        )
    if physical_object_function_id is not None:
        public_urban_objects_query = public_urban_objects_query.where(
            physical_object_types_dict.c.physical_object_function_id == physical_object_function_id
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
                "object_geometry_id": row.object_geometry_id,
                "territory_id": row.territory_id,
                "geometry": row.geometry,
                "centre_point": row.centre_point,
                "address": row.address,
                "osm_id": row.osm_id,
                "physical_object": {
                    "physical_object_id": row.physical_object_id,
                    "physical_object_type_id": row.physical_object_type_id,
                    "name": row.physical_object_name,
                    "is_scenario_object": False,
                },
                "service": (
                    {
                        "service_id": row.service_id,
                        "service_type_id": row.service_type_id,
                        "territory_type_id": row.territory_type_id,
                        "name": row.service_name,
                        "capacity_real": row.capacity_real,
                        "is_scenario_object": False,
                    }
                    if row.service_id
                    else None
                ),
                "is_scenario_object": False,
            }
        )

    # Шаг 3: Собрать все записи из user_projects.urban_objects_data для данного сценария
    scenario_urban_objects_query = (
        select(
            projects_urban_objects_data.c.urban_object_id,
            projects_urban_objects_data.c.physical_object_id,
            projects_urban_objects_data.c.object_geometry_id,
            projects_urban_objects_data.c.service_id,
            projects_urban_objects_data.c.public_physical_object_id,
            projects_urban_objects_data.c.public_object_geometry_id,
            projects_urban_objects_data.c.public_service_id,
            projects_physical_objects_data.c.physical_object_type_id,
            projects_physical_objects_data.c.name.label("physical_object_name"),
            projects_object_geometries_data.c.territory_id,
            projects_object_geometries_data.c.address,
            projects_object_geometries_data.c.osm_id,
            cast(ST_AsGeoJSON(projects_object_geometries_data.c.geometry), JSONB).label("geometry"),
            cast(ST_AsGeoJSON(projects_object_geometries_data.c.centre_point), JSONB).label("centre_point"),
            projects_services_data.c.service_type_id,
            projects_services_data.c.territory_type_id,
            projects_services_data.c.name.label("service_name"),
            projects_services_data.c.capacity_real,
            physical_objects_data.c.physical_object_type_id.label("public_physical_object_type_id"),
            physical_objects_data.c.name.label("public_physical_object_name"),
            object_geometries_data.c.territory_id.label("public_territory_id"),
            object_geometries_data.c.address.label("public_address"),
            object_geometries_data.c.osm_id.label("public_osm_id"),
            cast(ST_AsGeoJSON(object_geometries_data.c.geometry), JSONB).label("public_geometry"),
            cast(ST_AsGeoJSON(object_geometries_data.c.centre_point), JSONB).label("public_centre_point"),
            services_data.c.service_type_id.label("public_service_type_id"),
            services_data.c.territory_type_id.label("public_territory_type_id"),
            services_data.c.name.label("public_service_name"),
            services_data.c.capacity_real.label("public_capacity_real"),
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
                projects_services_data, projects_services_data.c.service_id == projects_urban_objects_data.c.service_id
            )
            .outerjoin(
                physical_objects_data,
                physical_objects_data.c.physical_object_id == projects_urban_objects_data.c.public_physical_object_id,
            )
            .outerjoin(
                object_geometries_data,
                object_geometries_data.c.object_geometry_id == projects_urban_objects_data.c.public_object_geometry_id,
            )
            .outerjoin(services_data, services_data.c.service_id == projects_urban_objects_data.c.public_service_id)
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
                service_types_dict,
                or_(
                    service_types_dict.c.service_type_id == projects_services_data.c.service_type_id,
                    service_types_dict.c.service_type_id == services_data.c.service_type_id,
                ),
            )
        )
        .where(projects_urban_objects_data.c.scenario_id == scenario_id)
        .where(projects_urban_objects_data.c.public_urban_object_id.is_(None))
    )

    # Условия фильтрации для объектов user_projects
    if physical_object_type_id:
        scenario_urban_objects_query = scenario_urban_objects_query.where(
            (projects_physical_objects_data.c.physical_object_type_id == physical_object_type_id)
            | (physical_objects_data.c.physical_object_type_id == physical_object_type_id)
        )
    if service_type_id:
        scenario_urban_objects_query = scenario_urban_objects_query.where(
            (projects_services_data.c.service_type_id == service_type_id)
            | (services_data.c.service_type_id == service_type_id)
        )
    if physical_object_function_id is not None:
        scenario_urban_objects_query = scenario_urban_objects_query.where(
            physical_object_types_dict.c.physical_object_function_id == physical_object_function_id
        )
    if urban_function_id is not None:
        scenario_urban_objects_query = scenario_urban_objects_query.where(
            service_types_dict.c.urban_function_id == urban_function_id
        )

    # Получаем все объекты из user_projects.urban_objects_data
    scenario_objects = []
    for row in (await conn.execute(scenario_urban_objects_query)).mappings().all():
        is_scenario_geometry = row.object_geometry_id is not None and row.public_object_geometry_id is None
        is_scenario_physical_object = row.physical_object_id is not None and row.public_physical_object_id is None
        is_scenario_service = row.service_id is not None and row.public_service_id is None

        scenario_objects.append(
            {
                "object_geometry_id": row.object_geometry_id or row.public_object_geometry_id,
                "territory_id": row.territory_id if is_scenario_geometry else row.public_territory_id,
                "geometry": row.geometry if is_scenario_geometry else row.public_geometry,
                "centre_point": row.centre_point if is_scenario_geometry else row.public_centre_point,
                "address": row.address if is_scenario_geometry else row.public_address,
                "osm_id": row.osm_id if is_scenario_geometry else row.public_osm_id,
                "physical_object": {
                    "physical_object_id": row.physical_object_id or row.public_physical_object_id,
                    "physical_object_type_id": (
                        row.physical_object_type_id
                        if is_scenario_physical_object
                        else row.public_physical_object_type_id
                    ),
                    "name": (
                        row.physical_object_name if is_scenario_physical_object else row.public_physical_object_name
                    ),
                    "is_scenario_object": is_scenario_physical_object,
                },
                "service": (
                    {
                        "service_id": row.user_projects_service_id or row.public_service_id,
                        "service_type_id": (row.service_type_id if is_scenario_service else row.public_service_type_id),
                        "territory_type_id": (
                            row.territory_type_id if is_scenario_service else row.public_territory_type_id
                        ),
                        "name": row.service_name if is_scenario_service else row.public_service_name,
                        "capacity_real": (row.capacity_real if is_scenario_service else row.public_capacity_real),
                        "is_scenario_object": is_scenario_service,
                    }
                    if row.service_id or row.public_service_id
                    else None
                ),
                "is_scenario_object": is_scenario_geometry,
            }
        )

    # Объединение и группировка по object_geometry_id
    grouped_objects = defaultdict(lambda: {"physical_objects": set(), "services": set()})
    for obj in public_objects + scenario_objects:
        geometry_id = obj["object_geometry_id"]
        is_scenario_geometry = obj["is_scenario_object"]

        # Проверка и добавление геометрии
        existing_entry = grouped_objects.get(geometry_id)
        if existing_entry is None:
            grouped_objects[geometry_id].update(
                {
                    "object_geometry_id": geometry_id,
                    "territory_id": obj.get("territory_id"),
                    "geometry": obj.get("geometry"),
                    "centre_point": obj.get("centre_point"),
                    "address": obj.get("address"),
                    "osm_id": obj.get("osm_id"),
                    "is_scenario_object": is_scenario_geometry,
                }
            )
            # Добавление соответствующих объектов
            grouped_objects[geometry_id]["physical_objects"].add(
                ShortScenarioPhysicalObjectDTO(**obj["physical_object"])
            )
            if obj["service"] is not None:
                grouped_objects[geometry_id]["services"].add(ShortScenarioServiceDTO(**obj["service"]))
        elif existing_entry.get("is_scenario_object") != is_scenario_geometry:
            grouped_objects[-geometry_id].update(
                {
                    "object_geometry_id": geometry_id,
                    "territory_id": obj.get("territory_id"),
                    "geometry": obj.get("geometry"),
                    "centre_point": obj.get("centre_point"),
                    "address": obj.get("address"),
                    "osm_id": obj.get("osm_id"),
                    "is_scenario_object": is_scenario_geometry,
                }
            )
            # Добавление соответствующих объектов
            grouped_objects[-geometry_id]["physical_objects"].add(
                ShortScenarioPhysicalObjectDTO(**obj["physical_object"])
            )
            if obj["service"] is not None:
                grouped_objects[-geometry_id]["services"].add(ShortScenarioServiceDTO(**obj["service"]))

    return [ScenarioGeometryWithAllObjectsDTO(**row) for row in list(grouped_objects.values())]
