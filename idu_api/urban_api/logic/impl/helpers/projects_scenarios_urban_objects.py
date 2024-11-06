from collections import defaultdict

from geoalchemy2.functions import ST_AsGeoJSON
from sqlalchemy import cast, select, union_all
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.common.db.entities import (
    object_geometries_data,
    physical_object_functions_dict,
    physical_object_types_dict,
    physical_objects_data,
    projects_object_geometries_data,
    projects_physical_objects_data,
    projects_services_data,
    projects_urban_objects_data,
    service_types_dict,
    services_data,
    territory_types_dict,
    urban_functions_dict,
    urban_objects_data,
)
from idu_api.urban_api.dto import ScenariosUrbanObjectDTO
from idu_api.urban_api.exceptions.logic.common import EntityNotFoundById


async def get_scenario_urban_object_by_id_from_db(
    conn: AsyncConnection, scenario_urban_object_id: int
) -> ScenariosUrbanObjectDTO:
    """Get urban object by urban object id."""

    statement = (
        select(
            projects_urban_objects_data,
            projects_physical_objects_data.c.physical_object_type_id,
            physical_object_types_dict.c.name.label("physical_object_type_name"),
            physical_object_types_dict.c.physical_object_function_id,
            physical_object_functions_dict.c.name.label("physical_object_function_name"),
            projects_physical_objects_data.c.name.label("physical_object_name"),
            projects_physical_objects_data.c.properties.label("physical_object_properties"),
            projects_physical_objects_data.c.created_at.label("physical_object_created_at"),
            projects_physical_objects_data.c.updated_at.label("physical_object_updated_at"),
            projects_object_geometries_data.c.territory_id,
            projects_object_geometries_data.c.osm_id,
            cast(ST_AsGeoJSON(projects_object_geometries_data.c.geometry), JSONB).label("geometry"),
            cast(ST_AsGeoJSON(projects_object_geometries_data.c.centre_point), JSONB).label("centre_point"),
            projects_object_geometries_data.c.created_at.label("object_geometry_created_at"),
            projects_object_geometries_data.c.updated_at.label("object_geometry_updated_at"),
            projects_services_data.c.name.label("service_name"),
            projects_services_data.c.capacity_real,
            projects_services_data.c.properties.label("service_properties"),
            projects_services_data.c.created_at.label("service_created_at"),
            projects_services_data.c.updated_at.label("service_updated_at"),
            projects_object_geometries_data.c.address,
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
            projects_urban_objects_data.join(
                projects_physical_objects_data,
                projects_physical_objects_data.c.physical_object_id == projects_urban_objects_data.c.physical_object_id,
            )
            .join(
                projects_object_geometries_data,
                projects_object_geometries_data.c.object_geometry_id
                == projects_urban_objects_data.c.object_geometry_id,
            )
            .join(
                physical_object_types_dict,
                physical_object_types_dict.c.physical_object_type_id
                == projects_physical_objects_data.c.physical_object_type_id,
            )
            .outerjoin(
                physical_object_functions_dict,
                physical_object_functions_dict.c.physical_object_function_id
                == physical_object_types_dict.c.physical_object_function_id,
            )
            .outerjoin(
                projects_services_data, projects_services_data.c.service_id == projects_urban_objects_data.c.service_id
            )
            .outerjoin(
                service_types_dict, service_types_dict.c.service_type_id == projects_services_data.c.service_type_id
            )
            .outerjoin(
                urban_functions_dict,
                urban_functions_dict.c.urban_function_id == service_types_dict.c.urban_function_id,
            )
            .outerjoin(
                territory_types_dict,
                territory_types_dict.c.territory_type_id == projects_services_data.c.territory_type_id,
            )
        )
        .where(projects_urban_objects_data.c.urban_object_id == scenario_urban_object_id)
    )

    scenario_urban_object = (await conn.execute(statement)).mappings().one_or_none()
    if scenario_urban_object is None:
        raise EntityNotFoundById(scenario_urban_object_id, "scenario urban object")

    return ScenariosUrbanObjectDTO(**scenario_urban_object)


async def get_urban_objects_by_scenario_id(
    conn: AsyncConnection,
    scenario_id: int,
    physical_object_type_id: int | None,
    service_type_id: int | None,
):
    # Шаг 1: Получить все public_urban_object_id для данного scenario_id
    public_urban_object_ids = (
        select(projects_urban_objects_data.c.public_urban_object_id)
        .where(projects_urban_objects_data.c.scenario_id == scenario_id)
        .where(projects_urban_objects_data.c.public_urban_object_id.isnot(None))
    ).alias("public_urban_object_ids")

    # Шаг 2: Собрать все записи из public.urban_objects_data по собранным public_urban_object_id
    public_urban_objects_query = (
        select(urban_objects_data.c.urban_object_id, physical_objects_data, object_geometries_data, services_data)
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
        )
        .where(urban_objects_data.c.urban_object_id.not_in(select(public_urban_object_ids)))
    )

    # Условия фильтрации для public объектов
    if physical_object_type_id:
        public_urban_objects_query = public_urban_objects_query.where(
            physical_objects_data.c.physical_object_type_id == physical_object_type_id
        )
    if service_type_id:
        public_urban_objects_query = public_urban_objects_query.where(
            services_data.c.service_type_id == service_type_id
        )

    # Получаем все объекты из public.urban_objects_data
    public_objects = []
    for row in await conn.execute(public_urban_objects_query):
        public_objects.append(
            {
                "object_geometry_id": row.object_geometry_id,
                "geometry": row.object_geometries_data.geometry,
                "centre_point": row.object_geometries_data.centre_point,
                "address": row.object_geometries_data.address,
                "osm_id": row.object_geometries_data.osm_id,
                "physical_object": {
                    "physical_object_id": row.physical_object_id,
                    "name": row.physical_objects_data.name,
                    "properties": row.physical_objects_data.properties,
                    "is_scenario_object": False,
                },
                "service": (
                    {
                        "service_id": row.service_id,
                        "name": row.services_data.name,
                        "capacity_real": row.services_data.capacity_real,
                        "properties": row.services_data.properties,
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
            projects_urban_objects_data.c.physical_object_id.label("user_projects_physical_object_id"),
            projects_urban_objects_data.c.object_geometry_id.label("user_projects_object_geometry_id"),
            projects_urban_objects_data.c.service_id.label("user_projects_service_id"),
            projects_urban_objects_data.c.public_physical_object_id,
            projects_urban_objects_data.c.public_object_geometry_id,
            projects_urban_objects_data.c.public_service_id,
            projects_physical_objects_data,
            projects_object_geometries_data,
            projects_services_data,
            physical_objects_data,
            object_geometries_data,
            services_data,
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

    # Получаем все объекты из user_projects.urban_objects_data
    scenario_objects = []
    for row in await conn.execute(scenario_urban_objects_query):
        is_scenario_geometry = row.object_geometry_id is not None and row.public_object_geometry_id is None
        is_scenario_physical_object = row.physical_object_id is not None and row.public_physical_object_id is None
        is_scenario_service = row.service_id is not None and row.public_service_id is None

        scenario_objects.append(
            {
                "object_geometry_id": row.user_projects_object_geometry_id or row.public_object_geometry_id,
                "geometry": row.projects_object_geometries_data.geometry if is_scenario_geometry else None,
                "centre_point": row.projects_object_geometries_data.centre_point if is_scenario_geometry else None,
                "address": row.projects_object_geometries_data.address if is_scenario_geometry else None,
                "osm_id": row.projects_object_geometries_data.osm_id if is_scenario_geometry else None,
                "physical_object": {
                    "physical_object_id": row.user_projects_physical_object_id or row.public_physical_object_id,
                    "name": (
                        row.projects_physical_objects_data.name
                        if is_scenario_physical_object
                        else row.physical_objects_data.name
                    ),
                    "properties": (
                        row.projects_physical_objects_data.properties
                        if is_scenario_physical_object
                        else row.physical_objects_data.properties
                    ),
                    "is_scenario_object": is_scenario_physical_object,
                },
                "service": (
                    {
                        "service_id": row.user_projects_service_id or row.public_service_id,
                        "name": row.projects_services_data.name if is_scenario_service else row.services_data.name,
                        "capacity_real": (
                            row.projects_services_data.capacity_real
                            if is_scenario_service
                            else row.services_data.capacity_real
                        ),
                        "properties": (
                            row.projects_services_data.properties
                            if is_scenario_service
                            else row.services_data.properties
                        ),
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
        if existing_entry is None or existing_entry.get("is_scenario_object") != is_scenario_geometry:
            # Создаем новый словарь, если еще не существует для данного geometry_id или различаются is_scenario_object
            grouped_objects[geometry_id].update(
                {
                    "object_geometry_id": geometry_id,
                    "geometry": obj.get("geometry"),
                    "centre_point": obj.get("centre_point"),
                    "address": obj.get("address"),
                    "osm_id": obj.get("osm_id"),
                    "is_scenario_object": is_scenario_geometry,
                }
            )

        # Добавление соответствующих объектов
        grouped_objects[geometry_id]["physical_objects"].add(obj["physical_object"])
        grouped_objects[geometry_id]["services"].add(obj["service"])

    return list(grouped_objects.values())
