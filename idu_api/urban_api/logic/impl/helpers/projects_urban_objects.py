"""Projects urban objects internal logic is defined here."""

from geoalchemy2.functions import ST_AsGeoJSON
from sqlalchemy import cast, or_, select
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.common.db.entities import (
    living_buildings_data,
    object_geometries_data,
    physical_object_functions_dict,
    physical_object_types_dict,
    physical_objects_data,
    projects_living_buildings_data,
    projects_object_geometries_data,
    projects_physical_objects_data,
    projects_services_data,
    projects_urban_objects_data,
    service_types_dict,
    services_data,
    territories_data,
    territory_types_dict,
    urban_functions_dict,
)
from idu_api.urban_api.dto import ScenarioUrbanObjectDTO
from idu_api.urban_api.exceptions.logic.common import EntitiesNotFoundByIds


async def get_scenario_urban_object_by_id_from_db(
    conn: AsyncConnection,
    urban_object_ids: list[int],
) -> list[ScenarioUrbanObjectDTO]:
    """Get scenario urban object by urban object identifier."""

    statement = (
        select(
            projects_urban_objects_data,
            physical_object_types_dict.c.physical_object_type_id,
            physical_object_types_dict.c.name.label("physical_object_type_name"),
            physical_object_types_dict.c.physical_object_function_id,
            physical_object_functions_dict.c.name.label("physical_object_function_name"),
            projects_physical_objects_data.c.name.label("physical_object_name"),
            projects_physical_objects_data.c.properties.label("physical_object_properties"),
            projects_physical_objects_data.c.created_at.label("physical_object_created_at"),
            projects_physical_objects_data.c.updated_at.label("physical_object_updated_at"),
            territories_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
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
            projects_object_geometries_data.c.osm_id,
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
            physical_objects_data.c.name.label("public_physical_object_name"),
            physical_objects_data.c.properties.label("public_physical_object_properties"),
            physical_objects_data.c.created_at.label("public_physical_object_created_at"),
            physical_objects_data.c.updated_at.label("public_physical_object_updated_at"),
            cast(ST_AsGeoJSON(object_geometries_data.c.geometry), JSONB).label("public_geometry"),
            cast(ST_AsGeoJSON(object_geometries_data.c.centre_point), JSONB).label("public_centre_point"),
            object_geometries_data.c.created_at.label("public_object_geometry_created_at"),
            object_geometries_data.c.updated_at.label("public_object_geometry_updated_at"),
            services_data.c.name.label("public_service_name"),
            services_data.c.capacity_real.label("public_capacity_real"),
            services_data.c.properties.label("public_service_properties"),
            services_data.c.created_at.label("public_service_created_at"),
            services_data.c.updated_at.label("public_service_updated_at"),
            object_geometries_data.c.address.label("public_address"),
            object_geometries_data.c.osm_id.label("public_osm_id"),
            projects_living_buildings_data.c.living_building_id,
            projects_living_buildings_data.c.living_area,
            projects_living_buildings_data.c.properties.label("living_building_properties"),
            living_buildings_data.c.living_building_id.label("public_living_building_id"),
            living_buildings_data.c.living_area.label("public_living_area"),
            living_buildings_data.c.properties.label("public_living_building_properties"),
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
            .join(
                territories_data,
                or_(
                    territories_data.c.territory_id == object_geometries_data.c.territory_id,
                    territories_data.c.territory_id == projects_object_geometries_data.c.territory_id,
                ),
            )
            .join(
                physical_object_types_dict,
                or_(
                    physical_object_types_dict.c.physical_object_type_id
                    == projects_physical_objects_data.c.physical_object_type_id,
                    physical_object_types_dict.c.physical_object_type_id
                    == physical_objects_data.c.physical_object_type_id,
                ),
            )
            .join(
                physical_object_functions_dict,
                physical_object_functions_dict.c.physical_object_function_id
                == physical_object_types_dict.c.physical_object_function_id,
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
            .outerjoin(
                living_buildings_data,
                living_buildings_data.c.physical_object_id == physical_objects_data.c.physical_object_id,
            )
            .outerjoin(
                projects_living_buildings_data,
                projects_living_buildings_data.c.physical_object_id
                == projects_physical_objects_data.c.physical_object_id,
            )
        )
        .where(projects_urban_objects_data.c.urban_object_id.in_(urban_object_ids))
        .limit(1)  # TODO: a temporary fix to avoid error with multiple living buildings in one physical object
    )

    result = (await conn.execute(statement)).mappings().all()
    if len(urban_object_ids) > len(list(result)):
        raise EntitiesNotFoundByIds("urban object")

    objects = []
    for row in result:
        is_scenario_service = row.service_id is not None and row.public_service_id is None
        is_scenario_physical_object = row.physical_object_id is not None and row.public_physical_object_id is None
        is_scenario_geometry = row.object_geometry_id is not None and row.public_object_geometry_id is None
        res = {
            "urban_object_id": row.urban_object_id,
            "scenario_id": row.scenario_id,
            "public_urban_object_id": row.public_urban_object_id,
            "physical_object_id": row.physical_object_id or row.public_physical_object_id,
            "physical_object_type_id": row.physical_object_type_id,
            "physical_object_type_name": row.physical_object_type_name,
            "physical_object_function_id": row.physical_object_function_id,
            "physical_object_function_name": row.physical_object_function_name,
            "physical_object_name": (
                row.physical_object_name if is_scenario_physical_object else row.public_physical_object_name
            ),
            "physical_object_properties": (
                row.physical_object_properties if is_scenario_physical_object else row.public_physical_object_properties
            ),
            "physical_object_created_at": (
                row.physical_object_created_at if is_scenario_physical_object else row.public_physical_object_created_at
            ),
            "physical_object_updated_at": (
                row.physical_object_updated_at if is_scenario_physical_object else row.public_physical_object_updated_at
            ),
            "living_building_id": (
                row.living_building_id if is_scenario_physical_object else row.public_living_building_id
            ),
            "living_area": row.living_area if is_scenario_physical_object else row.public_living_area,
            "living_building_properties": (
                row.living_building_properties if is_scenario_physical_object else row.public_living_building_properties
            ),
            "is_scenario_physical_object": is_scenario_physical_object,
            "object_geometry_id": row.object_geometry_id or row.public_object_geometry_id,
            "territory_id": row.territory_id,
            "territory_name": row.territory_name,
            "geometry": row.geometry if is_scenario_geometry else row.public_geometry,
            "centre_point": row.centre_point if is_scenario_geometry else row.public_centre_point,
            "address": row.address if is_scenario_geometry else row.public_address,
            "osm_id": row.osm_id if is_scenario_geometry else row.public_osm_id,
            "object_geometry_created_at": (
                row.object_geometry_created_at if is_scenario_geometry else row.public_object_geometry_created_at
            ),
            "object_geometry_updated_at": (
                row.object_geometry_updated_at if is_scenario_geometry else row.public_object_geometry_updated_at
            ),
            "is_scenario_geometry": is_scenario_geometry,
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
            "service_name": row.service_name if is_scenario_service else row.public_service_name,
            "capacity_real": row.capacity_real if is_scenario_service else row.public_capacity_real,
            "service_properties": row.service_properties if is_scenario_service else row.public_service_properties,
            "service_created_at": row.service_created_at if is_scenario_service else row.public_service_created_at,
            "service_updated_at": row.service_updated_at if is_scenario_service else row.public_service_updated_at,
            "is_scenario_service": is_scenario_service,
        }
        objects.append(res)

    return [ScenarioUrbanObjectDTO(**obj) for obj in objects]
