from geoalchemy2.functions import ST_AsGeoJSON
from sqlalchemy import cast, select
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.common.db.entities import (
    physical_object_types_dict,
    projects_object_geometries_data,
    projects_physical_objects_data,
    projects_services_data,
    projects_urban_objects_data,
    service_types_dict,
    territory_types_dict,
)
from idu_api.urban_api.dto import ScenarioUrbanObjectDTO
from idu_api.urban_api.exceptions.logic.common import EntityNotFoundById


async def get_scenario_urban_object_by_id_from_db(
    conn: AsyncConnection, scenario_urban_object_id: int
) -> ScenarioUrbanObjectDTO:
    """Get urban object by urban object id."""

    statement = (
        select(
            projects_urban_objects_data.c.urban_object_id,
            projects_urban_objects_data.c.scenario_id,
            projects_urban_objects_data.c.object_geometry_id,
            projects_urban_objects_data.c.service_id,
            projects_physical_objects_data.c.physical_object_id,
            projects_physical_objects_data.c.physical_object_type_id,
            physical_object_types_dict.c.name.label("physical_object_type_name"),
            projects_physical_objects_data.c.name.label("physical_object_name"),
            projects_physical_objects_data.c.properties.label("physical_object_properties"),
            projects_physical_objects_data.c.created_at.label("physical_object_created_at"),
            projects_physical_objects_data.c.updated_at.label("physical_object_updated_at"),
            projects_object_geometries_data.c.territory_id,
            cast(ST_AsGeoJSON(projects_object_geometries_data.c.geometry), JSONB).label("geometry"),
            cast(ST_AsGeoJSON(projects_object_geometries_data.c.centre_point), JSONB).label("centre_point"),
            projects_services_data.c.name.label("service_name"),
            projects_services_data.c.capacity_real,
            projects_services_data.c.properties.label("service_properties"),
            projects_services_data.c.created_at.label("service_created_at"),
            projects_services_data.c.updated_at.label("service_updated_at"),
            projects_object_geometries_data.c.address,
            service_types_dict.c.service_type_id,
            service_types_dict.c.urban_function_id,
            service_types_dict.c.name.label("service_type_name"),
            service_types_dict.c.capacity_modeled.label("service_type_capacity_modeled"),
            service_types_dict.c.code.label("service_type_code"),
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
                projects_services_data, projects_services_data.c.service_id == projects_urban_objects_data.c.service_id
            )
            .outerjoin(
                service_types_dict, service_types_dict.c.service_type_id == projects_services_data.c.service_type_id
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

    return ScenarioUrbanObjectDTO(**scenario_urban_object)
