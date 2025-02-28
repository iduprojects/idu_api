"""Unit tests for scenario urban objects are defined here."""

import pytest
from geoalchemy2.functions import ST_AsEWKB
from sqlalchemy import or_, select

from idu_api.common.db.entities import (
    buildings_data,
    object_geometries_data,
    physical_object_functions_dict,
    physical_object_types_dict,
    physical_objects_data,
    projects_buildings_data,
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
from idu_api.urban_api.exceptions.logic.common import EntitiesNotFoundByIds, TooManyObjectsError
from idu_api.urban_api.logic.impl.helpers.projects_urban_objects import get_scenario_urban_object_by_ids_from_db
from idu_api.urban_api.logic.impl.helpers.utils import OBJECTS_NUMBER_LIMIT
from idu_api.urban_api.schemas import ScenarioUrbanObject
from tests.urban_api.helpers.connection import MockConnection

####################################################################################
#                           Default use-case tests                                 #
####################################################################################


@pytest.mark.asyncio
async def test_get_services_by_scenario_id_from_db(mock_conn: MockConnection):
    """Test the get_services_by_scenario_id_from_db function."""

    # Arrange
    ids = [1]
    not_found_ids = [1, 2]
    too_many_ids = list(range(OBJECTS_NUMBER_LIMIT + 1))
    project_building_columns = [
        col for col in projects_buildings_data.c if col.name not in ("physical_object_id", "properties")
    ]
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
            ST_AsEWKB(projects_object_geometries_data.c.geometry).label("geometry"),
            ST_AsEWKB(projects_object_geometries_data.c.centre_point).label("centre_point"),
            projects_object_geometries_data.c.created_at.label("object_geometry_created_at"),
            projects_object_geometries_data.c.updated_at.label("object_geometry_updated_at"),
            projects_services_data.c.name.label("service_name"),
            projects_services_data.c.capacity,
            projects_services_data.c.is_capacity_real,
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
            ST_AsEWKB(object_geometries_data.c.geometry).label("public_geometry"),
            ST_AsEWKB(object_geometries_data.c.centre_point).label("public_centre_point"),
            object_geometries_data.c.created_at.label("public_object_geometry_created_at"),
            object_geometries_data.c.updated_at.label("public_object_geometry_updated_at"),
            services_data.c.name.label("public_service_name"),
            services_data.c.capacity.label("public_capacity"),
            services_data.c.is_capacity_real.label("public_is_capacity_real"),
            services_data.c.properties.label("public_service_properties"),
            services_data.c.created_at.label("public_service_created_at"),
            services_data.c.updated_at.label("public_service_updated_at"),
            object_geometries_data.c.address.label("public_address"),
            object_geometries_data.c.osm_id.label("public_osm_id"),
            *project_building_columns,
            projects_buildings_data.c.properties.label("building_properties"),
            buildings_data.c.building_id.label("public_building_id"),
            buildings_data.c.properties.label("public_building_properties"),
            buildings_data.c.floors.label("public_floors"),
            buildings_data.c.building_area_official.label("public_building_area_official"),
            buildings_data.c.building_area_modeled.label("public_building_area_modeled"),
            buildings_data.c.project_type.label("public_project_type"),
            buildings_data.c.floor_type.label("public_floor_type"),
            buildings_data.c.wall_material.label("public_wall_material"),
            buildings_data.c.built_year.label("public_built_year"),
            buildings_data.c.exploitation_start_year.label("public_exploitation_start_year"),
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
                buildings_data,
                buildings_data.c.physical_object_id == physical_objects_data.c.physical_object_id,
            )
            .outerjoin(
                projects_buildings_data,
                projects_buildings_data.c.physical_object_id == projects_physical_objects_data.c.physical_object_id,
            )
        )
        .where(projects_urban_objects_data.c.urban_object_id.in_(ids))
    )

    # Act
    with pytest.raises(EntitiesNotFoundByIds):
        await get_scenario_urban_object_by_ids_from_db(mock_conn, not_found_ids)
    with pytest.raises(TooManyObjectsError):
        await get_scenario_urban_object_by_ids_from_db(mock_conn, too_many_ids)
    result = await get_scenario_urban_object_by_ids_from_db(mock_conn, ids)

    # Assert
    assert isinstance(result, list), "Result should be a list."
    assert all(
        isinstance(item, ScenarioUrbanObjectDTO) for item in result
    ), "Each item should be a ScenarioUrbanObjectDTO."
    assert isinstance(
        ScenarioUrbanObject.from_dto(result[0]), ScenarioUrbanObject
    ), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(statement))
