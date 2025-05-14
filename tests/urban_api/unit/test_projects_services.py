"""Unit tests for scenario services objects are defined here."""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch, MagicMock

import pytest
from geoalchemy2.functions import ST_Intersects, ST_Within, ST_AsEWKB
from sqlalchemy import delete, insert, literal, or_, select, update, ScalarSelect

from idu_api.common.db.entities import (
    object_geometries_data,
    projects_object_geometries_data,
    projects_services_data,
    projects_territory_data,
    projects_urban_objects_data,
    service_types_dict,
    services_data,
    territories_data,
    territory_types_dict,
    urban_functions_dict,
    urban_objects_data,
)
from idu_api.urban_api.dto import ScenarioServiceDTO, ScenarioUrbanObjectDTO, ServiceDTO, UserDTO, \
    ScenarioServiceWithGeometryDTO, ServiceWithGeometryDTO
from idu_api.urban_api.exceptions.logic.common import EntityNotFoundById
from idu_api.urban_api.logic.impl.helpers.projects_services import (
    add_service_to_db,
    delete_service_from_db,
    get_context_services_from_db,
    get_scenario_service_by_id_from_db,
    get_services_by_scenario_id_from_db,
    patch_service_to_db,
    put_service_to_db, get_services_with_geometry_by_scenario_id_from_db, get_context_services_with_geometry_from_db,
)
from idu_api.urban_api.logic.impl.helpers.utils import get_context_territories_geometry
from idu_api.urban_api.schemas import (
    ScenarioService,
    ScenarioServicePost,
    ScenarioUrbanObject,
    Service,
    ServicePatch,
    ServicePut, ScenarioServiceWithGeometryAttributes,
)
from idu_api.urban_api.schemas.geometries import GeoJSONResponse
from tests.urban_api.helpers.connection import MockConnection

####################################################################################
#                           Default use-case tests                                 #
####################################################################################


@pytest.mark.asyncio
async def test_get_services_by_scenario_id_from_db(mock_conn: MockConnection):
    """Test the get_services_by_scenario_id_from_db function."""

    # Arrange
    scenario_id = 1
    user = UserDTO(id="mock_string", is_superuser=False)
    service_type_id = 1
    urban_function_id = None

    public_urban_object_ids = (
        select(projects_urban_objects_data.c.public_urban_object_id)
        .where(projects_urban_objects_data.c.scenario_id == scenario_id)
        .where(projects_urban_objects_data.c.public_urban_object_id.isnot(None))
    ).cte(name="public_urban_object_ids")

    project_geometry = (
        select(projects_territory_data.c.geometry).where(projects_territory_data.c.project_id == 1)
    ).scalar_subquery()

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
            territories_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
        )
        .select_from(
            urban_objects_data.join(services_data, services_data.c.service_id == urban_objects_data.c.service_id)
            .join(
                object_geometries_data,
                object_geometries_data.c.object_geometry_id == urban_objects_data.c.object_geometry_id,
            )
            .join(
                territories_data,
                territories_data.c.territory_id == object_geometries_data.c.territory_id,
            )
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
            service_types_dict.c.service_type_id == service_type_id,
        )
    )

    scenario_urban_objects_query = (
        select(
            projects_services_data.c.service_id,
            projects_services_data.c.name,
            projects_services_data.c.capacity,
            projects_services_data.c.is_capacity_real,
            projects_services_data.c.properties,
            projects_services_data.c.created_at,
            projects_services_data.c.updated_at,
            services_data.c.service_id.label("public_service_id"),
            services_data.c.name.label("public_name"),
            services_data.c.capacity.label("public_capacity"),
            services_data.c.is_capacity_real.label("public_is_capacity_real"),
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
            territories_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
        )
        .select_from(
            projects_urban_objects_data.outerjoin(
                projects_services_data, projects_services_data.c.service_id == projects_urban_objects_data.c.service_id
            )
            .outerjoin(services_data, services_data.c.service_id == projects_urban_objects_data.c.public_service_id)
            .outerjoin(
                projects_object_geometries_data,
                projects_object_geometries_data.c.object_geometry_id
                == projects_urban_objects_data.c.object_geometry_id,
            )
            .outerjoin(
                object_geometries_data,
                object_geometries_data.c.object_geometry_id == projects_urban_objects_data.c.public_object_geometry_id,
            )
            .outerjoin(
                territories_data,
                or_(
                    territories_data.c.territory_id == projects_object_geometries_data.c.territory_id,
                    territories_data.c.territory_id == object_geometries_data.c.territory_id,
                ),
            )
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
        .where(
            projects_urban_objects_data.c.scenario_id == scenario_id,
            projects_urban_objects_data.c.public_urban_object_id.is_(None),
            service_types_dict.c.service_type_id == service_type_id,
        )
    )

    # Act
    result = await get_services_by_scenario_id_from_db(mock_conn, scenario_id, user, service_type_id, urban_function_id)

    # Assert
    assert isinstance(result, list), "Result should be a list."
    assert all(isinstance(item, ScenarioServiceDTO) for item in result), "Each item should be a ScenarioServiceDTO."
    assert isinstance(ScenarioService.from_dto(result[0]), ScenarioService), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(public_urban_objects_query))
    mock_conn.execute_mock.assert_any_call(str(scenario_urban_objects_query))


@pytest.mark.asyncio
async def test_get_services_with_geometry_by_scenario_id_from_db(mock_conn: MockConnection):
    """Test the get_services_with_geometry_by_scenario_id_from_db function."""

    # Arrange
    scenario_id = 1
    user = UserDTO(id="mock_string", is_superuser=False)
    service_type_id = 1
    urban_function_id = None

    public_urban_object_ids = (
        select(projects_urban_objects_data.c.public_urban_object_id)
        .where(projects_urban_objects_data.c.scenario_id == scenario_id)
        .where(projects_urban_objects_data.c.public_urban_object_id.isnot(None))
    ).cte(name="public_urban_object_ids")

    project_geometry = (
        select(projects_territory_data.c.geometry).where(projects_territory_data.c.project_id == 1)
    ).scalar_subquery()

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
            territories_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
            object_geometries_data.c.object_geometry_id,
            object_geometries_data.c.address,
            object_geometries_data.c.osm_id,
            ST_AsEWKB(object_geometries_data.c.geometry).label("geometry"),
            ST_AsEWKB(object_geometries_data.c.centre_point).label("centre_point"),
        )
        .select_from(
            urban_objects_data.join(services_data, services_data.c.service_id == urban_objects_data.c.service_id)
            .join(
                object_geometries_data,
                object_geometries_data.c.object_geometry_id == urban_objects_data.c.object_geometry_id,
            )
            .join(
                territories_data,
                territories_data.c.territory_id == object_geometries_data.c.territory_id,
            )
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
            service_types_dict.c.service_type_id == service_type_id,
        )
    )

    scenario_urban_objects_query = (
        select(
            projects_services_data.c.service_id,
            projects_services_data.c.name,
            projects_services_data.c.capacity,
            projects_services_data.c.is_capacity_real,
            projects_services_data.c.properties,
            projects_services_data.c.created_at,
            projects_services_data.c.updated_at,
            projects_object_geometries_data.c.object_geometry_id,
            projects_object_geometries_data.c.address,
            projects_object_geometries_data.c.osm_id,
            ST_AsEWKB(projects_object_geometries_data.c.geometry).label("geometry"),
            ST_AsEWKB(projects_object_geometries_data.c.centre_point).label("centre_point"),
            services_data.c.service_id.label("public_service_id"),
            services_data.c.name.label("public_name"),
            services_data.c.capacity.label("public_capacity"),
            services_data.c.is_capacity_real.label("public_is_capacity_real"),
            services_data.c.properties.label("public_properties"),
            services_data.c.created_at.label("public_created_at"),
            services_data.c.updated_at.label("public_updated_at"),
            object_geometries_data.c.object_geometry_id.label("public_object_geometry_id"),
            object_geometries_data.c.address.label("public_address"),
            object_geometries_data.c.osm_id.label("public_osm_id"),
            ST_AsEWKB(object_geometries_data.c.geometry).label("public_geometry"),
            ST_AsEWKB(object_geometries_data.c.centre_point).label("public_centre_point"),
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
            territories_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
        )
        .select_from(
            projects_urban_objects_data.outerjoin(
                projects_services_data, projects_services_data.c.service_id == projects_urban_objects_data.c.service_id
            )
            .outerjoin(services_data, services_data.c.service_id == projects_urban_objects_data.c.public_service_id)
            .outerjoin(
                projects_object_geometries_data,
                projects_object_geometries_data.c.object_geometry_id
                == projects_urban_objects_data.c.object_geometry_id,
            )
            .outerjoin(
                object_geometries_data,
                object_geometries_data.c.object_geometry_id == projects_urban_objects_data.c.public_object_geometry_id,
            )
            .outerjoin(
                territories_data,
                or_(
                    territories_data.c.territory_id == projects_object_geometries_data.c.territory_id,
                    territories_data.c.territory_id == object_geometries_data.c.territory_id,
                ),
            )
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
        .where(
            projects_urban_objects_data.c.scenario_id == scenario_id,
            projects_urban_objects_data.c.public_urban_object_id.is_(None),
            service_types_dict.c.service_type_id == service_type_id,
        )
    )

    # Act
    with patch("idu_api.urban_api.logic.impl.helpers.projects_services.get_project_by_scenario_id") as mock_check:
        mock_check.return_value.is_regional = False
        result = await get_services_with_geometry_by_scenario_id_from_db(mock_conn, scenario_id, user, service_type_id, urban_function_id)
    geojson_result = await GeoJSONResponse.from_list([r.to_geojson_dict() for r in result])

    # Assert
    assert isinstance(result, list), "Result should be a list."
    assert all(isinstance(item, ScenarioServiceWithGeometryDTO) for item in result), "Each item should be a ScenarioServiceWithGeometryDTO."
    assert isinstance(
        ScenarioServiceWithGeometryAttributes(**geojson_result.features[0].properties),
        ScenarioServiceWithGeometryAttributes,
    ), "Couldn't create pydantic model from geojson properties."
    mock_conn.execute_mock.assert_any_call(str(public_urban_objects_query))
    mock_conn.execute_mock.assert_any_call(str(scenario_urban_objects_query))


@pytest.mark.asyncio
async def test_get_context_services_from_db(mock_conn: MockConnection):
    """Test the get_context_services_from_db function."""

    # Arrange
    project_id = 1
    user = UserDTO(id="mock_string", is_superuser=False)
    service_type_id = 1
    urban_function_id = None
    mock_geom = str(MagicMock(spec=ScalarSelect))
    objects_intersecting = (
        select(object_geometries_data.c.object_geometry_id)
        .select_from(
            object_geometries_data.join(
                urban_objects_data,
                urban_objects_data.c.object_geometry_id == object_geometries_data.c.object_geometry_id,
            ).join(territories_data, territories_data.c.territory_id == object_geometries_data.c.territory_id)
        )
        .where(
            object_geometries_data.c.territory_id.in_([1])
            | ST_Intersects(object_geometries_data.c.geometry, mock_geom)
        )
        .cte(name="objects_intersecting")
    )
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
            territories_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
        )
        .select_from(
            urban_objects_data.join(services_data, services_data.c.service_id == urban_objects_data.c.service_id)
            .join(
                object_geometries_data,
                object_geometries_data.c.object_geometry_id == urban_objects_data.c.object_geometry_id,
            )
            .join(
                objects_intersecting,
                objects_intersecting.c.object_geometry_id == object_geometries_data.c.object_geometry_id,
            )
            .join(
                territories_data,
                territories_data.c.territory_id == object_geometries_data.c.territory_id,
            )
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
        .where(service_types_dict.c.service_type_id == service_type_id)
    )

    # Act
    with patch(
        "idu_api.urban_api.logic.impl.helpers.projects_services.get_context_territories_geometry",
        new_callable=AsyncMock,
    ) as mock_get_context:
        mock_get_context.return_value = mock_geom, [1]
        result = await get_context_services_from_db(mock_conn, project_id, user, service_type_id, urban_function_id)

    # Assert
    assert isinstance(result, list), "Result should be a list."
    assert all(isinstance(item, ServiceDTO) for item in result), "Each item should be a ServiceDTO."
    assert isinstance(Service.from_dto(result[0]), Service), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(statement))


@pytest.mark.asyncio
async def test_get_context_services_with_geometry_from_db(mock_conn: MockConnection):
    """Test the get_context_services_with_geometry_from_db function."""

    # Arrange
    project_id = 1
    user = UserDTO(id="mock_string", is_superuser=False)
    service_type_id = 1
    urban_function_id = None
    mock_geom = str(MagicMock(spec=ScalarSelect))
    objects_intersecting = (
        select(object_geometries_data.c.object_geometry_id)
        .select_from(
            object_geometries_data.join(
                urban_objects_data,
                urban_objects_data.c.object_geometry_id == object_geometries_data.c.object_geometry_id,
            ).join(territories_data, territories_data.c.territory_id == object_geometries_data.c.territory_id)
        )
        .where(
            object_geometries_data.c.territory_id.in_([1])
            | ST_Intersects(object_geometries_data.c.geometry, mock_geom)
        )
        .cte(name="objects_intersecting")
    )
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
            object_geometries_data.c.object_geometry_id,
            object_geometries_data.c.address,
            object_geometries_data.c.osm_id,
            ST_AsEWKB(object_geometries_data.c.geometry).label("geometry"),
            ST_AsEWKB(object_geometries_data.c.centre_point).label("centre_point"),
            territories_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
        ).select_from(
            urban_objects_data.join(services_data, services_data.c.service_id == urban_objects_data.c.service_id)
            .join(
                object_geometries_data,
                object_geometries_data.c.object_geometry_id == urban_objects_data.c.object_geometry_id,
            )
            .join(
                objects_intersecting,
                objects_intersecting.c.object_geometry_id == object_geometries_data.c.object_geometry_id,
            )
            .join(
                territories_data,
                territories_data.c.territory_id == object_geometries_data.c.territory_id,
            )
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
        .where(service_types_dict.c.service_type_id == service_type_id)
    )

    # Act
    with patch(
        "idu_api.urban_api.logic.impl.helpers.projects_services.get_context_territories_geometry",
        new_callable=AsyncMock,
    ) as mock_get_context:
        mock_get_context.return_value = mock_geom, [1]
        result = await get_context_services_with_geometry_from_db(mock_conn, project_id, user, service_type_id, urban_function_id)
    geojson_result = await GeoJSONResponse.from_list([r.to_geojson_dict() for r in result])

    # Assert
    assert isinstance(result, list), "Result should be a list."
    assert all(isinstance(item, ServiceWithGeometryDTO) for item in result), "Each item should be a ServiceWithGeometryDTO."
    assert isinstance(
        Service(**geojson_result.features[0].properties), Service
    ), "Couldn't create pydantic model from geojson properties."
    mock_conn.execute_mock.assert_any_call(str(statement))


@pytest.mark.asyncio
async def test_get_scenario_service_by_id_from_db(mock_conn: MockConnection):
    """Test the get_scenario_service_by_id_from_db function."""

    # Arrange
    service_id = 1
    statement = (
        select(
            projects_services_data,
            service_types_dict.c.urban_function_id,
            urban_functions_dict.c.name.label("urban_function_name"),
            service_types_dict.c.name.label("service_type_name"),
            service_types_dict.c.capacity_modeled.label("service_type_capacity_modeled"),
            service_types_dict.c.code.label("service_type_code"),
            service_types_dict.c.infrastructure_type,
            service_types_dict.c.properties.label("service_type_properties"),
            territory_types_dict.c.name.label("territory_type_name"),
            literal(True).label("is_scenario_object"),
            territories_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
        )
        .select_from(
            projects_urban_objects_data.join(
                projects_services_data,
                projects_services_data.c.service_id == projects_urban_objects_data.c.service_id,
            )
            .outerjoin(
                object_geometries_data,
                object_geometries_data.c.object_geometry_id == projects_urban_objects_data.c.public_object_geometry_id,
            )
            .outerjoin(
                projects_object_geometries_data,
                projects_object_geometries_data.c.object_geometry_id
                == projects_urban_objects_data.c.object_geometry_id,
            )
            .outerjoin(
                territories_data,
                or_(
                    territories_data.c.territory_id == projects_object_geometries_data.c.territory_id,
                    territories_data.c.territory_id == object_geometries_data.c.territory_id,
                ),
            )
            .outerjoin(
                service_types_dict,
                service_types_dict.c.service_type_id == projects_services_data.c.service_type_id,
            )
            .outerjoin(
                territory_types_dict,
                territory_types_dict.c.territory_type_id == projects_services_data.c.territory_type_id,
            )
            .outerjoin(
                urban_functions_dict,
                urban_functions_dict.c.urban_function_id == service_types_dict.c.urban_function_id,
            )
        )
        .where(projects_services_data.c.service_id == service_id)
        .distinct()
    )

    # Act
    result = await get_scenario_service_by_id_from_db(mock_conn, service_id)

    # Assert
    assert isinstance(result, ScenarioServiceDTO), "Result should be a ScenarioPhysicalObjectDTO."
    assert isinstance(ScenarioService.from_dto(result), ScenarioService), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_called_once_with(str(statement))


@pytest.mark.asyncio
@patch("idu_api.urban_api.logic.impl.helpers.projects_services.check_scenario")
async def test_add_service_to_db(
    mock_check: AsyncMock, mock_conn: MockConnection, scenario_service_post_req: ScenarioServicePost
):
    """Test the add_physical_object_with_geometry_to_db function."""

    # Arrange
    async def check_service_type(conn, table, conditions):
        if table == service_types_dict:
            return False
        return True

    async def check_territory_type(conn, table, conditions):
        if table == territory_types_dict:
            return False
        return True

    scenario_id = 1
    service_id = 1
    user = UserDTO(id="mock_string", is_superuser=False)
    check_statement = select(projects_urban_objects_data).where(
        projects_urban_objects_data.c.physical_object_id == scenario_service_post_req.physical_object_id,
        projects_urban_objects_data.c.object_geometry_id == scenario_service_post_req.object_geometry_id,
        projects_urban_objects_data.c.scenario_id == scenario_id,
    )
    insert_service_statement = (
        insert(projects_services_data)
        .values(
            **scenario_service_post_req.model_dump(
                exclude={
                    "physical_object_id",
                    "object_geometry_id",
                    "is_scenario_physical_object",
                    "is_scenario_geometry",
                }
            )
        )
        .returning(services_data.c.service_id)
    )
    insert_urban_object_statement = (
        insert(projects_urban_objects_data)
        .values(
            scenario_id=scenario_id,
            service_id=service_id,
            physical_object_id=scenario_service_post_req.physical_object_id,
            object_geometry_id=scenario_service_post_req.object_geometry_id,
            public_physical_object_id=None,
            public_object_geometry_id=None,
        )
        .returning(projects_urban_objects_data.c.urban_object_id)
    )

    # Act
    with patch(
        "idu_api.urban_api.logic.impl.helpers.projects_services.check_existence",
        new=AsyncMock(side_effect=check_service_type),
    ):
        with pytest.raises(EntityNotFoundById):
            await add_service_to_db(mock_conn, scenario_service_post_req, scenario_id, user)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.projects_services.check_existence",
        new=AsyncMock(side_effect=check_territory_type),
    ):
        with pytest.raises(EntityNotFoundById):
            await add_service_to_db(mock_conn, scenario_service_post_req, scenario_id, user)
    result = await add_service_to_db(mock_conn, scenario_service_post_req, scenario_id, user)

    # Assert
    assert isinstance(result, ScenarioUrbanObjectDTO), "Result should be a ScenarioUrbanObjectDTO."
    assert isinstance(
        ScenarioUrbanObject.from_dto(result), ScenarioUrbanObject
    ), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(check_statement))
    mock_conn.execute_mock.assert_any_call(str(insert_service_statement))
    mock_conn.execute_mock.assert_any_call(str(insert_urban_object_statement))
    mock_conn.commit_mock.assert_called_once()
    mock_check.assert_any_call(mock_conn, scenario_id, user, to_edit=True)


@pytest.mark.asyncio
async def test_put_scenario_service_to_db(mock_conn: MockConnection, service_put_req: ServicePut):
    """Test the put_physical_object_to_db function."""

    # Arrange
    async def check_service(conn, table, conditions):
        if table == projects_services_data:
            return False
        return True

    async def check_service_type(conn, table, conditions):
        if table == service_types_dict:
            return False
        return True

    async def check_territory_type(conn, table, conditions):
        if table == territory_types_dict:
            return False
        return True

    scenario_id = 1
    service_id = 1
    is_scenario_object = True
    user = UserDTO(id="mock_string", is_superuser=False)
    update_statement = (
        update(projects_services_data)
        .where(projects_services_data.c.service_id == service_id)
        .values(**service_put_req.model_dump(), updated_at=datetime.now(timezone.utc))
        .returning(projects_services_data.c.service_id)
    )

    # Act
    with patch(
        "idu_api.urban_api.logic.impl.helpers.projects_services.check_existence",
        new=AsyncMock(side_effect=check_service),
    ):
        with pytest.raises(EntityNotFoundById):
            await put_service_to_db(mock_conn, service_put_req, scenario_id, service_id, is_scenario_object, user)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.projects_services.check_existence",
        new=AsyncMock(side_effect=check_service_type),
    ):
        with pytest.raises(EntityNotFoundById):
            await put_service_to_db(mock_conn, service_put_req, scenario_id, service_id, is_scenario_object, user)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.projects_services.check_existence",
        new=AsyncMock(side_effect=check_territory_type),
    ):
        with pytest.raises(EntityNotFoundById):
            await put_service_to_db(mock_conn, service_put_req, scenario_id, service_id, is_scenario_object, user)
    result = await put_service_to_db(mock_conn, service_put_req, scenario_id, service_id, is_scenario_object, user)

    # Assert
    assert isinstance(result, ScenarioServiceDTO), "Result should be a ScenarioServiceDTO."
    assert isinstance(ScenarioService.from_dto(result), ScenarioService), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(update_statement))
    mock_conn.commit_mock.assert_called_once()


@pytest.mark.asyncio
async def test_patch_scenario_service_to_db(mock_conn: MockConnection, service_patch_req: ServicePatch):
    """Test the patch_service_to_db function."""

    # Arrange
    async def check_service_type(conn, table, conditions):
        if table == service_types_dict:
            return False
        return True

    async def check_territory_type(conn, table, conditions):
        if table == territory_types_dict:
            return False
        return True

    scenario_id = 1
    service_id = 1
    is_scenario_object = True
    user = UserDTO(id="mock_string", is_superuser=False)
    update_statement = (
        update(projects_services_data)
        .where(projects_services_data.c.service_id == service_id)
        .values(**service_patch_req.model_dump(exclude_unset=True), updated_at=datetime.now(timezone.utc))
        .returning(projects_services_data.c.service_id)
    )

    # Act
    with patch(
        "idu_api.urban_api.logic.impl.helpers.projects_services.check_existence",
        new=AsyncMock(side_effect=check_service_type),
    ):
        with pytest.raises(EntityNotFoundById):
            await patch_service_to_db(mock_conn, service_patch_req, scenario_id, service_id, is_scenario_object, user)
    with patch(
        "idu_api.urban_api.logic.impl.helpers.projects_services.check_existence",
        new=AsyncMock(side_effect=check_territory_type),
    ):
        with pytest.raises(EntityNotFoundById):
            await patch_service_to_db(mock_conn, service_patch_req, scenario_id, service_id, is_scenario_object, user)
    result = await patch_service_to_db(mock_conn, service_patch_req, scenario_id, service_id, is_scenario_object, user)

    # Assert
    assert isinstance(result, ScenarioServiceDTO), "Result should be a ScenarioServiceDTO."
    assert isinstance(ScenarioService.from_dto(result), ScenarioService), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(update_statement))
    mock_conn.commit_mock.assert_called_once()


@pytest.mark.asyncio
async def test_delete_public_service_from_db(mock_conn: MockConnection):
    """Test the delete_service_from_db function."""

    # Arrange
    scenario_id = 1
    service_id = 1
    is_scenario_object = False
    user = UserDTO(id="mock_string", is_superuser=False)
    delete_statement = delete(projects_urban_objects_data).where(
        projects_urban_objects_data.c.public_service_id == service_id
    )
    project_geometry = (
        select(projects_territory_data.c.geometry).where(projects_territory_data.c.project_id == 1)
    ).alias("project_geometry")
    public_urban_object_ids = (
        select(projects_urban_objects_data.c.public_urban_object_id.label("urban_object_id"))
        .where(
            projects_urban_objects_data.c.scenario_id == scenario_id,
            projects_urban_objects_data.c.public_urban_object_id.is_not(None),
        )
        .alias("public_urban_object_ids")
    )
    select_urban_object_statement = (
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
    insert_public_urban_objects_statement = insert(projects_urban_objects_data).values(
        [{"public_urban_object_id": 1, "scenario_id": scenario_id}]
    )

    # Act
    with patch("idu_api.urban_api.logic.impl.helpers.projects_services.check_existence") as mock_check_existence:
        result = await delete_service_from_db(mock_conn, scenario_id, service_id, is_scenario_object, user)
        mock_check_existence.return_value = False
        with pytest.raises(EntityNotFoundById):
            await delete_service_from_db(mock_conn, scenario_id, service_id, is_scenario_object, user)

    # Assert
    assert result == {"status": "ok"}, "Result should be {'status': 'ok'}."
    mock_conn.execute_mock.assert_any_call(str(delete_statement))
    mock_conn.execute_mock.assert_any_call(str(select_urban_object_statement))
    mock_conn.execute_mock.assert_any_call(str(insert_public_urban_objects_statement))
    mock_conn.commit_mock.assert_called_once()


@pytest.mark.asyncio
async def test_delete_scenario_service_from_db(mock_conn: MockConnection):
    """Test the delete_service_from_db function."""

    # Arrange
    scenario_id = 1
    service_id = 1
    is_scenario_object = True
    user = UserDTO(id="mock_string", is_superuser=False)
    delete_statement = delete(projects_services_data).where(projects_services_data.c.service_id == service_id)

    # Act
    with patch("idu_api.urban_api.logic.impl.helpers.projects_services.check_existence") as mock_check_existence:
        result = await delete_service_from_db(mock_conn, scenario_id, service_id, is_scenario_object, user)
        mock_check_existence.return_value = False
        with pytest.raises(EntityNotFoundById):
            await delete_service_from_db(mock_conn, scenario_id, service_id, is_scenario_object, user)

    # Assert
    assert result == {"status": "ok"}, "Result should be {'status': 'ok'}."
    mock_conn.execute_mock.assert_any_call(str(delete_statement))
    mock_conn.commit_mock.assert_called_once()
