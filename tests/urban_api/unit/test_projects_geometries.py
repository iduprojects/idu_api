"""Unit tests for scenario geometries objects are defined here."""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest
from geoalchemy2.functions import ST_AsGeoJSON, ST_Centroid, ST_GeomFromText, ST_Intersection, ST_Intersects, ST_Within
from sqlalchemy import cast, delete, insert, literal, or_, select, text, update
from sqlalchemy.dialects.postgresql import JSONB

from idu_api.common.db.entities import (
    living_buildings_data,
    object_geometries_data,
    physical_object_types_dict,
    physical_objects_data,
    projects_living_buildings_data,
    projects_object_geometries_data,
    projects_physical_objects_data,
    projects_services_data,
    projects_territory_data,
    projects_urban_objects_data,
    service_types_dict,
    services_data,
    territories_data,
    territory_types_dict,
    urban_objects_data,
)
from idu_api.urban_api.dto import ObjectGeometryDTO, ScenarioGeometryDTO, ScenarioGeometryWithAllObjectsDTO
from idu_api.urban_api.dto.object_geometries import GeometryWithAllObjectsDTO
from idu_api.urban_api.exceptions.logic.common import EntityNotFoundById
from idu_api.urban_api.logic.impl.helpers.projects_geometries import (
    delete_object_geometry_from_db,
    get_context_geometries_from_db,
    get_context_geometries_with_all_objects_from_db,
    get_geometries_by_scenario_id_from_db,
    get_geometries_with_all_objects_by_scenario_id_from_db,
    get_scenario_object_geometry_by_id_from_db,
    patch_object_geometry_to_db,
    put_object_geometry_to_db,
)
from idu_api.urban_api.logic.impl.helpers.utils import DECIMAL_PLACES, get_all_context_territories
from idu_api.urban_api.schemas import (
    AllObjects,
    GeometryAttributes,
    ObjectGeometryPatch,
    ObjectGeometryPut,
    ScenarioAllObjects,
    ScenarioGeometryAttributes,
    ScenarioObjectGeometry,
)
from idu_api.urban_api.schemas.geometries import GeoJSONResponse
from tests.urban_api.helpers.connection import MockConnection

####################################################################################
#                           Default use-case tests                                 #
####################################################################################


@pytest.mark.asyncio
async def test_get_geometries_by_scenario_id_from_db(mock_conn: MockConnection):
    """Test the get_geometries_by_scenario_id_from_db function."""

    # Arrange
    scenario_id = 1
    user_id = "mock_string"
    physical_object_id = 1
    service_id = 1

    public_urban_object_ids = (
        select(projects_urban_objects_data.c.public_urban_object_id)
        .where(projects_urban_objects_data.c.scenario_id == scenario_id)
        .where(projects_urban_objects_data.c.public_urban_object_id.isnot(None))
    ).alias("public_urban_object_ids")

    project_geometry = (
        select(projects_territory_data.c.geometry).where(projects_territory_data.c.project_id == 1)
    ).alias("project_geometry")

    public_urban_objects_query = (
        select(
            object_geometries_data.c.object_geometry_id,
            object_geometries_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
            object_geometries_data.c.address,
            object_geometries_data.c.osm_id,
            cast(ST_AsGeoJSON(object_geometries_data.c.geometry, DECIMAL_PLACES), JSONB).label("geometry"),
            cast(ST_AsGeoJSON(object_geometries_data.c.centre_point, DECIMAL_PLACES), JSONB).label("centre_point"),
            object_geometries_data.c.created_at,
            object_geometries_data.c.updated_at,
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
            physical_objects_data.c.physical_object_id == physical_object_id,
            services_data.c.service_id == service_id,
        )
        .distinct()
    )

    scenario_urban_objects_query = (
        select(
            projects_urban_objects_data.c.object_geometry_id,
            territories_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
            projects_object_geometries_data.c.address,
            projects_object_geometries_data.c.osm_id,
            cast(ST_AsGeoJSON(projects_object_geometries_data.c.geometry, DECIMAL_PLACES), JSONB).label("geometry"),
            cast(ST_AsGeoJSON(projects_object_geometries_data.c.centre_point, DECIMAL_PLACES), JSONB).label(
                "centre_point"
            ),
            projects_object_geometries_data.c.created_at,
            projects_object_geometries_data.c.updated_at,
            object_geometries_data.c.object_geometry_id.label("public_object_geometry_id"),
            object_geometries_data.c.address.label("public_address"),
            object_geometries_data.c.osm_id.label("public_osm_id"),
            cast(ST_AsGeoJSON(object_geometries_data.c.geometry, DECIMAL_PLACES), JSONB).label("public_geometry"),
            cast(ST_AsGeoJSON(object_geometries_data.c.centre_point, DECIMAL_PLACES), JSONB).label(
                "public_centre_point"
            ),
            object_geometries_data.c.created_at.label("public_created_at"),
            object_geometries_data.c.updated_at.label("public_updated_at"),
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
        .where(
            projects_urban_objects_data.c.scenario_id == scenario_id,
            projects_urban_objects_data.c.public_urban_object_id.is_(None),
            physical_objects_data.c.physical_object_id == physical_object_id,
            services_data.c.service_id == service_id,
        )
        .distinct()
    )

    # Act
    result = await get_geometries_by_scenario_id_from_db(
        mock_conn, scenario_id, user_id, physical_object_id, service_id
    )
    geojson_result = await GeoJSONResponse.from_list([item.to_geojson_dict() for item in result])

    # Assert
    assert isinstance(result, list), "Result should be a list."
    assert all(isinstance(item, ScenarioGeometryDTO) for item in result), "Each item should be a ScenarioGeometryDTO."
    assert isinstance(
        ScenarioGeometryAttributes(**geojson_result.features[0].properties), ScenarioGeometryAttributes
    ), "Couldn't create pydantic model from geojson properties."
    mock_conn.execute_mock.assert_any_call(str(public_urban_objects_query))
    mock_conn.execute_mock.assert_any_call(str(scenario_urban_objects_query))


@pytest.mark.asyncio
async def test_get_geometries_with_all_objects_by_scenario_id_from_db(mock_conn: MockConnection):
    """Test the get_geometries_with_all_objects_by_scenario_id_from_db function."""

    # Arrange
    scenario_id = 1
    user_id = "mock_string"
    physical_object_type_id = 1
    service_type_id = 1

    public_urban_object_ids = (
        select(projects_urban_objects_data.c.public_urban_object_id)
        .where(projects_urban_objects_data.c.scenario_id == scenario_id)
        .where(projects_urban_objects_data.c.public_urban_object_id.isnot(None))
    ).alias("public_urban_object_ids")

    project_geometry = (
        select(projects_territory_data.c.geometry).where(projects_territory_data.c.project_id == 1)
    ).alias("project_geometry")

    public_urban_objects_query = (
        select(
            physical_objects_data.c.physical_object_id,
            physical_object_types_dict.c.physical_object_type_id,
            physical_object_types_dict.c.name.label("physical_object_type_name"),
            physical_objects_data.c.name.label("physical_object_name"),
            physical_objects_data.c.properties.label("physical_object_properties"),
            living_buildings_data.c.living_building_id,
            living_buildings_data.c.living_area,
            living_buildings_data.c.properties.label("living_building_properties"),
            object_geometries_data.c.object_geometry_id,
            territories_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
            object_geometries_data.c.address,
            object_geometries_data.c.osm_id,
            cast(ST_AsGeoJSON(object_geometries_data.c.geometry, DECIMAL_PLACES), JSONB).label("geometry"),
            cast(ST_AsGeoJSON(object_geometries_data.c.centre_point, DECIMAL_PLACES), JSONB).label("centre_point"),
            services_data.c.service_id,
            services_data.c.name.label("service_name"),
            services_data.c.capacity_real,
            services_data.c.properties.label("service_properties"),
            service_types_dict.c.service_type_id,
            service_types_dict.c.name.label("service_type_name"),
            territory_types_dict.c.territory_type_id,
            territory_types_dict.c.name.label("territory_type_name"),
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
            .outerjoin(
                territory_types_dict,
                territory_types_dict.c.territory_type_id == services_data.c.territory_type_id,
            )
            .outerjoin(
                living_buildings_data,
                living_buildings_data.c.physical_object_id == physical_objects_data.c.physical_object_id,
            )
            .join(
                territories_data,
                territories_data.c.territory_id == object_geometries_data.c.territory_id,
            )
        )
        .where(
            urban_objects_data.c.urban_object_id.not_in(select(public_urban_object_ids)),
            ST_Within(object_geometries_data.c.geometry, select(project_geometry).scalar_subquery()),
            physical_object_types_dict.c.physical_object_type_id == physical_object_type_id,
            service_types_dict.c.service_type_id == service_type_id,
        )
    )

    # Mock the scenario urban objects query
    scenario_urban_objects_query = (
        select(
            projects_urban_objects_data.c.urban_object_id,
            projects_urban_objects_data.c.physical_object_id,
            projects_urban_objects_data.c.object_geometry_id,
            projects_urban_objects_data.c.service_id,
            projects_urban_objects_data.c.public_physical_object_id,
            projects_urban_objects_data.c.public_object_geometry_id,
            projects_urban_objects_data.c.public_service_id,
            projects_physical_objects_data.c.name.label("physical_object_name"),
            projects_physical_objects_data.c.properties.label("physical_object_properties"),
            projects_living_buildings_data.c.living_building_id,
            projects_living_buildings_data.c.living_area,
            projects_living_buildings_data.c.properties.label("living_building_properties"),
            projects_object_geometries_data.c.territory_id,
            projects_object_geometries_data.c.address,
            projects_object_geometries_data.c.osm_id,
            cast(ST_AsGeoJSON(projects_object_geometries_data.c.geometry, DECIMAL_PLACES), JSONB).label("geometry"),
            cast(ST_AsGeoJSON(projects_object_geometries_data.c.centre_point, DECIMAL_PLACES), JSONB).label(
                "centre_point"
            ),
            projects_services_data.c.name.label("service_name"),
            projects_services_data.c.capacity_real,
            projects_services_data.c.properties.label("service_properties"),
            physical_objects_data.c.name.label("public_physical_object_name"),
            physical_objects_data.c.properties.label("public_physical_object_properties"),
            living_buildings_data.c.living_building_id.label("public_living_building_id"),
            living_buildings_data.c.living_area.label("public_living_area"),
            living_buildings_data.c.properties.label("public_living_building_properties"),
            object_geometries_data.c.address.label("public_address"),
            object_geometries_data.c.osm_id.label("public_osm_id"),
            cast(ST_AsGeoJSON(object_geometries_data.c.geometry, DECIMAL_PLACES), JSONB).label("public_geometry"),
            cast(ST_AsGeoJSON(object_geometries_data.c.centre_point, DECIMAL_PLACES), JSONB).label(
                "public_centre_point"
            ),
            services_data.c.name.label("public_service_name"),
            services_data.c.capacity_real.label("public_capacity_real"),
            services_data.c.properties.label("public_service_properties"),
            physical_object_types_dict.c.physical_object_type_id,
            physical_object_types_dict.c.name.label("physical_object_type_name"),
            territories_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
            service_types_dict.c.service_type_id,
            service_types_dict.c.name.label("service_type_name"),
            territory_types_dict.c.territory_type_id,
            territory_types_dict.c.name.label("territory_type_name"),
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
            .outerjoin(
                territory_types_dict,
                or_(
                    territory_types_dict.c.territory_type_id == projects_services_data.c.territory_type_id,
                    territory_types_dict.c.territory_type_id == services_data.c.territory_type_id,
                ),
            )
            .outerjoin(
                territories_data,
                or_(
                    territories_data.c.territory_id == projects_object_geometries_data.c.territory_id,
                    territories_data.c.territory_id == object_geometries_data.c.territory_id,
                ),
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
        .where(
            projects_urban_objects_data.c.scenario_id == scenario_id,
            projects_urban_objects_data.c.public_urban_object_id.is_(None),
            physical_object_types_dict.c.physical_object_type_id == physical_object_type_id,
            service_types_dict.c.service_type_id == service_type_id,
        )
    )

    # Act
    result = await get_geometries_with_all_objects_by_scenario_id_from_db(
        mock_conn, scenario_id, user_id, physical_object_type_id, service_type_id, None, None
    )
    geojson_result = await GeoJSONResponse.from_list([item.to_geojson_dict() for item in result])

    # Assert
    assert isinstance(result, list), "Result should be a list."
    assert all(
        isinstance(item, ScenarioGeometryWithAllObjectsDTO) for item in result
    ), "Each item should be a ScenarioGeometryWithAllObjectsDTO."
    assert isinstance(
        ScenarioAllObjects(**geojson_result.features[0].properties), ScenarioAllObjects
    ), "Couldn't create pydantic model from geojson properties."
    mock_conn.execute_mock.assert_any_call(str(public_urban_objects_query))
    mock_conn.execute_mock.assert_any_call(str(scenario_urban_objects_query))


@pytest.mark.asyncio
async def test_get_context_geometries_from_db(mock_conn: MockConnection):
    """Test the get_geometries_by_scenario_id_from_db function."""

    # Arrange
    project_id = 1
    user_id = "mock_string"
    physical_object_id = 1
    service_id = 1

    context = await get_all_context_territories(mock_conn, project_id, user_id)

    objects_intersecting = (
        select(object_geometries_data.c.object_geometry_id)
        .where(
            object_geometries_data.c.territory_id.in_(select(context["territories"].c.territory_id)),
            ST_Intersects(object_geometries_data.c.geometry, context["geometry"]),
        )
        .subquery()
    )

    statement = (
        select(
            object_geometries_data.c.object_geometry_id,
            object_geometries_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
            object_geometries_data.c.address,
            object_geometries_data.c.osm_id,
            cast(
                ST_AsGeoJSON(ST_Intersection(object_geometries_data.c.geometry, context["geometry"]), DECIMAL_PLACES),
                JSONB,
            ).label("geometry"),
            cast(
                ST_AsGeoJSON(
                    ST_Centroid(ST_Intersection(object_geometries_data.c.geometry, context["geometry"])), DECIMAL_PLACES
                ),
                JSONB,
            ).label("centre_point"),
            object_geometries_data.c.created_at,
            object_geometries_data.c.updated_at,
        )
        .select_from(
            object_geometries_data.join(
                territories_data,
                territories_data.c.territory_id == object_geometries_data.c.territory_id,
            )
            .join(
                urban_objects_data,
                urban_objects_data.c.object_geometry_id == object_geometries_data.c.object_geometry_id,
            )
            .join(
                physical_objects_data,
                physical_objects_data.c.physical_object_id == urban_objects_data.c.physical_object_id,
            )
            .outerjoin(services_data, services_data.c.service_id == urban_objects_data.c.service_id)
        )
        .where(
            object_geometries_data.c.object_geometry_id.in_(select(objects_intersecting)),
            physical_objects_data.c.physical_object_id == physical_object_id,
            services_data.c.service_id == service_id,
        )
        .distinct()
    )

    # Act
    result = await get_context_geometries_from_db(mock_conn, project_id, user_id, physical_object_id, service_id)
    geojson_result = await GeoJSONResponse.from_list([item.to_geojson_dict() for item in result])

    # Assert
    assert isinstance(result, list), "Result should be a list."
    assert all(isinstance(item, ObjectGeometryDTO) for item in result), "Each item should be a ObjectGeometryDTO."
    assert isinstance(
        GeometryAttributes(**geojson_result.features[0].properties), GeometryAttributes
    ), "Couldn't create pydantic model from geojson properties."
    mock_conn.execute_mock.assert_any_call(str(statement))


@pytest.mark.asyncio
async def test_get_context_geometries_with_all_objects_from_db(mock_conn: MockConnection):
    """Test the get_geometries_by_scenario_id_from_db function."""

    # Arrange
    project_id = 1
    user_id = "mock_string"
    physical_object_type_id = 1
    service_type_id = 1

    context = await get_all_context_territories(mock_conn, project_id, user_id)

    objects_intersecting = (
        select(object_geometries_data.c.object_geometry_id)
        .where(
            object_geometries_data.c.territory_id.in_(select(context["territories"].c.territory_id)),
            ST_Intersects(object_geometries_data.c.geometry, context["geometry"]),
        )
        .subquery()
    )

    statement = (
        select(
            physical_objects_data.c.physical_object_id,
            physical_object_types_dict.c.physical_object_type_id,
            physical_object_types_dict.c.name.label("physical_object_type_name"),
            physical_objects_data.c.name.label("physical_object_name"),
            physical_objects_data.c.properties.label("physical_object_properties"),
            living_buildings_data.c.living_building_id,
            living_buildings_data.c.living_area,
            living_buildings_data.c.properties.label("living_building_properties"),
            object_geometries_data.c.object_geometry_id,
            territories_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
            object_geometries_data.c.address,
            object_geometries_data.c.osm_id,
            cast(
                ST_AsGeoJSON(ST_Intersection(object_geometries_data.c.geometry, context["geometry"]), DECIMAL_PLACES),
                JSONB,
            ).label("geometry"),
            cast(
                ST_AsGeoJSON(
                    ST_Centroid(ST_Intersection(object_geometries_data.c.geometry, context["geometry"])), DECIMAL_PLACES
                ),
                JSONB,
            ).label("centre_point"),
            services_data.c.service_id,
            services_data.c.name.label("service_name"),
            services_data.c.capacity_real,
            services_data.c.properties.label("service_properties"),
            service_types_dict.c.service_type_id,
            service_types_dict.c.name.label("service_type_name"),
            territory_types_dict.c.territory_type_id,
            territory_types_dict.c.name.label("territory_type_name"),
        )
        .select_from(
            object_geometries_data.join(
                territories_data,
                territories_data.c.territory_id == object_geometries_data.c.territory_id,
            )
            .join(
                urban_objects_data,
                urban_objects_data.c.object_geometry_id == object_geometries_data.c.object_geometry_id,
            )
            .join(
                physical_objects_data,
                physical_objects_data.c.physical_object_id == urban_objects_data.c.physical_object_id,
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
            .outerjoin(
                territory_types_dict,
                territory_types_dict.c.territory_type_id == services_data.c.territory_type_id,
            )
            .outerjoin(
                living_buildings_data,
                living_buildings_data.c.physical_object_id == physical_objects_data.c.physical_object_id,
            )
        )
        .where(
            object_geometries_data.c.object_geometry_id.in_(select(objects_intersecting)),
            physical_objects_data.c.physical_object_type_id == physical_object_type_id,
            services_data.c.service_type_id == service_type_id,
        )
        .distinct()
    )

    # Act
    result = await get_context_geometries_with_all_objects_from_db(
        mock_conn, project_id, user_id, physical_object_type_id, service_type_id, None, None
    )
    geojson_result = await GeoJSONResponse.from_list([item.to_geojson_dict() for item in result])

    # Assert
    assert isinstance(result, list), "Result should be a list."
    assert all(
        isinstance(item, GeometryWithAllObjectsDTO) for item in result
    ), "Each item should be a ObjectGeometryDTO."
    assert isinstance(
        AllObjects(**geojson_result.features[0].properties), AllObjects
    ), "Couldn't create pydantic model from geojson properties."
    mock_conn.execute_mock.assert_any_call(str(statement))


@pytest.mark.asyncio
async def test_get_scenario_object_geometry_by_id_from_db(mock_conn: MockConnection):
    """Test the get_scenario_object_geometry_by_id_from_db function."""

    # Arrange
    object_geometry_id = 1
    statement = (
        select(
            projects_object_geometries_data.c.object_geometry_id,
            projects_object_geometries_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
            projects_object_geometries_data.c.address,
            projects_object_geometries_data.c.osm_id,
            cast(ST_AsGeoJSON(projects_object_geometries_data.c.geometry, DECIMAL_PLACES), JSONB).label("geometry"),
            cast(ST_AsGeoJSON(projects_object_geometries_data.c.centre_point, DECIMAL_PLACES), JSONB).label(
                "centre_point"
            ),
            projects_object_geometries_data.c.created_at,
            projects_object_geometries_data.c.updated_at,
            literal(True).label("is_scenario_object"),
        )
        .select_from(
            projects_object_geometries_data.join(
                territories_data,
                territories_data.c.territory_id == projects_object_geometries_data.c.territory_id,
            )
        )
        .where(projects_object_geometries_data.c.object_geometry_id == object_geometry_id)
    )

    # Act
    result = await get_scenario_object_geometry_by_id_from_db(mock_conn, object_geometry_id)

    # Assert
    assert isinstance(result, ScenarioGeometryDTO), "Result should be a ScenarioGeometryDTO."
    assert isinstance(
        ScenarioObjectGeometry.from_dto(result), ScenarioObjectGeometry
    ), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_called_once_with(str(statement))


@pytest.mark.asyncio
async def test_put_scenario_object_geometry_to_db(
    mock_conn: MockConnection, object_geometries_put_req: ObjectGeometryPut
):
    """Test the put_object_geometry_to_db function."""

    # Arrange
    async def check_geometry(conn, table, conditions):
        if table == projects_object_geometries_data:
            return False
        return True

    async def check_territory(conn, table, conditions):
        if table == territories_data:
            return False
        return True

    scenario_id = 1
    object_geometry_id = 1
    is_scenario_object = True
    user_id = "mock_string"
    update_statement = (
        update(projects_object_geometries_data)
        .where(projects_object_geometries_data.c.object_geometry_id == object_geometry_id)
        .values(
            territory_id=object_geometries_put_req.territory_id,
            geometry=ST_GeomFromText(object_geometries_put_req.geometry.as_shapely_geometry().wkt, text("4326")),
            centre_point=ST_GeomFromText(
                object_geometries_put_req.centre_point.as_shapely_geometry().wkt, text("4326")
            ),
            address=object_geometries_put_req.address,
            osm_id=object_geometries_put_req.osm_id,
            updated_at=datetime.now(timezone.utc),
        )
        .returning(projects_object_geometries_data.c.object_geometry_id)
    )

    # Act
    with patch(
        "idu_api.urban_api.logic.impl.helpers.projects_geometries.check_existence",
        new=AsyncMock(side_effect=check_geometry),
    ):
        with pytest.raises(EntityNotFoundById):
            await put_object_geometry_to_db(
                mock_conn, object_geometries_put_req, scenario_id, object_geometry_id, is_scenario_object, user_id
            )
    with patch(
        "idu_api.urban_api.logic.impl.helpers.projects_geometries.check_existence",
        new=AsyncMock(side_effect=check_territory),
    ):
        with pytest.raises(EntityNotFoundById):
            await put_object_geometry_to_db(
                mock_conn, object_geometries_put_req, scenario_id, object_geometry_id, is_scenario_object, user_id
            )
    result = await put_object_geometry_to_db(
        mock_conn, object_geometries_put_req, scenario_id, object_geometry_id, is_scenario_object, user_id
    )

    # Assert
    assert isinstance(result, ScenarioGeometryDTO), "Result should be a ScenarioGeometryDTO."
    assert isinstance(
        ScenarioObjectGeometry.from_dto(result), ScenarioObjectGeometry
    ), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(update_statement))
    mock_conn.commit_mock.assert_called_once()


@pytest.mark.asyncio
async def test_patch_scenario_object_geometry_to_db(
    mock_conn: MockConnection, object_geometries_patch_req: ObjectGeometryPatch
):
    """Test the patch_object_geometry_to_db function."""

    # Arrange
    async def check_territory(conn, table, conditions):
        if table == territories_data:
            return False
        return True

    scenario_id = 1
    object_geometry_id = 1
    is_scenario_object = True
    user_id = "mock_string"
    update_statement = (
        update(projects_object_geometries_data)
        .where(projects_object_geometries_data.c.object_geometry_id == object_geometry_id)
        .values(**object_geometries_patch_req.model_dump(exclude_unset=True), updated_at=datetime.now(timezone.utc))
        .returning(projects_object_geometries_data.c.object_geometry_id)
    )

    # Act
    with patch(
        "idu_api.urban_api.logic.impl.helpers.projects_geometries.check_existence",
        new=AsyncMock(side_effect=check_territory),
    ):
        with pytest.raises(EntityNotFoundById):
            await patch_object_geometry_to_db(
                mock_conn, object_geometries_patch_req, scenario_id, object_geometry_id, is_scenario_object, user_id
            )
    result = await patch_object_geometry_to_db(
        mock_conn, object_geometries_patch_req, scenario_id, object_geometry_id, is_scenario_object, user_id
    )

    # Assert
    assert isinstance(result, ScenarioGeometryDTO), "Result should be a ScenarioGeometryDTO."
    assert isinstance(
        ScenarioObjectGeometry.from_dto(result), ScenarioObjectGeometry
    ), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(update_statement))
    mock_conn.commit_mock.assert_called_once()


@pytest.mark.asyncio
async def test_delete_public_object_geometry_from_db(mock_conn: MockConnection):
    """Test the delete_object_geometry_from_db function."""

    # Arrange
    scenario_id = 1
    object_geometry_id = 1
    is_scenario_object = False
    user_id = "mock_string"
    delete_statement = delete(projects_urban_objects_data).where(
        projects_urban_objects_data.c.public_object_geometry_id == object_geometry_id
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
            urban_objects_data.c.object_geometry_id == object_geometry_id,
            urban_objects_data.c.urban_object_id.not_in(select(public_urban_object_ids.c.urban_object_id)),
            ST_Within(object_geometries_data.c.geometry, select(project_geometry).scalar_subquery()),
        )
    )
    insert_public_urban_objects_statement = insert(projects_urban_objects_data).values(
        [{"public_urban_object_id": 1, "scenario_id": scenario_id}]
    )

    # Act
    with patch("idu_api.urban_api.logic.impl.helpers.projects_geometries.check_existence") as mock_check_existence:
        result = await delete_object_geometry_from_db(
            mock_conn, scenario_id, object_geometry_id, is_scenario_object, user_id
        )
        mock_check_existence.return_value = False
        with pytest.raises(EntityNotFoundById):
            await delete_object_geometry_from_db(
                mock_conn, scenario_id, object_geometry_id, is_scenario_object, user_id
            )

    # Assert
    assert result == {"status": "ok"}, "Result should be {'status': 'ok'}."
    mock_conn.execute_mock.assert_any_call(str(delete_statement))
    mock_conn.execute_mock.assert_any_call(str(select_urban_object_statement))
    mock_conn.execute_mock.assert_any_call(str(insert_public_urban_objects_statement))
    mock_conn.commit_mock.assert_called_once()


@pytest.mark.asyncio
async def test_delete_scenario_object_geometry_from_db(mock_conn: MockConnection):
    """Test the delete_object_geometry_from_db function."""

    # Arrange
    scenario_id = 1
    object_geometry_id = 1
    is_scenario_object = True
    user_id = "mock_string"
    delete_statement = delete(projects_object_geometries_data).where(
        projects_object_geometries_data.c.object_geometry_id == object_geometry_id
    )

    # Act
    with patch("idu_api.urban_api.logic.impl.helpers.projects_geometries.check_existence") as mock_check_existence:
        result = await delete_object_geometry_from_db(
            mock_conn, scenario_id, object_geometry_id, is_scenario_object, user_id
        )
        mock_check_existence.return_value = False
        with pytest.raises(EntityNotFoundById):
            await delete_object_geometry_from_db(
                mock_conn, scenario_id, object_geometry_id, is_scenario_object, user_id
            )

    # Assert
    assert result == {"status": "ok"}, "Result should be {'status': 'ok'}."
    mock_conn.execute_mock.assert_any_call(str(delete_statement))
    mock_conn.commit_mock.assert_called_once()
