"""Unit tests for scenario geometries objects are defined here."""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from geoalchemy2.functions import ST_AsEWKB, ST_Centroid, ST_GeomFromWKB, ST_Intersection, ST_Intersects, ST_Within
from sqlalchemy import ScalarSelect, case, delete, insert, literal, or_, select, text, update

from idu_api.common.db.entities import (
    buildings_data,
    object_geometries_data,
    physical_object_types_dict,
    physical_objects_data,
    projects_buildings_data,
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
from idu_api.urban_api.dto import ObjectGeometryDTO, ScenarioGeometryDTO, ScenarioGeometryWithAllObjectsDTO, UserDTO
from idu_api.urban_api.dto.object_geometries import GeometryWithAllObjectsDTO
from idu_api.urban_api.exceptions.logic.common import EntityNotFoundById
from idu_api.urban_api.exceptions.logic.projects import NotAllowedInRegionalScenario
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
from idu_api.urban_api.logic.impl.helpers.utils import SRID, include_child_territories_cte
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
    user = UserDTO(id="mock_string", is_superuser=False)
    physical_object_id = 1
    service_id = 1

    territories_cte = include_child_territories_cte(1)
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
            object_geometries_data.c.object_geometry_id,
            object_geometries_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
            object_geometries_data.c.address,
            object_geometries_data.c.osm_id,
            ST_AsEWKB(object_geometries_data.c.geometry).label("geometry"),
            ST_AsEWKB(object_geometries_data.c.centre_point).label("centre_point"),
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
            True,
            object_geometries_data.c.territory_id.in_(select(territories_cte.c.territory_id)),
            physical_objects_data.c.physical_object_id == physical_object_id,
            services_data.c.service_id == service_id,
        )
    )

    scenario_urban_objects_query = (
        select(
            projects_urban_objects_data.c.object_geometry_id,
            territories_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
            projects_object_geometries_data.c.address,
            projects_object_geometries_data.c.osm_id,
            ST_AsEWKB(projects_object_geometries_data.c.geometry).label("geometry"),
            ST_AsEWKB(projects_object_geometries_data.c.centre_point).label("centre_point"),
            projects_object_geometries_data.c.created_at,
            projects_object_geometries_data.c.updated_at,
            object_geometries_data.c.object_geometry_id.label("public_object_geometry_id"),
            object_geometries_data.c.address.label("public_address"),
            object_geometries_data.c.osm_id.label("public_osm_id"),
            ST_AsEWKB(object_geometries_data.c.geometry).label("public_geometry"),
            ST_AsEWKB(object_geometries_data.c.centre_point).label("public_centre_point"),
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
    )

    # Act
    with patch("idu_api.urban_api.logic.impl.helpers.projects_geometries.get_project_by_scenario_id") as mock_check:
        result = await get_geometries_by_scenario_id_from_db(
            mock_conn, scenario_id, user, physical_object_id, service_id
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
    user = UserDTO(id="mock_string", is_superuser=False)
    physical_object_type_id = 1
    service_type_id = 1
    building_columns = [col for col in buildings_data.c if col.name not in ("physical_object_id", "properties")]
    project_building_columns = [
        col for col in projects_buildings_data.c if col.name not in ("physical_object_id", "properties")
    ]

    territories_cte = include_child_territories_cte(1)
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
            physical_objects_data.c.physical_object_id,
            physical_object_types_dict.c.physical_object_type_id,
            physical_object_types_dict.c.name.label("physical_object_type_name"),
            physical_objects_data.c.name.label("physical_object_name"),
            physical_objects_data.c.properties.label("physical_object_properties"),
            *building_columns,
            buildings_data.c.properties.label("building_properties"),
            object_geometries_data.c.object_geometry_id,
            territories_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
            object_geometries_data.c.address,
            object_geometries_data.c.osm_id,
            ST_AsEWKB(object_geometries_data.c.geometry).label("geometry"),
            ST_AsEWKB(object_geometries_data.c.centre_point).label("centre_point"),
            services_data.c.service_id,
            services_data.c.name.label("service_name"),
            services_data.c.capacity,
            services_data.c.is_capacity_real,
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
                buildings_data,
                buildings_data.c.physical_object_id == physical_objects_data.c.physical_object_id,
            )
            .join(
                territories_data,
                territories_data.c.territory_id == object_geometries_data.c.territory_id,
            )
        )
        .where(
            urban_objects_data.c.urban_object_id.not_in(select(public_urban_object_ids)),
            True,
            object_geometries_data.c.territory_id.in_(select(territories_cte.c.territory_id)),
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
            *project_building_columns,
            projects_buildings_data.c.properties.label("building_properties"),
            projects_object_geometries_data.c.territory_id,
            projects_object_geometries_data.c.address,
            projects_object_geometries_data.c.osm_id,
            ST_AsEWKB(projects_object_geometries_data.c.geometry).label("geometry"),
            ST_AsEWKB(projects_object_geometries_data.c.centre_point).label("centre_point"),
            projects_services_data.c.name.label("service_name"),
            projects_services_data.c.capacity,
            projects_services_data.c.is_capacity_real,
            projects_services_data.c.properties.label("service_properties"),
            physical_objects_data.c.name.label("public_physical_object_name"),
            physical_objects_data.c.properties.label("public_physical_object_properties"),
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
            object_geometries_data.c.address.label("public_address"),
            object_geometries_data.c.osm_id.label("public_osm_id"),
            ST_AsEWKB(object_geometries_data.c.geometry).label("public_geometry"),
            ST_AsEWKB(object_geometries_data.c.centre_point).label("public_centre_point"),
            services_data.c.name.label("public_service_name"),
            services_data.c.capacity.label("public_capacity"),
            services_data.c.is_capacity_real.label("public_is_capacity_real"),
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
                buildings_data,
                buildings_data.c.physical_object_id == physical_objects_data.c.physical_object_id,
            )
            .outerjoin(
                projects_buildings_data,
                projects_buildings_data.c.physical_object_id == projects_physical_objects_data.c.physical_object_id,
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
    with patch("idu_api.urban_api.logic.impl.helpers.projects_geometries.get_project_by_scenario_id") as mock_check:
        result = await get_geometries_with_all_objects_by_scenario_id_from_db(
            mock_conn, scenario_id, user, physical_object_type_id, service_type_id, None, None
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
    user = UserDTO(id="mock_string", is_superuser=False)
    mock_geom = str(MagicMock(spec=ScalarSelect))
    statement = (
        select(
            object_geometries_data.c.object_geometry_id,
            object_geometries_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
            object_geometries_data.c.address,
            object_geometries_data.c.osm_id,
            ST_AsEWKB(
                case(
                    (
                        ~ST_Within(object_geometries_data.c.geometry, mock_geom),
                        ST_Intersection(object_geometries_data.c.geometry, mock_geom),
                    ),
                    else_=object_geometries_data.c.geometry,
                )
            ).label("geometry"),
            ST_AsEWKB(
                case(
                    (
                        ~ST_Within(object_geometries_data.c.geometry, mock_geom),
                        ST_Centroid(ST_Intersection(object_geometries_data.c.geometry, mock_geom)),
                    ),
                    else_=object_geometries_data.c.centre_point,
                )
            ).label("centre_point"),
            object_geometries_data.c.created_at,
            object_geometries_data.c.updated_at,
        )
        .select_from(
            urban_objects_data.join(
                object_geometries_data,
                object_geometries_data.c.object_geometry_id == urban_objects_data.c.object_geometry_id,
            ).join(
                territories_data,
                territories_data.c.territory_id == object_geometries_data.c.territory_id,
            )
        )
        .where(
            object_geometries_data.c.territory_id.in_([1]) | ST_Intersects(object_geometries_data.c.geometry, mock_geom)
        )
        .distinct()
    )

    # Act
    with patch(
        "idu_api.urban_api.logic.impl.helpers.projects_geometries.get_context_territories_geometry",
        new_callable=AsyncMock,
    ) as mock_get_context:
        mock_get_context.return_value = mock_geom, [1]
        result = await get_context_geometries_from_db(mock_conn, project_id, user, None, None)
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
    user = UserDTO(id="mock_string", is_superuser=False)
    physical_object_type_id = 1
    service_type_id = 1
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
            object_geometries_data.c.territory_id.in_([1]) | ST_Intersects(object_geometries_data.c.geometry, mock_geom)
        )
        .cte(name="objects_intersecting")
    )
    building_columns = [col for col in buildings_data.c if col.name not in ("physical_object_id", "properties")]
    statement = (
        select(
            physical_objects_data.c.physical_object_id,
            physical_object_types_dict.c.physical_object_type_id,
            physical_object_types_dict.c.name.label("physical_object_type_name"),
            physical_objects_data.c.name.label("physical_object_name"),
            physical_objects_data.c.properties.label("physical_object_properties"),
            *building_columns,
            buildings_data.c.properties.label("building_properties"),
            object_geometries_data.c.object_geometry_id,
            territories_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
            object_geometries_data.c.address,
            object_geometries_data.c.osm_id,
            ST_AsEWKB(
                case(
                    (
                        ~ST_Within(object_geometries_data.c.geometry, mock_geom),
                        ST_Intersection(object_geometries_data.c.geometry, mock_geom),
                    ),
                    else_=object_geometries_data.c.geometry,
                )
            ).label("geometry"),
            ST_AsEWKB(
                case(
                    (
                        ~ST_Within(object_geometries_data.c.geometry, mock_geom),
                        ST_Centroid(ST_Intersection(object_geometries_data.c.geometry, mock_geom)),
                    ),
                    else_=object_geometries_data.c.centre_point,
                )
            ).label("centre_point"),
            services_data.c.service_id,
            services_data.c.name.label("service_name"),
            services_data.c.capacity,
            services_data.c.is_capacity_real,
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
            .join(
                objects_intersecting,
                objects_intersecting.c.object_geometry_id == object_geometries_data.c.object_geometry_id,
            )
            .join(
                territories_data,
                territories_data.c.territory_id == object_geometries_data.c.territory_id,
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
                buildings_data,
                buildings_data.c.physical_object_id == physical_objects_data.c.physical_object_id,
            )
        )
        .where(
            physical_object_types_dict.c.physical_object_type_id == physical_object_type_id,
            service_types_dict.c.service_type_id == service_type_id,
        )
    )

    # Act
    with patch(
        "idu_api.urban_api.logic.impl.helpers.projects_geometries.get_context_territories_geometry",
        new_callable=AsyncMock,
    ) as mock_get_context:
        mock_get_context.return_value = mock_geom, [1]
        result = await get_context_geometries_with_all_objects_from_db(
            mock_conn, project_id, user, physical_object_type_id, service_type_id, None, None
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
            ST_AsEWKB(projects_object_geometries_data.c.geometry).label("geometry"),
            ST_AsEWKB(projects_object_geometries_data.c.centre_point).label("centre_point"),
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
    user = UserDTO(id="mock_string", is_superuser=False)
    update_statement = (
        update(projects_object_geometries_data)
        .where(projects_object_geometries_data.c.object_geometry_id == object_geometry_id)
        .values(
            territory_id=object_geometries_put_req.territory_id,
            geometry=ST_GeomFromWKB(object_geometries_put_req.geometry.as_shapely_geometry().wkb, text(str(SRID))),
            centre_point=ST_GeomFromWKB(
                object_geometries_put_req.centre_point.as_shapely_geometry().wkb, text(str(SRID))
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
                mock_conn, object_geometries_put_req, scenario_id, object_geometry_id, is_scenario_object, user
            )
    with patch(
        "idu_api.urban_api.logic.impl.helpers.projects_geometries.check_existence",
        new=AsyncMock(side_effect=check_territory),
    ):
        with pytest.raises(EntityNotFoundById):
            await put_object_geometry_to_db(
                mock_conn, object_geometries_put_req, scenario_id, object_geometry_id, is_scenario_object, user
            )
    result = await put_object_geometry_to_db(
        mock_conn, object_geometries_put_req, scenario_id, object_geometry_id, is_scenario_object, user
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
    user = UserDTO(id="mock_string", is_superuser=False)
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
                mock_conn, object_geometries_patch_req, scenario_id, object_geometry_id, is_scenario_object, user
            )
    result = await patch_object_geometry_to_db(
        mock_conn, object_geometries_patch_req, scenario_id, object_geometry_id, is_scenario_object, user
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
    user = UserDTO(id="mock_string", is_superuser=False)
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
            mock_conn, scenario_id, object_geometry_id, is_scenario_object, user
        )
        mock_check_existence.return_value = False
        with pytest.raises(EntityNotFoundById):
            await delete_object_geometry_from_db(mock_conn, scenario_id, object_geometry_id, is_scenario_object, user)

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
    user = UserDTO(id="mock_string", is_superuser=False)
    delete_statement = delete(projects_object_geometries_data).where(
        projects_object_geometries_data.c.object_geometry_id == object_geometry_id
    )

    # Act
    with patch("idu_api.urban_api.logic.impl.helpers.projects_geometries.check_existence") as mock_check_existence:
        result = await delete_object_geometry_from_db(
            mock_conn, scenario_id, object_geometry_id, is_scenario_object, user
        )
        mock_check_existence.return_value = False
        with pytest.raises(EntityNotFoundById):
            await delete_object_geometry_from_db(mock_conn, scenario_id, object_geometry_id, is_scenario_object, user)

    # Assert
    assert result == {"status": "ok"}, "Result should be {'status': 'ok'}."
    mock_conn.execute_mock.assert_any_call(str(delete_statement))
    mock_conn.commit_mock.assert_called_once()
