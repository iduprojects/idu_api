"""Unit tests for scenario services objects are defined here."""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from geoalchemy2.functions import ST_AsEWKB, ST_Intersects, ST_Within
from sqlalchemy import ScalarSelect, delete, insert, literal, or_, select, union_all, update
from sqlalchemy.sql.functions import coalesce

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
from idu_api.urban_api.dto import (
    ScenarioServiceDTO,
    ScenarioServiceWithGeometryDTO,
    ScenarioUrbanObjectDTO,
    ServiceDTO,
    ServiceWithGeometryDTO,
    UserDTO,
)
from idu_api.urban_api.exceptions.logic.common import EntityNotFoundById
from idu_api.urban_api.logic.impl.helpers.projects_services import (
    add_service_to_db,
    delete_service_from_db,
    get_context_services_from_db,
    get_context_services_with_geometry_from_db,
    get_scenario_service_by_id_from_db,
    get_services_by_scenario_id_from_db,
    get_services_with_geometry_by_scenario_id_from_db,
    patch_service_to_db,
    put_service_to_db,
)
from idu_api.urban_api.logic.impl.helpers.utils import get_context_territories_geometry, include_child_territories_cte
from idu_api.urban_api.schemas import (
    ScenarioService,
    ScenarioServicePost,
    ScenarioServiceWithGeometryAttributes,
    ScenarioUrbanObject,
    Service,
    ServicePatch,
    ServicePut,
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

    territories_cte = include_child_territories_cte(1)
    public_urban_object_ids = (
        select(projects_urban_objects_data.c.public_urban_object_id)
        .where(projects_urban_objects_data.c.scenario_id == scenario_id)
        .where(projects_urban_objects_data.c.public_urban_object_id.isnot(None))
    ).cte(name="public_urban_object_ids")

    public_urban_objects_query = (
        select(
            services_data.c.service_id,
            services_data.c.name,
            services_data.c.capacity,
            services_data.c.is_capacity_real,
            services_data.c.properties,
            services_data.c.created_at,
            services_data.c.updated_at,
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
            literal(False).label("is_scenario_object"),
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
            True,
            object_geometries_data.c.territory_id.in_(select(territories_cte.c.territory_id)),
            service_types_dict.c.service_type_id == service_type_id,
        )
    )

    scenario_urban_objects_query = (
        select(
            coalesce(projects_services_data.c.service_id, services_data.c.service_id).label("service_id"),
            coalesce(projects_services_data.c.name, services_data.c.name).label("name"),
            coalesce(projects_services_data.c.capacity, services_data.c.capacity).label("capacity"),
            coalesce(
                projects_services_data.c.is_capacity_real,
                services_data.c.is_capacity_real,
            ).label("is_capacity_real"),
            coalesce(projects_services_data.c.properties, services_data.c.properties).label("properties"),
            coalesce(projects_services_data.c.created_at, services_data.c.created_at).label("created_at"),
            coalesce(projects_services_data.c.updated_at, services_data.c.updated_at).label("updated_at"),
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
            (projects_urban_objects_data.c.service_id.isnot(None)).label("is_scenario_object"),
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
            (
                projects_urban_objects_data.c.service_id.isnot(None)
                | projects_urban_objects_data.c.public_service_id.isnot(None)
            ),
            service_types_dict.c.service_type_id == service_type_id,
        )
    )
    union_query = union_all(public_urban_objects_query, scenario_urban_objects_query)

    # Act
    result = await get_services_by_scenario_id_from_db(mock_conn, scenario_id, user, service_type_id, urban_function_id)

    # Assert
    assert isinstance(result, list), "Result should be a list."
    assert all(isinstance(item, ScenarioServiceDTO) for item in result), "Each item should be a ScenarioServiceDTO."
    assert isinstance(ScenarioService.from_dto(result[0]), ScenarioService), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(union_query))


@pytest.mark.asyncio
async def test_get_services_with_geometry_by_scenario_id_from_db(mock_conn: MockConnection):
    """Test the get_services_with_geometry_by_scenario_id_from_db function."""

    # Arrange
    scenario_id = 1
    user = UserDTO(id="mock_string", is_superuser=False)
    service_type_id = 1
    urban_function_id = None

    territories_cte = include_child_territories_cte(1)
    public_urban_object_ids = (
        select(projects_urban_objects_data.c.public_urban_object_id)
        .where(projects_urban_objects_data.c.scenario_id == scenario_id)
        .where(projects_urban_objects_data.c.public_urban_object_id.isnot(None))
    ).cte(name="public_urban_object_ids")

    public_urban_objects_query = (
        select(
            services_data.c.service_id,
            services_data.c.name,
            services_data.c.capacity,
            services_data.c.is_capacity_real,
            services_data.c.properties,
            services_data.c.created_at,
            services_data.c.updated_at,
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
            object_geometries_data.c.object_geometry_id,
            object_geometries_data.c.address,
            object_geometries_data.c.osm_id,
            ST_AsEWKB(object_geometries_data.c.geometry).label("geometry"),
            ST_AsEWKB(object_geometries_data.c.centre_point).label("centre_point"),
            territories_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
            literal(False).label("is_scenario_service"),
            literal(False).label("is_scenario_geometry"),
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
            True,
            object_geometries_data.c.territory_id.in_(select(territories_cte.c.territory_id)),
            service_types_dict.c.service_type_id == service_type_id,
        )
    )
    scenario_urban_objects_query = (
        select(
            coalesce(projects_services_data.c.service_id, services_data.c.service_id).label("service_id"),
            coalesce(projects_services_data.c.name, services_data.c.name).label("name"),
            coalesce(projects_services_data.c.capacity, services_data.c.capacity).label("capacity"),
            coalesce(
                projects_services_data.c.is_capacity_real,
                services_data.c.is_capacity_real,
            ).label("is_capacity_real"),
            coalesce(projects_services_data.c.properties, services_data.c.properties).label("properties"),
            coalesce(projects_services_data.c.created_at, services_data.c.created_at).label("created_at"),
            coalesce(projects_services_data.c.updated_at, services_data.c.updated_at).label("updated_at"),
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
            coalesce(
                projects_object_geometries_data.c.object_geometry_id,
                object_geometries_data.c.object_geometry_id,
            ).label("object_geometry_id"),
            coalesce(
                projects_object_geometries_data.c.address,
                object_geometries_data.c.address,
            ).label("address"),
            coalesce(
                projects_object_geometries_data.c.osm_id,
                object_geometries_data.c.osm_id,
            ).label("osm_id"),
            ST_AsEWKB(
                coalesce(
                    projects_object_geometries_data.c.geometry,
                    object_geometries_data.c.geometry,
                ),
            ).label("geometry"),
            ST_AsEWKB(
                coalesce(
                    projects_object_geometries_data.c.centre_point,
                    object_geometries_data.c.centre_point,
                ),
            ).label("centre_point"),
            territories_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
            (projects_urban_objects_data.c.service_id.isnot(None)).label("is_scenario_service"),
            (projects_urban_objects_data.c.object_geometry_id.isnot(None)).label("is_scenario_geometry"),
        )
        .select_from(
            projects_urban_objects_data.outerjoin(
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
                projects_services_data, projects_services_data.c.service_id == projects_urban_objects_data.c.service_id
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
        .where(
            projects_urban_objects_data.c.scenario_id == scenario_id,
            projects_urban_objects_data.c.public_urban_object_id.is_(None),
            (
                projects_urban_objects_data.c.service_id.isnot(None)
                | projects_urban_objects_data.c.public_service_id.isnot(None)
            ),
            service_types_dict.c.service_type_id == service_type_id,
        )
    )
    union_query = union_all(public_urban_objects_query, scenario_urban_objects_query)

    # Act
    result = await get_services_with_geometry_by_scenario_id_from_db(
        mock_conn, scenario_id, user, service_type_id, urban_function_id
    )
    geojson_result = await GeoJSONResponse.from_list([r.to_geojson_dict() for r in result])

    # Assert
    assert isinstance(result, list), "Result should be a list."
    assert all(
        isinstance(item, ScenarioServiceWithGeometryDTO) for item in result
    ), "Each item should be a ScenarioServiceWithGeometryDTO."
    assert isinstance(
        ScenarioServiceWithGeometryAttributes(**geojson_result.features[0].properties),
        ScenarioServiceWithGeometryAttributes,
    ), "Couldn't create pydantic model from geojson properties."
    mock_conn.execute_mock.assert_any_call(str(union_query))


@pytest.mark.asyncio
async def test_get_context_services_from_db(mock_conn: MockConnection):
    """Test the get_context_services_from_db function."""

    # Arrange
    project_id = 1
    user = UserDTO(id="mock_string", is_superuser=False)
    service_type_id = 1
    urban_function_id = None
    mock_geom = str(MagicMock(spec=ScalarSelect))

    public_urban_object_ids = (
        select(projects_urban_objects_data.c.public_urban_object_id)
        .where(projects_urban_objects_data.c.scenario_id == 1)
        .where(projects_urban_objects_data.c.public_urban_object_id.isnot(None))
    ).cte(name="public_urban_object_ids")
    objects_intersecting = (
        select(object_geometries_data.c.object_geometry_id)
        .select_from(
            object_geometries_data.join(
                urban_objects_data,
                urban_objects_data.c.object_geometry_id == object_geometries_data.c.object_geometry_id,
            )
        )
        .where(
            urban_objects_data.c.urban_object_id.not_in(select(public_urban_object_ids)),
            object_geometries_data.c.territory_id.in_([1])
            | ST_Intersects(object_geometries_data.c.geometry, mock_geom),
        )
        .cte(name="objects_intersecting")
    )
    public_services_query = (
        select(
            services_data.c.service_id,
            services_data.c.name,
            services_data.c.capacity,
            services_data.c.is_capacity_real,
            services_data.c.properties,
            services_data.c.created_at,
            services_data.c.updated_at,
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
            literal(False).label("is_scenario_object"),
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
    scenario_services_query = (
        select(
            coalesce(projects_services_data.c.service_id, services_data.c.service_id).label("service_id"),
            coalesce(projects_services_data.c.name, services_data.c.name).label("name"),
            coalesce(projects_services_data.c.capacity, services_data.c.capacity).label("capacity"),
            coalesce(
                projects_services_data.c.is_capacity_real,
                services_data.c.is_capacity_real,
            ).label("is_capacity_real"),
            coalesce(projects_services_data.c.properties, services_data.c.properties).label("properties"),
            coalesce(projects_services_data.c.created_at, services_data.c.created_at).label("created_at"),
            coalesce(projects_services_data.c.updated_at, services_data.c.updated_at).label("updated_at"),
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
            (projects_urban_objects_data.c.service_id.isnot(None)).label("is_scenario_object"),
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
            projects_urban_objects_data.c.scenario_id == 1,
            projects_urban_objects_data.c.public_urban_object_id.is_(None),
            (
                projects_urban_objects_data.c.service_id.isnot(None)
                | projects_urban_objects_data.c.public_service_id.isnot(None)
            ),
            service_types_dict.c.service_type_id == service_type_id,
        )
    )
    union_query = union_all(public_services_query, scenario_services_query)

    # Act
    with patch(
        "idu_api.urban_api.logic.impl.helpers.projects_services.get_context_territories_geometry",
        new_callable=AsyncMock,
    ) as mock_get_context:
        mock_get_context.return_value = 1, mock_geom, [1]
        result = await get_context_services_from_db(mock_conn, project_id, user, service_type_id, urban_function_id)

    # Assert
    assert isinstance(result, list), "Result should be a list."
    assert all(isinstance(item, ScenarioServiceDTO) for item in result), "Each item should be a ScenarioServiceDTO."
    assert isinstance(ScenarioService.from_dto(result[0]), ScenarioService), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(union_query))


@pytest.mark.asyncio
async def test_get_context_services_with_geometry_from_db(mock_conn: MockConnection):
    """Test the get_context_services_with_geometry_from_db function."""

    # Arrange
    project_id = 1
    user = UserDTO(id="mock_string", is_superuser=False)
    service_type_id = 1
    urban_function_id = None
    mock_geom = str(MagicMock(spec=ScalarSelect))

    public_urban_object_ids = (
        select(projects_urban_objects_data.c.public_urban_object_id)
        .where(projects_urban_objects_data.c.scenario_id == 1)
        .where(projects_urban_objects_data.c.public_urban_object_id.isnot(None))
    ).cte(name="public_urban_object_ids")
    objects_intersecting = (
        select(object_geometries_data.c.object_geometry_id)
        .select_from(
            object_geometries_data.join(
                urban_objects_data,
                urban_objects_data.c.object_geometry_id == object_geometries_data.c.object_geometry_id,
            )
        )
        .where(
            urban_objects_data.c.urban_object_id.not_in(select(public_urban_object_ids)),
            object_geometries_data.c.territory_id.in_([1])
            | ST_Intersects(object_geometries_data.c.geometry, mock_geom),
        )
        .cte(name="objects_intersecting")
    )
    public_services_query = (
        select(
            services_data.c.service_id,
            services_data.c.name,
            services_data.c.capacity,
            services_data.c.is_capacity_real,
            services_data.c.properties,
            services_data.c.created_at,
            services_data.c.updated_at,
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
            object_geometries_data.c.object_geometry_id,
            object_geometries_data.c.address,
            object_geometries_data.c.osm_id,
            ST_AsEWKB(object_geometries_data.c.geometry).label("geometry"),
            ST_AsEWKB(object_geometries_data.c.centre_point).label("centre_point"),
            territories_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
            literal(False).label("is_scenario_service"),
            literal(False).label("is_scenario_geometry"),
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
    scenario_services_query = (
        select(
            coalesce(projects_services_data.c.service_id, services_data.c.service_id).label("service_id"),
            coalesce(projects_services_data.c.name, services_data.c.name).label("name"),
            coalesce(projects_services_data.c.capacity, services_data.c.capacity).label("capacity"),
            coalesce(
                projects_services_data.c.is_capacity_real,
                services_data.c.is_capacity_real,
            ).label("is_capacity_real"),
            coalesce(projects_services_data.c.properties, services_data.c.properties).label("properties"),
            coalesce(projects_services_data.c.created_at, services_data.c.created_at).label("created_at"),
            coalesce(projects_services_data.c.updated_at, services_data.c.updated_at).label("updated_at"),
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
            coalesce(
                projects_object_geometries_data.c.object_geometry_id,
                object_geometries_data.c.object_geometry_id,
            ).label("object_geometry_id"),
            coalesce(
                projects_object_geometries_data.c.address,
                object_geometries_data.c.address,
            ).label("address"),
            coalesce(
                projects_object_geometries_data.c.osm_id,
                object_geometries_data.c.osm_id,
            ).label("osm_id"),
            ST_AsEWKB(
                coalesce(
                    projects_object_geometries_data.c.geometry,
                    object_geometries_data.c.geometry,
                ),
            ).label("geometry"),
            ST_AsEWKB(
                coalesce(
                    projects_object_geometries_data.c.centre_point,
                    object_geometries_data.c.centre_point,
                ),
            ).label("centre_point"),
            territories_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
            (projects_urban_objects_data.c.service_id.isnot(None)).label("is_scenario_service"),
            (projects_urban_objects_data.c.object_geometry_id.isnot(None)).label("is_scenario_geometry"),
        )
        .select_from(
            projects_urban_objects_data.outerjoin(
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
                projects_services_data, projects_services_data.c.service_id == projects_urban_objects_data.c.service_id
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
        .where(
            projects_urban_objects_data.c.scenario_id == 1,
            projects_urban_objects_data.c.public_urban_object_id.is_(None),
            (
                projects_urban_objects_data.c.service_id.isnot(None)
                | projects_urban_objects_data.c.public_service_id.isnot(None)
            ),
            service_types_dict.c.service_type_id == service_type_id,
        )
    )
    union_query = union_all(public_services_query, scenario_services_query)

    # Act
    with patch(
        "idu_api.urban_api.logic.impl.helpers.projects_services.get_context_territories_geometry",
        new_callable=AsyncMock,
    ) as mock_get_context:
        mock_get_context.return_value = 1, mock_geom, [1]
        result = await get_context_services_with_geometry_from_db(
            mock_conn, project_id, user, service_type_id, urban_function_id
        )
    geojson_result = await GeoJSONResponse.from_list([r.to_geojson_dict() for r in result])

    # Assert
    assert isinstance(result, list), "Result should be a list."
    assert all(
        isinstance(item, ScenarioServiceWithGeometryDTO) for item in result
    ), "Each item should be a ScenarioServiceWithGeometryDTO."
    assert isinstance(
        ScenarioServiceWithGeometryAttributes(**geojson_result.features[0].properties),
        ScenarioServiceWithGeometryAttributes,
    ), "Couldn't create pydantic model from geojson properties."
    mock_conn.execute_mock.assert_any_call(str(union_query))


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
    territories_cte = include_child_territories_cte(1)
    public_urban_object_ids = (
        select(projects_urban_objects_data.c.public_urban_object_id.label("urban_object_id"))
        .where(
            projects_urban_objects_data.c.scenario_id == scenario_id,
            projects_urban_objects_data.c.public_urban_object_id.is_not(None),
        )
        .cte(name="public_urban_object_ids")
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
            True,
            object_geometries_data.c.territory_id.in_(select(territories_cte.c.territory_id)),
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
