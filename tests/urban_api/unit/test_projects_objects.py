"""Unit tests for project objects are defined here."""

import io
from collections.abc import Callable
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest
import structlog
from aioresponses import aioresponses
from aioresponses.core import merge_params, normalize_url
from fastapi_pagination.bases import RawParams
from geoalchemy2.functions import ST_AsEWKB
from otteroad import KafkaProducerClient
from otteroad.models import BaseScenarioCreated, ProjectCreated
from sqlalchemy import and_, delete, func, insert, or_, select, update

from idu_api.common.db.entities import (
    projects_data,
    projects_object_geometries_data,
    projects_phases_data,
    projects_physical_objects_data,
    projects_services_data,
    projects_territory_data,
    scenarios_data,
    territories_data,
)
from idu_api.urban_api.config import UrbanAPIConfig
from idu_api.urban_api.dto import (
    PageDTO,
    ProjectDTO,
    ProjectPhasesDTO,
    ProjectTerritoryDTO,
    ProjectWithTerritoryDTO,
    ScenarioDTO,
    UserDTO,
)
from idu_api.urban_api.logic.impl.helpers.projects_objects import (
    add_project_to_db,
    create_base_scenario_to_db,
    delete_project_from_db,
    get_project_by_id_from_db,
    get_project_phases_by_id_from_db,
    get_project_territory_by_id_from_db,
    get_projects_from_db,
    get_projects_territories_from_db,
    patch_project_to_db,
    put_project_phases_to_db,
    put_project_to_db,
)
from idu_api.urban_api.minio.services import ProjectStorageManager
from idu_api.urban_api.schemas import (
    Project,
    ProjectPatch,
    ProjectPhases,
    ProjectPhasesPut,
    ProjectPost,
    ProjectPut,
    ProjectTerritory,
    Scenario,
)
from idu_api.urban_api.schemas.geometries import GeoJSONResponse
from tests.urban_api.helpers.connection import MockConnection, MockResult, MockRow
from tests.urban_api.helpers.minio_client import MockAsyncMinioClient

func: Callable

####################################################################################
#                           Default use-case tests                                 #
####################################################################################


@pytest.mark.asyncio
async def test_get_project_by_id_from_db(mock_conn: MockConnection):
    """Test the get_project_by_id_from_db function."""

    # Arrange
    project_id = 1
    user = UserDTO(id="mock_string", is_superuser=False)
    regional_scenarios = scenarios_data.alias("regional_scenarios")
    statement = (
        select(
            projects_data,
            territories_data.c.name.label("territory_name"),
            scenarios_data.c.scenario_id,
            scenarios_data.c.name.label("scenario_name"),
        )
        .select_from(
            projects_data.join(territories_data, territories_data.c.territory_id == projects_data.c.territory_id)
            .outerjoin(
                scenarios_data,
                and_(
                    scenarios_data.c.project_id == projects_data.c.project_id,
                    scenarios_data.c.is_based.is_(True),
                    projects_data.c.is_regional.is_(False),
                ),
            )
            .outerjoin(
                regional_scenarios,
                regional_scenarios.c.scenario_id == scenarios_data.c.parent_id,
            )
        )
        .where(
            projects_data.c.project_id == project_id,
            or_(
                projects_data.c.is_regional.is_(True),
                and_(
                    scenarios_data.c.is_based.is_(True),
                    regional_scenarios.c.is_based.is_(True),
                ),
            ),
        )
    )

    # Act
    result = await get_project_by_id_from_db(mock_conn, project_id, user)

    # Assert
    assert isinstance(result, ProjectDTO), "Result should be a ProjectDTO."
    assert isinstance(Project.from_dto(result), Project), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_called_once_with(str(statement))


@pytest.mark.asyncio
@patch("idu_api.urban_api.logic.impl.helpers.projects_objects.check_project")
async def test_get_project_territory_by_id_from_db(mock_check: AsyncMock, mock_conn: MockConnection):
    """Test the get_project_territory_by_id_from_db function."""

    # Arrange
    project_id = 1
    user = UserDTO(id="mock_string", is_superuser=False)
    regional_scenarios = scenarios_data.alias("regional_scenarios")
    statement = (
        select(
            projects_territory_data.c.project_territory_id,
            projects_data.c.project_id,
            projects_data.c.name.label("project_name"),
            projects_data.c.user_id.label("project_user_id"),
            territories_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
            ST_AsEWKB(projects_territory_data.c.geometry).label("geometry"),
            ST_AsEWKB(projects_territory_data.c.centre_point).label("centre_point"),
            projects_territory_data.c.properties,
            scenarios_data.c.scenario_id,
            scenarios_data.c.name.label("scenario_name"),
        )
        .select_from(
            projects_territory_data.join(
                projects_data, projects_data.c.project_id == projects_territory_data.c.project_id
            )
            .join(territories_data, territories_data.c.territory_id == projects_data.c.territory_id)
            .outerjoin(
                scenarios_data,
                scenarios_data.c.project_id == projects_data.c.project_id,
            )
            .outerjoin(
                regional_scenarios,
                regional_scenarios.c.scenario_id == scenarios_data.c.parent_id,
            )
        )
        .where(
            projects_territory_data.c.project_id == project_id,
            scenarios_data.c.is_based.is_(True),
            regional_scenarios.c.is_based.is_(True),
        )
    )

    # Act
    result = await get_project_territory_by_id_from_db(mock_conn, project_id, user)

    # Assert
    assert isinstance(result, ProjectTerritoryDTO), "Result should be a ProjectTerritoryDTO."
    assert isinstance(ProjectTerritory.from_dto(result), ProjectTerritory), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(statement))
    mock_check.assert_called_once_with(mock_conn, project_id, user, allow_regional=False)


@pytest.mark.asyncio
@patch("idu_api.urban_api.utils.pagination.verify_params")
async def test_get_projects_from_db(mock_verify_params, mock_conn: MockConnection):
    """Test the get_projects_from_db function."""

    # Arrange
    user = UserDTO(id="mock_string", is_superuser=False)
    filters = {
        "only_own": False,
        "is_regional": False,
        "project_type": "common",
        "territory_id": 1,
        "name": "mock_string",
        "created_at": datetime.now(timezone.utc),
        "order_by": None,
        "ordering": "asc",
    }
    limit, offset = 10, 0
    regional_scenarios = scenarios_data.alias("regional_scenarios")
    statement = (
        select(
            projects_data,
            territories_data.c.name.label("territory_name"),
            scenarios_data.c.scenario_id,
            scenarios_data.c.name.label("scenario_name"),
        )
        .select_from(
            projects_data.join(
                territories_data,
                territories_data.c.territory_id == projects_data.c.territory_id,
            )
            .outerjoin(
                scenarios_data,
                and_(
                    scenarios_data.c.project_id == projects_data.c.project_id,
                    scenarios_data.c.is_based.is_(True),
                    projects_data.c.is_regional.is_(False),
                ),
            )
            .outerjoin(
                regional_scenarios,
                regional_scenarios.c.scenario_id == scenarios_data.c.parent_id,
            )
        )
        .where(
            projects_data.c.is_regional.is_(filters["is_regional"]),
            or_(
                projects_data.c.is_regional.is_(True),
                and_(
                    scenarios_data.c.is_based.is_(True),
                    regional_scenarios.c.is_based.is_(True),
                ),
            ),
            or_(projects_data.c.user_id == user.id, projects_data.c.public.is_(True)),
            projects_data.c.is_city.is_(False),
            projects_data.c.territory_id == filters["territory_id"],
            projects_data.c.name.ilike(f'%{filters["name"]}%'),
            func.date(projects_data.c.created_at) >= filters["created_at"],
        )
        .order_by(projects_data.c.project_id)
        .offset(offset)
        .limit(limit)
    )
    mock_verify_params.return_value = (None, RawParams(limit=limit, offset=offset))

    # Act
    list_result = await get_projects_from_db(mock_conn, user, **filters, paginate=False)
    paginate_result = await get_projects_from_db(mock_conn, user, **filters, paginate=True)

    # Assert
    assert isinstance(list_result, list), "Result should be a list."
    assert all(
        isinstance(item, ProjectDTO) for item in list_result
    ), "Each item should be a ProjectWithBaseScenarioDTO."
    assert isinstance(Project.from_dto(list_result[0]), Project), "Couldn't create pydantic model from DTO."
    assert isinstance(paginate_result, PageDTO), "Result should be a PageDTO."
    assert all(
        isinstance(item, ProjectDTO) for item in paginate_result.items
    ), "Each item should be a ProjectWithBaseScenarioDTO."
    assert isinstance(Project.from_dto(paginate_result.items[0]), Project), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(statement))


@pytest.mark.asyncio
async def test_get_projects_territories_from_db(mock_conn: MockConnection):
    """Test the get_projects_territories_from_db function."""

    # Arrange
    user = UserDTO(id="mock_string", is_superuser=False)
    regional_scenarios = scenarios_data.alias("regional_scenarios")
    statement = (
        select(
            projects_data,
            territories_data.c.name.label("territory_name"),
            ST_AsEWKB(projects_territory_data.c.geometry).label("geometry"),
            ST_AsEWKB(projects_territory_data.c.centre_point).label("centre_point"),
            scenarios_data.c.scenario_id,
            scenarios_data.c.name.label("scenario_name"),
        )
        .select_from(
            projects_data.join(territories_data, territories_data.c.territory_id == projects_data.c.territory_id)
            .join(scenarios_data, scenarios_data.c.project_id == projects_data.c.project_id)
            .join(regional_scenarios, regional_scenarios.c.scenario_id == scenarios_data.c.parent_id)
            .join(projects_territory_data, projects_territory_data.c.project_id == projects_data.c.project_id)
        )
        .where(
            scenarios_data.c.is_based.is_(True),
            regional_scenarios.c.is_based.is_(True),
            projects_data.c.is_regional.is_(False),
        )
    )
    statement_with_filters = statement.where(
        projects_data.c.user_id == user.id,
        projects_data.c.is_city.is_(False),
        projects_data.c.territory_id == 1,
    )
    statement = statement.where(or_(projects_data.c.user_id == user.id, projects_data.c.public.is_(True)))

    # Act
    await get_projects_territories_from_db(mock_conn, user, True, "common", 1)
    result = await get_projects_territories_from_db(mock_conn, user, False, None, None)
    geojson_result = await GeoJSONResponse.from_list([r.to_geojson_dict() for r in result])

    # Assert
    assert isinstance(result, list), "Result should be a list."
    assert all(
        isinstance(item, ProjectWithTerritoryDTO) for item in result
    ), "Each item should be a ProjectWithBaseScenarioDTO."
    assert isinstance(
        Project(**geojson_result.features[0].properties), Project
    ), "Couldn't create pydantic model from geojson properties."
    mock_conn.execute_mock.assert_any_call(str(statement))
    mock_conn.execute_mock.assert_any_call(str(statement_with_filters))


@pytest.mark.asyncio
async def test_add_project_to_db(config: UrbanAPIConfig, mock_conn: MockConnection, project_post_req: ProjectPost):
    """Test the add_project_to_db function."""

    # Arrange
    user = UserDTO(id="mock_string", is_superuser=False)
    statement_for_project = (
        insert(projects_data)
        .values(
            user_id=user.id,
            territory_id=project_post_req.territory_id,
            name=project_post_req.name,
            description=project_post_req.description,
            public=project_post_req.public,
            is_regional=False,
            is_city=project_post_req.is_city,
            properties=project_post_req.properties,
        )
        .returning(projects_data.c.project_id)
    )
    statement_for_base_scenario = (
        insert(scenarios_data)
        .values(
            project_id=1,
            functional_zone_type_id=None,
            name="Исходный пользовательский сценарий",
            is_based=True,
            parent_id=1,
        )
        .returning(scenarios_data.c.scenario_id)
    )
    scenario_id = 1
    project_id = 1
    logger: structlog.stdlib.BoundLogger = structlog.get_logger()
    api_url = f"{config.external.hextech_api}/hextech/indicators_saving/save_all"
    params = {
        "scenario_id": scenario_id,
        "project_id": project_id,
        "background": "true",
    }
    normal_api_url = normalize_url(merge_params(api_url, params))
    kafka_producer = AsyncMock(spec=KafkaProducerClient)
    event = ProjectCreated(project_id=1, base_scenario_id=1, territory_id=1)
    project_storage_manager = AsyncMock(spec=ProjectStorageManager)

    # Act
    with aioresponses() as mocked:
        mocked.put(normal_api_url, status=200)
        result = await add_project_to_db(
            mock_conn, project_post_req, user, kafka_producer, project_storage_manager, logger
        )

    # Assert
    assert isinstance(result, ProjectDTO), "Result should be a ProjectDTO."
    mock_conn.execute_mock.assert_any_call(str(statement_for_project))
    assert any(
        "INSERT INTO user_projects.projects_territory_data" in str(args[0])
        for args in mock_conn.execute_mock.call_args_list
    ), "Expected insertion into user_projects.projects_territory_data table not found."
    mock_conn.execute_mock.assert_any_call(str(statement_for_base_scenario))
    assert any(
        "INSERT INTO user_projects.functional_zones_data" in str(args[0])
        for args in mock_conn.execute_mock.call_args_list
    ), "Expected insertion into user_projects.functional_zones_data table not found."
    assert any(
        "INSERT INTO user_projects.object_geometries_data" in str(args[0])
        for args in mock_conn.execute_mock.call_args_list
    ), "Expected insertion into user_projects.object_geometries_data table not found."
    assert any(
        "INSERT INTO user_projects.urban_objects_data" in str(args[0]) for args in mock_conn.execute_mock.call_args_list
    ), "Expected insertion into user_projects.urban_objects_data table not found."
    mock_conn.commit_mock.assert_called_once()
    mocked.assert_called_once_with(
        url=api_url,
        method="PUT",
        data=None,
        params=params,
    )
    assert mocked.requests[("PUT", normal_api_url)][0].kwargs["params"] == params, "Request params do not match."
    kafka_producer.send.assert_any_call(event)
    project_storage_manager.init_project.assert_called_once()


@pytest.mark.asyncio
async def test_create_base_scenario_to_db(config: UrbanAPIConfig, project_post_req: ProjectPost):
    """Test the create_base_scenario_to_db function."""

    # Arrange
    project_id = 1
    scenario_id = 1
    check_project_statement = (
        select(projects_data, projects_territory_data.c.geometry)
        .select_from(
            projects_data.outerjoin(
                projects_territory_data,
                projects_territory_data.c.project_id == projects_data.c.project_id,
            )
        )
        .where(projects_data.c.project_id == project_id)
    )
    check_scenario_statement = (
        select(scenarios_data, projects_data.c.is_regional)
        .select_from(scenarios_data.join(projects_data, projects_data.c.project_id == scenarios_data.c.project_id))
        .where(scenarios_data.c.scenario_id == scenario_id)
    )
    logger: structlog.stdlib.BoundLogger = structlog.get_logger("test")
    api_url = f"{config.external.hextech_api}/hextech/indicators_saving/save_all"
    params = {
        "scenario_id": scenario_id,
        "project_id": project_id,
        "background": "true",
    }
    normal_api_url = normalize_url(merge_params(api_url, params))
    kafka_producer = AsyncMock(spec=KafkaProducerClient)
    event = BaseScenarioCreated(
        project_id=project_id,
        base_scenario_id=1,
        regional_scenario_id=scenario_id,
    )
    data = {"is_regional": False, "territory_id": 1, "geometry": "geom"}
    preset_results = [MockResult([MockRow(**data)])]
    mock_conn = MockConnection(preset_results=preset_results)

    # Act
    with patch("idu_api.urban_api.logic.impl.helpers.projects_objects.check_existence") as mock_check:
        mock_check.return_value = False
        with aioresponses() as mocked:
            mocked.put(normal_api_url, status=200)
            result = await create_base_scenario_to_db(mock_conn, project_id, scenario_id, kafka_producer, logger)

    # Assert
    assert isinstance(result, ScenarioDTO), "Result should be a ScenarioDTO."
    assert isinstance(Scenario.from_dto(result), Scenario), "Couldn't create Pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(check_project_statement))
    mock_conn.execute_mock.assert_any_call(str(check_scenario_statement))
    assert any(
        "INSERT INTO user_projects.functional_zones_data" in str(args[0])
        for args in mock_conn.execute_mock.call_args_list
    ), "Expected insertion into user_projects.functional_zones_data table not found."
    assert any(
        "INSERT INTO user_projects.object_geometries_data" in str(args[0])
        for args in mock_conn.execute_mock.call_args_list
    ), "Expected insertion into user_projects.object_geometries_data table not found."
    assert any(
        "INSERT INTO user_projects.urban_objects_data" in str(args[0]) for args in mock_conn.execute_mock.call_args_list
    ), "Expected insertion into user_projects.urban_objects_data table not found."
    assert any(
        "SELECT regional_urban_objects" in str(args[0]) for args in mock_conn.execute_mock.call_args_list
    ), "Expected selection urban objects from regional scenario not found."
    mock_conn.commit_mock.assert_called_once()
    mocked.assert_called_once_with(
        url=api_url,
        method="PUT",
        data=None,
        params=params,
    )
    assert mocked.requests[("PUT", normal_api_url)][0].kwargs["params"] == params, "Request params do not match."
    kafka_producer.send.assert_any_call(event)


@pytest.mark.asyncio
@patch("idu_api.urban_api.logic.impl.helpers.projects_objects.check_project")
async def test_put_project_to_db(mock_check: AsyncMock, mock_conn: MockConnection, project_put_req: ProjectPut):
    """Test the put_project_to_db function."""

    # Arrange
    project_id = 1
    user = UserDTO(id="mock_string", is_superuser=False)
    update_statement = (
        update(projects_data)
        .where(projects_data.c.project_id == project_id)
        .values(**project_put_req.model_dump(), updated_at=datetime.now(timezone.utc))
    )

    # Act
    result = await put_project_to_db(mock_conn, project_put_req, project_id, user)

    # Assert
    assert isinstance(result, ProjectDTO), "Result should be a ProjectDTO"
    mock_conn.execute_mock.assert_any_call(str(update_statement))
    mock_conn.commit_mock.assert_called_once()
    mock_check.assert_called_once_with(mock_conn, project_id, user, to_edit=True)


@pytest.mark.asyncio
@patch("idu_api.urban_api.logic.impl.helpers.projects_objects.check_project")
async def test_patch_project_to_db(mock_check: AsyncMock, mock_conn: MockConnection, project_patch_req: ProjectPatch):
    """Test the patch_project_to_db function."""

    # Arrange
    project_id = 1
    user = UserDTO(id="mock_string", is_superuser=False)
    update_statement = (
        update(projects_data)
        .where(projects_data.c.project_id == project_id)
        .values(**project_patch_req.model_dump(exclude_unset=True), updated_at=datetime.now(timezone.utc))
        .returning(projects_data)
    )

    # Act
    result = await patch_project_to_db(mock_conn, project_patch_req, project_id, user)

    # Assert
    assert isinstance(result, ProjectDTO), "Result should be a ProjectDTO."
    mock_conn.execute_mock.assert_any_call(str(update_statement))
    mock_conn.commit_mock.assert_called_once()
    mock_check.assert_called_once_with(mock_conn, project_id, user, to_edit=True)


@pytest.mark.asyncio
@patch("idu_api.urban_api.logic.impl.helpers.projects_objects.check_project")
async def test_delete_project_from_db(
    mock_check: AsyncMock, mock_conn: MockConnection, mock_minio_client: MockAsyncMinioClient, project_image: io.BytesIO
):
    """Test the delete_project_from_db function."""

    # Arrange
    project_id = 1
    user = UserDTO(id="mock_string", is_superuser=False)
    logger: structlog.stdlib.BoundLogger = structlog.get_logger()
    delete_geometry_statement = delete(projects_object_geometries_data).where(
        projects_object_geometries_data.c.object_geometry_id.in_([1])
    )
    delete_physical_statement = delete(projects_physical_objects_data).where(
        projects_physical_objects_data.c.physical_object_id.in_([1])
    )
    delete_service_statement = delete(projects_services_data).where(projects_services_data.c.service_id.in_([1]))
    delete_statement = delete(projects_data).where(projects_data.c.project_id == project_id)
    await mock_minio_client.upload_file(..., project_image, f"projects/{project_id}/", logger)
    project_storage_manager = AsyncMock(spec=ProjectStorageManager)

    # Act
    result = await delete_project_from_db(mock_conn, project_id, project_storage_manager, user, logger)

    # Assert
    assert result == {"status": "ok"}, "Result should be {'status': 'ok'}."
    mock_conn.execute_mock.assert_any_call(str(delete_geometry_statement))
    mock_conn.execute_mock.assert_any_call(str(delete_physical_statement))
    mock_conn.execute_mock.assert_any_call(str(delete_service_statement))
    mock_conn.execute_mock.assert_any_call(str(delete_statement))
    mock_conn.commit_mock.assert_called_once()
    project_storage_manager.delete_project.assert_called_once_with(project_id, logger)
    mock_check.assert_called_once_with(mock_conn, project_id, user, to_edit=True)


@pytest.mark.asyncio
@patch("idu_api.urban_api.logic.impl.helpers.projects_objects.check_project")
async def test_get_project_phases_by_id_from_db(mock_check: AsyncMock, mock_conn: MockConnection):
    """Test the get_project_phases_by_id_from_db function."""

    # Arrange
    project_id = 1
    user = UserDTO(id="mock_string", is_superuser=False)
    statement = (
        select(
            projects_phases_data.c.actual_start_date,
            projects_phases_data.c.planned_start_date,
            projects_phases_data.c.actual_end_date,
            projects_phases_data.c.planned_end_date,
            projects_phases_data.c.investment,
            projects_phases_data.c.pre_design,
            projects_phases_data.c.design,
            projects_phases_data.c.construction,
            projects_phases_data.c.operation,
            projects_phases_data.c.decommission,
        )
        .select_from(projects_phases_data)
        .where(projects_phases_data.c.project_id == project_id)
    )

    # Act
    result = await get_project_phases_by_id_from_db(mock_conn, project_id, user)

    # Assert
    assert isinstance(result, ProjectPhasesDTO), "Result should be a ProjectPhasesDTO."
    assert isinstance(ProjectPhases.from_dto(result), ProjectPhases), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(statement))
    mock_check.assert_called_once_with(mock_conn, project_id, user, allow_regional=False)


@pytest.mark.asyncio
@patch("idu_api.urban_api.logic.impl.helpers.projects_objects.check_project")
async def test_put_project_phases_to_db(
    mock_check: AsyncMock, mock_conn: MockConnection, project_phases_put_req: ProjectPhasesPut
):
    """Test the get_project_phases_by_id_from_db function."""

    # Arrange

    project_id = 1
    user = UserDTO(id="mock_string", is_superuser=False)

    update_statement = (
        update(projects_phases_data)
        .where(projects_phases_data.c.project_id == project_id)
        .values(**project_phases_put_req.model_dump())
    )

    # Act
    result = await put_project_phases_to_db(mock_conn, project_id, project_phases_put_req, user)

    # Assert
    assert isinstance(result, ProjectPhasesDTO), "Result should be a ProjectPhasesDTO."
    assert isinstance(ProjectPhases.from_dto(result), ProjectPhases), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(update_statement))
    mock_check.assert_any_call(mock_conn, project_id, user, to_edit=True, allow_regional=False)
