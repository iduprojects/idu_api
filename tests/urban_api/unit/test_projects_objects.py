"""Unit tests for project objects are defined here."""

import io
from collections.abc import Callable
from datetime import datetime, timezone
from unittest.mock import AsyncMock, call, patch

import pytest
import structlog
from aioresponses import aioresponses
from aioresponses.core import merge_params, normalize_url
from fastapi_pagination.bases import RawParams
from geoalchemy2.functions import ST_AsGeoJSON
from sqlalchemy import cast, delete, func, insert, or_, select, update
from sqlalchemy.dialects.postgresql import JSONB

from idu_api.common.db.entities import (
    projects_data,
    projects_object_geometries_data,
    projects_physical_objects_data,
    projects_services_data,
    projects_territory_data,
    scenarios_data,
    territories_data,
)
from idu_api.urban_api.dto import PageDTO, ProjectDTO, ProjectTerritoryDTO, ProjectWithTerritoryDTO
from idu_api.urban_api.logic.impl.helpers.projects_objects import (
    add_project_to_db,
    delete_project_from_db,
    get_full_project_image_from_minio,
    get_full_project_image_url_from_minio,
    get_preview_project_image_from_minio,
    get_preview_projects_images_from_minio,
    get_preview_projects_images_url_from_minio,
    get_project_by_id_from_db,
    get_project_territory_by_id_from_db,
    get_projects_from_db,
    get_projects_territories_from_db,
    get_user_preview_projects_images_from_minio,
    get_user_preview_projects_images_url_from_minio,
    get_user_projects_from_db,
    patch_project_to_db,
    put_project_to_db,
    upload_project_image_to_minio,
)
from idu_api.urban_api.logic.impl.helpers.utils import DECIMAL_PLACES
from idu_api.urban_api.schemas import (
    Project,
    ProjectPatch,
    ProjectPost,
    ProjectPut,
    ProjectTerritory,
)
from idu_api.urban_api.schemas.geometries import GeoJSONResponse
from tests.urban_api.helpers.connection import MockConnection
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
    user_id = "mock_string"
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
            ).join(scenarios_data, scenarios_data.c.project_id == projects_data.c.project_id)
        )
        .where(projects_data.c.project_id == project_id, scenarios_data.c.is_based.is_(True))
    )

    # Act
    result = await get_project_by_id_from_db(mock_conn, project_id, user_id)

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
    user_id = "mock_string"
    statement = (
        select(
            projects_territory_data.c.project_territory_id,
            projects_data.c.project_id,
            projects_data.c.name.label("project_name"),
            projects_data.c.user_id.label("project_user_id"),
            territories_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
            cast(ST_AsGeoJSON(projects_territory_data.c.geometry, DECIMAL_PLACES), JSONB).label("geometry"),
            cast(ST_AsGeoJSON(projects_territory_data.c.centre_point, DECIMAL_PLACES), JSONB).label("centre_point"),
            projects_territory_data.c.properties,
            scenarios_data.c.scenario_id,
            scenarios_data.c.name.label("scenario_name"),
        )
        .select_from(
            projects_territory_data.join(
                projects_data, projects_data.c.project_id == projects_territory_data.c.project_id
            )
            .join(territories_data, territories_data.c.territory_id == projects_data.c.territory_id)
            .join(
                scenarios_data,
                scenarios_data.c.project_id == projects_data.c.project_id,
            )
        )
        .where(
            projects_territory_data.c.project_id == project_id,
            scenarios_data.c.is_based.is_(True),
        )
    )

    # Act
    result = await get_project_territory_by_id_from_db(mock_conn, project_id, user_id)

    # Assert
    assert isinstance(result, ProjectTerritoryDTO), "Result should be a ProjectTerritoryDTO."
    assert isinstance(ProjectTerritory.from_dto(result), ProjectTerritory), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(statement))
    mock_check.assert_called_once_with(mock_conn, project_id, user_id)


@pytest.mark.asyncio
@patch("idu_api.urban_api.utils.pagination.verify_params")
async def test_get_projects_from_db(mock_verify_params, mock_conn: MockConnection):
    """Test the get_projects_from_db function."""

    # Arrange
    user_id = "mock_string"
    filters = {
        "only_own": False,
        "is_regional": False,
        "territory_id": 1,
        "name": "mock_string",
        "created_at": datetime.now(timezone.utc),
        "order_by": None,
        "ordering": "asc",
    }
    limit, offset = 10, 0
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
            ).join(scenarios_data, scenarios_data.c.project_id == projects_data.c.project_id)
        )
        .where(
            scenarios_data.c.is_based.is_(True),
            projects_data.c.is_regional.is_(filters["is_regional"]),
            or_(projects_data.c.user_id == user_id, projects_data.c.public.is_(True)),
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
    list_result = await get_projects_from_db(mock_conn, user_id, **filters, paginate=False)
    paginate_result = await get_projects_from_db(mock_conn, user_id, **filters, paginate=True)

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
    user_id = "mock_string"
    decimal_places = 15
    statement = (
        select(
            projects_data,
            territories_data.c.name.label("territory_name"),
            cast(ST_AsGeoJSON(projects_territory_data.c.geometry, decimal_places), JSONB).label("geometry"),
            cast(ST_AsGeoJSON(projects_territory_data.c.centre_point, decimal_places), JSONB).label("centre_point"),
            scenarios_data.c.scenario_id,
            scenarios_data.c.name.label("scenario_name"),
        )
        .select_from(
            projects_data.join(territories_data, territories_data.c.territory_id == projects_data.c.territory_id)
            .join(scenarios_data, scenarios_data.c.project_id == projects_data.c.project_id)
            .join(projects_territory_data, projects_territory_data.c.project_id == projects_data.c.project_id)
        )
        .where(
            scenarios_data.c.is_based.is_(True),
            projects_data.c.is_regional.is_(False),
            or_(projects_data.c.user_id == user_id, projects_data.c.public.is_(True)),
        )
    )

    # Act
    result = await get_projects_territories_from_db(mock_conn, user_id, False, None)
    geojson_result = await GeoJSONResponse.from_list([r.to_geojson_dict() for r in result])

    # Assert
    assert isinstance(result, list), "Result should be a list."
    assert all(
        isinstance(item, ProjectWithTerritoryDTO) for item in result
    ), "Each item should be a ProjectWithBaseScenarioDTO."
    assert isinstance(
        Project(**geojson_result.features[0].properties), Project
    ), "Couldn't create pydantic model from geojson properties."
    mock_conn.execute_mock.assert_called_once_with(str(statement))


@pytest.mark.asyncio
async def test_get_preview_projects_images_from_minio(
    mock_conn: MockConnection, mock_minio_client: MockAsyncMinioClient, project_image: io.BytesIO
):
    """Test the get_preview_projects_images_from_minio function."""

    # Arrange
    project_id = 1
    user_id = "mock_string"
    filters = {
        "only_own": False,
        "is_regional": False,
        "territory_id": 1,
        "name": "mock_string",
        "created_at": datetime.now(timezone.utc),
        "order_by": None,
        "ordering": "asc",
    }
    page, page_size = 1, 10
    logger: structlog.stdlib.BoundLogger = structlog.get_logger()
    statement = (
        select(projects_data.c.project_id)
        .where(
            projects_data.c.is_regional.is_(filters["is_regional"]),
            or_(projects_data.c.user_id == user_id, projects_data.c.public.is_(True)),
            projects_data.c.territory_id == filters["territory_id"],
            projects_data.c.name.ilike(f'%{filters["name"]}%'),
            func.date(projects_data.c.created_at) >= filters["created_at"],
        )
        .order_by(projects_data.c.project_id)
        .offset(page_size * (page - 1))
        .limit(page_size)
    )
    await mock_minio_client.upload_file(project_image, f"projects/{project_id}/preview.png", logger)

    # Act
    result = await get_preview_projects_images_from_minio(
        mock_conn,
        mock_minio_client,
        user_id,
        **filters,
        page=page,
        page_size=page_size,
        logger=logger,
    )
    # Assert
    assert isinstance(result, io.BytesIO), "Result should be a io.BytesIO."
    mock_conn.execute_mock.assert_called_once_with(str(statement))
    mock_minio_client.get_files_mock.assert_has_calls([call(["projects/1/preview.png"])])


@pytest.mark.asyncio
async def test_get_preview_projects_images_url_from_minio(
    mock_conn: MockConnection, mock_minio_client: MockAsyncMinioClient, project_image: io.BytesIO
):
    """Test the get_preview_projects_images_url_from_minio function."""

    # Arrange
    project_id = 1
    user_id = "mock_string"
    filters = {
        "only_own": False,
        "is_regional": False,
        "territory_id": 1,
        "name": "mock_string",
        "created_at": datetime.now(timezone.utc),
        "order_by": None,
        "ordering": "asc",
    }
    page, page_size = 1, 10
    logger: structlog.stdlib.BoundLogger = structlog.get_logger()
    statement = (
        select(projects_data.c.project_id)
        .where(
            projects_data.c.is_regional.is_(filters["is_regional"]),
            or_(projects_data.c.user_id == user_id, projects_data.c.public.is_(True)),
            projects_data.c.territory_id == filters["territory_id"],
            projects_data.c.name.ilike(f'%{filters["name"]}%'),
            func.date(projects_data.c.created_at) >= filters["created_at"],
        )
        .order_by(projects_data.c.project_id)
        .offset(page_size * (page - 1))
        .limit(page_size)
    )
    await mock_minio_client.upload_file(project_image, f"projects/{project_id}/preview.png", logger)

    # Act
    result = await get_preview_projects_images_url_from_minio(
        mock_conn,
        mock_minio_client,
        user_id,
        **filters,
        page=page,
        page_size=page_size,
        logger=logger,
    )

    # Assert
    assert isinstance(result, list), "Result should be a list."
    assert all(isinstance(elem, dict) for elem in result), "Each item should be a dictionary."
    mock_conn.execute_mock.assert_called_once_with(str(statement))
    mock_minio_client.generate_presigned_urls_mock.assert_has_calls([call(["projects/1/preview.png"])])


@pytest.mark.asyncio
@patch("idu_api.urban_api.utils.pagination.verify_params")
async def test_get_user_projects_from_db(mock_verify_params, mock_conn: MockConnection):
    """Test the get_user_projects_from_db function."""

    # Arrange
    user_id = "mock_string"
    is_regional = False
    territory_id = 1
    limit, offset = 10, 0
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
            ).join(scenarios_data, scenarios_data.c.project_id == projects_data.c.project_id)
        )
        .where(
            scenarios_data.c.is_based.is_(True),
            projects_data.c.is_regional.is_(is_regional),
            projects_data.c.user_id == user_id,
            projects_data.c.territory_id == territory_id,
        )
        .order_by(projects_data.c.project_id)
        .offset(offset)
        .limit(limit)
    )
    mock_verify_params.return_value = (None, RawParams(limit=limit, offset=offset))

    # Act
    result = await get_user_projects_from_db(mock_conn, user_id, is_regional, territory_id)

    # Assert
    assert isinstance(result, PageDTO), "Result should be a PageDTO."
    assert all(
        isinstance(item, ProjectDTO) for item in result.items
    ), "Each item should be a ProjectWithBaseScenarioDTO."
    assert isinstance(Project.from_dto(result.items[0]), Project), "Couldn't create pydantic model from DTO."
    mock_conn.execute_mock.assert_any_call(str(statement))


@pytest.mark.asyncio
async def test_get_user_preview_projects_images_from_minio(
    mock_conn: MockConnection, mock_minio_client: MockAsyncMinioClient, project_image: io.BytesIO
):
    """Test the get_user_preview_projects_images_from_minio function."""

    # Arrange
    project_id = 1
    user_id = "mock_string"
    is_regional = False
    territory_id = 1
    page, page_size = 1, 10
    logger: structlog.stdlib.BoundLogger = structlog.get_logger()
    statement = (
        select(projects_data.c.project_id)
        .where(
            projects_data.c.user_id == user_id,
            projects_data.c.is_regional.is_(is_regional),
            projects_data.c.territory_id == territory_id,
        )
        .order_by(projects_data.c.project_id)
        .offset(page_size * (page - 1))
        .limit(page_size)
    )
    await mock_minio_client.upload_file(project_image, f"projects/{project_id}/preview.png", logger)

    # Act
    result = await get_user_preview_projects_images_from_minio(
        mock_conn, mock_minio_client, user_id, is_regional, territory_id, page, page_size, logger
    )

    # Assert
    assert isinstance(result, io.BytesIO), "Result should be a io.BytesIO."
    mock_conn.execute_mock.assert_called_once_with(str(statement))
    mock_minio_client.get_files_mock.assert_has_calls([call(["projects/1/preview.png"])])


@pytest.mark.asyncio
async def test_get_user_preview_projects_images_url_from_minio(
    mock_conn: MockConnection, mock_minio_client: MockAsyncMinioClient, project_image: io.BytesIO
):
    """Test the get_user_preview_projects_images_url_from_minio function."""

    # Arrange
    project_id = 1
    user_id = "mock_string"
    is_regional = False
    territory_id = 1
    page, page_size = 1, 10
    logger: structlog.stdlib.BoundLogger = structlog.get_logger()
    statement = (
        select(projects_data.c.project_id)
        .where(
            projects_data.c.user_id == user_id,
            projects_data.c.is_regional.is_(is_regional),
            projects_data.c.territory_id == territory_id,
        )
        .order_by(projects_data.c.project_id)
        .offset(page_size * (page - 1))
        .limit(page_size)
    )
    await mock_minio_client.upload_file(project_image, f"projects/{project_id}/preview.png", logger)

    # Act
    result = await get_user_preview_projects_images_url_from_minio(
        mock_conn, mock_minio_client, user_id, is_regional, territory_id, page, page_size, logger
    )

    # Assert
    assert isinstance(result, list), "Result should be a list."
    assert all(isinstance(elem, dict) for elem in result), "Each item should be a dictionary."
    mock_conn.execute_mock.assert_called_once_with(str(statement))
    mock_minio_client.generate_presigned_urls_mock.assert_has_calls([call(["projects/1/preview.png"])])


@pytest.mark.asyncio
async def test_add_project_to_db(mock_conn: MockConnection, project_post_req: ProjectPost):
    """Test the add_project_to_db function."""

    # Arrange
    user_id = "mock_string"
    statement_for_project = (
        insert(projects_data)
        .values(
            user_id=user_id,
            territory_id=project_post_req.territory_id,
            name=project_post_req.name,
            description=project_post_req.description,
            public=project_post_req.public,
            is_regional=project_post_req.is_regional,
            properties=project_post_req.properties,
        )
        .returning(projects_data.c.project_id)
    )
    statement_for_base_scenario = (
        insert(scenarios_data)
        .values(
            project_id=1,
            functional_zone_type_id=None,
            name="base scenario for user project",
            is_based=True,
            parent_id=1,
        )
        .returning(scenarios_data.c.scenario_id)
    )
    scenario_id = 1
    project_id = 1
    logger: structlog.stdlib.BoundLogger = structlog.get_logger()
    api_url = "/hextech/indicators_saving/save_all"
    params = {
        "scenario_id": scenario_id,
        "project_id": project_id,
        "background": "true",
    }
    normal_api_url = normalize_url(merge_params(api_url, params))

    # Act
    with aioresponses() as mocked:
        mocked.put(normal_api_url, status=200)
        result = await add_project_to_db(mock_conn, project_post_req, user_id, logger)

    # Assert
    assert isinstance(result, ProjectDTO), "Result should be a ProjectDTO."
    mock_conn.execute_mock.assert_any_call(str(statement_for_project))
    assert any(
        "INSERT INTO user_projects.projects_territory_data" in str(args[0])
        for args in mock_conn.execute_mock.call_args_list
    ), "Expected insertion into user_projects.projects_territory_data table not found."
    mock_conn.execute_mock.assert_any_call(str(statement_for_base_scenario))
    assert any(
        "INSERT INTO user_projects.profiles_data" in str(args[0]) for args in mock_conn.execute_mock.call_args_list
    ), "Expected insertion into user_projects.profiles_data table not found."
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


@pytest.mark.asyncio
@patch("idu_api.urban_api.logic.impl.helpers.projects_objects.check_project")
async def test_put_project_to_db(mock_check: AsyncMock, mock_conn: MockConnection, project_put_req: ProjectPut):
    """Test the put_project_to_db function."""

    # Arrange
    project_id = 1
    user_id = "mock_string"
    update_statement = (
        update(projects_data)
        .where(projects_data.c.project_id == project_id)
        .values(**project_put_req.model_dump(), updated_at=datetime.now(timezone.utc))
    )

    # Act
    result = await put_project_to_db(mock_conn, project_put_req, project_id, user_id)

    # Assert
    assert isinstance(result, ProjectDTO), "Result should be a ProjectDTO"
    mock_conn.execute_mock.assert_any_call(str(update_statement))
    mock_conn.commit_mock.assert_called_once()
    mock_check.assert_called_once_with(mock_conn, project_id, user_id, to_edit=True)


@pytest.mark.asyncio
@patch("idu_api.urban_api.logic.impl.helpers.projects_objects.check_project")
async def test_patch_project_to_db(mock_check: AsyncMock, mock_conn: MockConnection, project_patch_req: ProjectPatch):
    """Test the patch_project_to_db function."""

    # Arrange
    project_id = 1
    user_id = "mock_string"
    update_statement = (
        update(projects_data)
        .where(projects_data.c.project_id == project_id)
        .values(**project_patch_req.model_dump(exclude_unset=True), updated_at=datetime.now(timezone.utc))
        .returning(projects_data)
    )

    # Act
    result = await patch_project_to_db(mock_conn, project_patch_req, project_id, user_id)

    # Assert
    assert isinstance(result, ProjectDTO), "Result should be a ProjectDTO."
    mock_conn.execute_mock.assert_any_call(str(update_statement))
    mock_conn.commit_mock.assert_called_once()
    mock_check.assert_called_once_with(mock_conn, project_id, user_id, to_edit=True)


@pytest.mark.asyncio
@patch("idu_api.urban_api.logic.impl.helpers.projects_objects.check_project")
async def test_delete_project_from_db(
    mock_check: AsyncMock, mock_conn: MockConnection, mock_minio_client: MockAsyncMinioClient, project_image: io.BytesIO
):
    """Test the delete_project_from_db function."""

    # Arrange
    project_id = 1
    user_id = "mock_string"
    logger: structlog.stdlib.BoundLogger = structlog.get_logger()
    delete_geometry_statement = delete(projects_object_geometries_data).where(
        projects_object_geometries_data.c.object_geometry_id.in_([1])
    )
    delete_physical_statement = delete(projects_physical_objects_data).where(
        projects_physical_objects_data.c.physical_object_id.in_([1])
    )
    delete_service_statement = delete(projects_services_data).where(projects_services_data.c.service_id.in_([1]))
    delete_statement = delete(projects_data).where(projects_data.c.project_id == project_id)
    await mock_minio_client.upload_file(project_image, f"projects/{project_id}/", logger)

    # Act
    result = await delete_project_from_db(mock_conn, project_id, mock_minio_client, user_id, logger)

    # Assert
    assert result == {"status": "ok"}, "Result should be {'status': 'ok'}."
    mock_conn.execute_mock.assert_any_call(str(delete_geometry_statement))
    mock_conn.execute_mock.assert_any_call(str(delete_physical_statement))
    mock_conn.execute_mock.assert_any_call(str(delete_service_statement))
    mock_conn.execute_mock.assert_any_call(str(delete_statement))
    mock_conn.commit_mock.assert_called_once()
    mock_minio_client.delete_file_mock.assert_called_once_with(f"projects/{project_id}/")
    mock_check.assert_called_once_with(mock_conn, project_id, user_id, to_edit=True)


@pytest.mark.asyncio
@patch("idu_api.urban_api.logic.impl.helpers.projects_objects.check_project")
async def test_upload_project_image_to_minio(
    mock_check: AsyncMock, mock_conn: MockConnection, mock_minio_client: MockAsyncMinioClient, project_image: io.BytesIO
):
    """Test the upload_project_image_to_minio function."""

    # Arrange
    project_id = 1
    user_id = "mock_string"
    logger: structlog.stdlib.BoundLogger = structlog.get_logger()

    # Act
    result = await upload_project_image_to_minio(
        mock_conn, mock_minio_client, project_id, user_id, project_image, logger
    )

    # Assert
    assert isinstance(result, dict), "Result should be a dictionary."
    assert "image_url" in result, "Expected key 'image_url' in result not found."
    assert "preview_url" in result, "Expected key 'preview_url' in result not found."
    mock_minio_client.upload_file_mock.assert_has_calls(
        [
            call(f"projects/{project_id}/image.jpg"),
            call(f"projects/{project_id}/preview.png"),
        ],
        any_order=False,
    )
    mock_minio_client.objects_exist_mock.assert_called_once_with(
        [f"projects/{project_id}/image.jpg", f"projects/{project_id}/preview.png"]
    )
    mock_minio_client.generate_presigned_urls_mock.assert_called_once_with(
        [f"projects/{project_id}/image.jpg", f"projects/{project_id}/preview.png"]
    )
    mock_check.assert_called_once_with(mock_conn, project_id, user_id, to_edit=True)


@pytest.mark.asyncio
@patch("idu_api.urban_api.logic.impl.helpers.projects_objects.check_project")
async def test_get_full_project_image_from_minio(
    mock_check: AsyncMock, mock_conn: MockConnection, mock_minio_client: MockAsyncMinioClient, project_image: io.BytesIO
):
    """Test the get_full_project_image_from_minio function."""

    # Arrange
    user_id = "mock_string"
    project_id = 1
    logger: structlog.stdlib.BoundLogger = structlog.get_logger()
    await mock_minio_client.upload_file(project_image, f"projects/{project_id}/image.jpg", logger)

    # Act
    result = await get_full_project_image_from_minio(mock_conn, mock_minio_client, project_id, user_id, logger)

    # Assert
    assert isinstance(result, io.BytesIO), "Result should be a io.BytesIO."
    mock_minio_client.get_files_mock.assert_called()
    mock_minio_client.get_files_mock.assert_has_calls([call(["projects/1/image.jpg"])])
    mock_check.assert_called_once_with(mock_conn, project_id, user_id)


@pytest.mark.asyncio
@patch("idu_api.urban_api.logic.impl.helpers.projects_objects.check_project")
async def test_get_preview_project_image_from_minio(
    mock_check: AsyncMock, mock_conn: MockConnection, mock_minio_client: MockAsyncMinioClient, project_image: io.BytesIO
):
    """Test the get_preview_project_image_from_minio function."""

    # Arrange
    user_id = "mock_string"
    project_id = 1
    logger: structlog.stdlib.BoundLogger = structlog.get_logger()
    await mock_minio_client.upload_file(project_image, f"projects/{project_id}/preview.png", logger)

    # Act
    result = await get_preview_project_image_from_minio(mock_conn, mock_minio_client, project_id, user_id, logger)

    # Assert
    assert isinstance(result, io.BytesIO), "Result should be a io.BytesIO."
    mock_minio_client.get_files_mock.assert_called()
    mock_minio_client.get_files_mock.assert_has_calls([call(["projects/1/preview.png"])])
    mock_check.assert_called_once_with(mock_conn, project_id, user_id)


@pytest.mark.asyncio
@patch("idu_api.urban_api.logic.impl.helpers.projects_objects.check_project")
async def test_get_full_project_image_url_from_minio(
    mock_check: AsyncMock, mock_conn: MockConnection, mock_minio_client: MockAsyncMinioClient, project_image: io.BytesIO
):
    """Test the get_full_project_image_url_from_minio function."""

    # Arrange
    user_id = "mock_string"
    project_id = 1
    logger: structlog.stdlib.BoundLogger = structlog.get_logger()
    await mock_minio_client.upload_file(project_image, f"projects/{project_id}/image.jpg", logger)

    # Act
    result = await get_full_project_image_url_from_minio(mock_conn, mock_minio_client, project_id, user_id, logger)

    # Assert
    assert isinstance(result, str), "Result should be a string."
    mock_minio_client.generate_presigned_urls_mock.assert_has_calls([call(["projects/1/image.jpg"])])
    mock_check.assert_called_once_with(mock_conn, project_id, user_id)
