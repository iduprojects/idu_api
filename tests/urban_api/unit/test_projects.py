import io
from datetime import datetime

import pytest
import structlog
from PIL import Image

from idu_api.urban_api.dto import ProjectDTO
from idu_api.urban_api.logic.impl.helpers import projects_objects
from idu_api.urban_api.schemas import Project, ProjectPatch, ProjectPost, ProjectPut


@pytest.mark.asyncio
async def test_add_project_to_db(mock_conn, project_post_req):
    """
    Test add_project_to_db function
    """

    # Arrange
    user_id = "mock_string"
    mocked_project = ProjectPost(**project_post_req)
    logger: structlog.stdlib.BoundLogger = structlog.get_logger()

    # Act
    result = await projects_objects.add_project_to_db(mock_conn, mocked_project, user_id, logger)

    # Asserting
    assert isinstance(result, ProjectDTO)
    assert isinstance(result.project_id, int), "project_id должен быть числом"
    assert isinstance(result.user_id, str), "user_id должен быть строкой"
    assert isinstance(result.properties, dict), "properties должен быть словарем"
    assert isinstance(result.created_at, datetime), "created_at должен быть объектом datetime"
    assert isinstance(result.updated_at, datetime), "updated_at должен быть объектом datetime"
    assert isinstance(Project.from_dto(result), Project), "не удалось собрать pydantic модель из DTO"


@pytest.mark.asyncio
async def test_put_project_to_db(mock_conn, project_put_req):
    """
    Test put_project_to_db function
    """

    # Arrange
    project_id = 1
    user_id = "mock_string"
    mocked_project = ProjectPut(**project_put_req)

    # Act
    result = await projects_objects.put_project_to_db(mock_conn, mocked_project, project_id, user_id)

    # Asserting
    assert isinstance(result, ProjectDTO)
    assert isinstance(result.project_id, int), "project_id должен быть числом"
    assert isinstance(result.user_id, str), "user_id должен быть строкой"
    assert isinstance(result.properties, dict), "properties должен быть словарем"
    assert isinstance(result.created_at, datetime), "created_at должен быть объектом datetime"
    assert isinstance(result.updated_at, datetime), "updated_at должен быть объектом datetime"
    assert isinstance(Project.from_dto(result), Project), "не удалось собрать pydantic модель из DTO"


@pytest.mark.asyncio
async def test_patch_project_to_db(mock_conn, project_patch_req):
    """
    Test patch_project_to_db function
    """

    # Arrange
    project_id = 1
    user_id = "mock_string"
    mocked_project = ProjectPatch(**project_patch_req)

    # Act
    result = await projects_objects.patch_project_to_db(mock_conn, mocked_project, project_id, user_id)

    # Asserting
    assert isinstance(result, ProjectDTO)
    assert isinstance(result.project_id, int), "project_id должен быть числом"
    assert isinstance(result.user_id, str), "user_id должен быть строкой"
    assert isinstance(result.properties, dict), "properties должен быть словарем"
    assert isinstance(result.created_at, datetime), "created_at должен быть объектом datetime"
    assert isinstance(result.updated_at, datetime), "updated_at должен быть объектом datetime"
    assert isinstance(Project.from_dto(result), Project), "не удалось собрать pydantic модель из DTO"


@pytest.mark.asyncio
async def test_delete_project_to_db(mock_conn, mock_minio_client):
    """
    Test delete_project_to_db function
    """

    # Arrange
    project_id = 1
    user_id = "mock_string"
    file_data = io.BytesIO()
    await mock_minio_client.upload_file(file_data, "projects/1/")

    # Act
    result = await projects_objects.delete_project_from_db(mock_conn, project_id, mock_minio_client, user_id)

    # Asserting
    assert isinstance(result, dict)
    assert result["status"] == "ok"


@pytest.mark.asyncio
async def test_upload_project_image_to_minio_without_error(mock_conn, mock_minio_client):
    """
    Test upload_project_image_to_minio function
    """

    # Arrange
    project_id = 1
    user_id = "mock_string"
    img = Image.new("RGB", (60, 30), color="red")
    img_byte_arr = io.BytesIO()
    img.save(img_byte_arr, format="PNG")
    img_byte_arr = img_byte_arr.getvalue()

    # Act
    result = await projects_objects.upload_project_image_to_minio(
        mock_conn, mock_minio_client, project_id, user_id, img_byte_arr
    )

    # Asserting
    assert isinstance(result, dict)
    assert "image_url" in result
    assert "preview_url" in result
