"""Integration tests for projects are defined here."""

import asyncio
from io import BytesIO
from typing import Any

import httpx
import pytest
from otteroad import KafkaConsumerService
from otteroad.models import BaseScenarioCreated, ProjectCreated
from pydantic import ValidationError

from idu_api.urban_api.config import UrbanAPIConfig
from idu_api.urban_api.schemas import (
    MinioImagesURL,
    MinioImageURL,
    OkResponse,
    Page,
    Project,
    ProjectPost,
    ProjectPut,
    ProjectTerritory,
    Scenario,
)
from idu_api.urban_api.schemas.geometries import GeoJSONResponse
from tests.urban_api.helpers import valid_token
from tests.urban_api.helpers.broker import mock_handler
from tests.urban_api.helpers.utils import assert_response

####################################################################################
#                           Default use-case tests                                 #
####################################################################################


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, project_id_param, is_regional_param",
    [
        (200, None, None, False),
        (200, None, None, True),
        (403, "denied", None, False),
        (404, "not found", 1e9, False),
    ],
    ids=["success_common", "success_regional", "forbidden", "not_found"],
)
async def test_get_project_by_id(
    urban_api_host: str,
    project: dict[str, Any],
    regional_project: dict[str, Any],
    valid_token: str,
    superuser_token: str,
    expected_status: int,
    error_message: str | None,
    project_id_param: int | None,
    is_regional_param: bool,
):
    """Test GET /projects/{project_id} method."""

    # Arrange
    project_id = project_id_param or (regional_project["project_id"] if is_regional_param else project["project_id"])
    headers = {"Authorization": f"Bearer {valid_token if expected_status == 403 else superuser_token}"}

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get(f"/projects/{project_id}", headers=headers)

    # Assert
    assert_response(response, expected_status, Project, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, project_id_param, is_regional_param",
    [
        (200, None, None, False),
        (400, "this method cannot be accessed in a regional project", None, True),
        (403, "denied", None, False),
        (404, "not found", 1e9, False),
    ],
    ids=["success", "regional_project", "forbidden", "not_found"],
)
async def test_get_project_territory_by_id(
    urban_api_host: str,
    project: dict[str, Any],
    regional_project: dict[str, Any],
    valid_token: str,
    superuser_token: str,
    expected_status: int,
    error_message: str | None,
    project_id_param: int | None,
    is_regional_param: bool,
):
    """Test GET /projects/{project_id}/territory method."""

    # Arrange
    project_id = project_id_param or (regional_project["project_id"] if is_regional_param else project["project_id"])
    headers = {
        "Authorization": f"Bearer {valid_token if expected_status == 403 and not is_regional_param else superuser_token}"
    }

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get(f"/projects/{project_id}/territory", headers=headers)
        result = response.json()

    # Assert
    assert_response(response, expected_status, ProjectTerritory, error_message)
    if response.status_code == 200:
        assert result["project"]["project_id"] == project_id, "Response did not match expected project identifier."


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, project_id_param, is_regional_param",
    [
        (200, None, None, False),
        (200, None, None, True),
        (403, "denied", None, False),
        (404, "not found", 1e9, False),
    ],
    ids=["success_common", "success_regional", "forbidden", "not_found"],
)
async def test_get_scenarios_by_project_id(
    urban_api_host: str,
    project: dict[str, Any],
    regional_project: dict[str, Any],
    valid_token: str,
    superuser_token: str,
    expected_status: int,
    error_message: str | None,
    project_id_param: int | None,
    is_regional_param: bool,
):
    """Test GET /projects/{project_id}/scenarios method."""

    # Arrange
    project_id = project_id_param or (regional_project["project_id"] if is_regional_param else project["project_id"])
    headers = {"Authorization": f"Bearer {valid_token if expected_status == 403 else superuser_token}"}

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get(f"/projects/{project_id}/scenarios", headers=headers)
        result = response.json()

    # Assert
    if response.status_code == 200:
        assert_response(response, expected_status, Scenario, error_message, result_type="list")
        assert all(
            item["project"]["project_id"] == project_id for item in result
        ), "Response did not match expected project identifier."
    else:
        assert_response(response, expected_status, Scenario, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, is_regional_param",
    [
        (200, None, False),
        (200, None, True),
        (400, "Please, choose either regional projects or certain project type", True),
        (401, "Authentication required to view own projects", False),
    ],
    ids=["success_common", "success_regional", "bad_request", "unauthorized"],
)
async def test_get_projects(
    urban_api_host: str,
    project: dict[str, Any],
    superuser_token: str,
    expected_status: int,
    error_message: str | None,
    is_regional_param: bool,
):
    """Test GET /projects method."""

    # Arrange
    headers = {"Authorization": f"Bearer {superuser_token}"} if expected_status != 401 else {}
    params = {
        "only_own": True,
        "is_regional": is_regional_param,
        "page_size": 1,
    }
    if not is_regional_param or expected_status == 400:
        params.update(
            {
                "project_type": "common",
                "territory_id": project["territory"]["id"],
                "name": project["name"],
                "created_at": project["created_at"][:10],
            }
        )

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get("/projects", headers=headers, params=params)
        result = response.json()

    # Assert
    assert_response(response, expected_status, Page, error_message)
    if response.status_code == 200:
        assert len(result["results"]) > 0, "Response should contain at least one item."
        assert (
            len(result["results"]) <= params["page_size"]
        ), f"Response should contain no more than {params['page_size']} items."
        try:
            Project(**result["results"][0])
        except ValidationError as e:
            pytest.fail(f"Pydantic validation error: {str(e)}")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message",
    [
        (200, None),
        (401, "Authentication required to view own projects"),
    ],
    ids=["success", "unauthorized"],
)
async def test_get_projects_territories(
    urban_api_host: str,
    project: dict[str, Any],
    superuser_token: str,
    expected_status: int,
    error_message: str | None,
):
    """Test GET /projects_territories method."""

    # Arrange
    headers = {"Authorization": f"Bearer {superuser_token}"} if expected_status != 401 else {}
    params = {
        "only_own": True,
        "project_type": "common",
        "territory_id": project["territory"]["id"],
        "page_size": 1,
    }

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get("/projects_territories", headers=headers, params=params)
        result = response.json()

    # Assert
    assert_response(response, expected_status, GeoJSONResponse, error_message)
    if response.status_code == 200:
        assert len(result["features"]) > 0, "Response should contain at least one feature."
        try:
            Project(**result["features"][0]["properties"])
        except ValidationError as e:
            pytest.fail(f"Pydantic validation error: {str(e)}")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, territory_id_param, is_regional_param",
    [
        (201, None, None, False),
        (201, None, None, True),
        (403, "not authenticated", None, False),
        (404, "not found", 1e9, False),
    ],
    ids=["success_common", "success_regional", "forbidden", "not_found"],
)
async def test_add_project(
    urban_api_host: str,
    project_post_req: ProjectPost,
    region: dict[str, Any],
    kafka_consumer: KafkaConsumerService,
    expected_status: int,
    error_message: str | None,
    superuser_token: str,
    territory_id_param: int | None,
    is_regional_param: bool,
):
    """Test POST /projects method."""

    # Arrange
    new_handler = mock_handler(ProjectCreated)
    kafka_consumer.register_handler(new_handler)
    new_project = project_post_req.model_dump()
    new_project["is_regional"] = is_regional_param
    new_project["territory"] = new_project["territory"] if not is_regional_param else None
    new_project["territory_id"] = territory_id_param or region["territory_id"]
    headers = {"Authorization": f"Bearer {superuser_token}"} if expected_status != 403 else {}

    # Act
    if expected_status == 201:
        await asyncio.sleep(5)
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.post("/projects", json=new_project, headers=headers)
    if expected_status == 201:
        await asyncio.sleep(5)

    # Assert
    assert_response(response, expected_status, Project, error_message)
    if expected_status == 201 and not is_regional_param:
        assert len(new_handler.received_events) == 1, "No one event was received"
        assert isinstance(new_handler.received_events[0], ProjectCreated), "Received event is not ProjectCreated"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, project_id_param, scenario_id_param, is_regional_project, is_regional_scenario",
    [
        (201, None, None, None, False, True),
        (400, "this method cannot be accessed in a regional project", None, None, True, True),
        (400, "this method cannot be accessed in a project scenario", None, None, False, False),
        (403, "you must be a superuser to create a new base scenario", None, None, True, True),
        (404, "not found", 1e9, None, False, True),
        (409, "already exists", None, None, False, True),
    ],
    ids=["success", "regional_project", "regional_scenario", "forbidden", "not_found", "conflict"],
)
async def test_create_base_scenario(
    urban_api_host: str,
    project: dict[str, Any],
    regional_project: dict[str, Any],
    scenario: dict[str, Any],
    regional_scenario: dict[str, Any],
    kafka_consumer: KafkaConsumerService,
    expected_status: int,
    error_message: str | None,
    valid_token: str,
    superuser_token: str,
    project_id_param: int | None,
    scenario_id_param: int | None,
    is_regional_project: bool,
    is_regional_scenario: bool,
):
    """Test POST /projects/{project_id}/base_scenario/{scenario_id} method."""

    # Arrange
    new_handler = mock_handler(BaseScenarioCreated)
    kafka_consumer.register_handler(new_handler)
    project_id = project_id_param or (regional_project["project_id"] if is_regional_project else project["project_id"])
    scenario_id = scenario_id_param or (
        regional_scenario["scenario_id"] if is_regional_scenario else scenario["scenario_id"]
    )
    headers = {"Authorization": f"Bearer {superuser_token if expected_status != 403 else valid_token}"}

    # Act
    if expected_status == 201:
        await asyncio.sleep(5)
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.post(f"/projects/{project_id}/base_scenario/{scenario_id}", headers=headers)
    if expected_status == 201:
        await asyncio.sleep(5)

    # Assert
    assert_response(response, expected_status, Scenario, error_message)
    if expected_status == 201:
        assert len(new_handler.received_events) == 1, "No one event was received"
        assert isinstance(
            new_handler.received_events[0], BaseScenarioCreated
        ), "Received event is not BaseScenarioCreated"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, project_id_param",
    [
        (200, None, None),
        (403, "denied", None),
        (404, "not found", 1e9),
    ],
    ids=["success", "forbidden", "not_found"],
)
async def test_put_project(
    urban_api_host: str,
    project_post_req: ProjectPost,
    project_put_req: ProjectPut,
    region: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    valid_token: str,
    superuser_token: str,
    project_id_param: int | None,
):
    """Test PUT /projects/{project_id} method."""

    # Arrange
    project_id = project_id_param
    if project_id_param is None:
        new_project = project_post_req.model_dump()
        new_project["territory_id"] = region["territory_id"]
        async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
            response = await client.post(
                "/projects", json=new_project, headers={"Authorization": f"Bearer {superuser_token}"}
            )
            project_id = response.json()["project_id"]
    new_project = project_put_req.model_dump()
    headers = {"Authorization": f"Bearer {valid_token if expected_status == 403 else superuser_token}"}

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.put(f"/projects/{project_id}", json=new_project, headers=headers)

    # Assert
    assert_response(response, expected_status, Project, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, project_id_param",
    [
        (200, None, None),
        (403, "denied", None),
        (404, "not found", 1e9),
    ],
    ids=["success", "forbidden", "not_found"],
)
async def test_patch_project(
    urban_api_host: str,
    project_post_req: ProjectPost,
    project_put_req: ProjectPut,
    region: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    valid_token: str,
    superuser_token: str,
    project_id_param: int | None,
):
    """Test PATCH /projects/{project_id} method."""

    # Arrange
    project_id = project_id_param
    if project_id_param is None:
        new_project = project_post_req.model_dump()
        new_project["territory_id"] = region["territory_id"]
        async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
            response = await client.post(
                "/projects", json=new_project, headers={"Authorization": f"Bearer {superuser_token}"}
            )
            project_id = response.json()["project_id"]
    new_project = project_put_req.model_dump()
    headers = {"Authorization": f"Bearer {valid_token if expected_status == 403 else superuser_token}"}

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.patch(f"/projects/{project_id}", json=new_project, headers=headers)

    # Assert
    assert_response(response, expected_status, Project, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, project_id_param",
    [
        (200, None, None),
        (403, "denied", None),
        (404, "not found", 1e9),
    ],
    ids=["success", "not_authenticated", "not_found"],
)
async def test_delete_project(
    urban_api_host: str,
    project_post_req: ProjectPost,
    region: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    valid_token: str,
    superuser_token: str,
    project_id_param: int | None,
):
    """Test DELETE /projects/{project_id} method."""

    # Arrange
    new_project = project_post_req.model_dump()
    new_project["territory_id"] = region["territory_id"]
    headers = {"Authorization": f"Bearer {valid_token if expected_status == 403 else superuser_token}"}

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        if project_id_param is None:
            response = await client.post(
                "/projects", json=new_project, headers={"Authorization": f"Bearer {superuser_token}"}
            )
            project_id = response.json()["project_id"]
            response = await client.delete(f"/projects/{project_id}", headers=headers)
        else:
            response = await client.delete(f"/projects/{project_id_param}", headers=headers)

    # Assert
    assert_response(response, expected_status, OkResponse, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message",
    [
        (200, None),
        (400, "Please, choose either regional projects or certain project type"),
        (401, "Authentication required to view own projects"),
    ],
    ids=["success", "bad_request", "unauthorized"],
)
async def test_get_projects_main_image_url(
    urban_api_host: str,
    project: dict[str, Any],
    superuser_token: str,
    expected_status: int,
    error_message: str | None,
):
    """Test GET /projects_preview_url method."""

    # Arrange
    headers = {"Authorization": f"Bearer {superuser_token}"} if expected_status != 401 else {}
    params = {
        "only_own": True,
        "is_regional": expected_status == 400,
        "project_type": "common",
        "territory_id": project["territory"]["id"],
        "name": project["name"],
        "created_at": project["created_at"][:10],
        "page": 1,
        "page_size": 1,
    }

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get("/projects_preview_url", headers=headers, params=params)
        result = response.json()

    # Assert
    if expected_status == 200:
        assert_response(response, expected_status, Page, error_message)
        assert len(result["results"]) > 0, "Response should contain at least one item."
        assert (
            len(result["results"]) <= params["page_size"]
        ), f"Response should contain no more than {params['page_size']} items."
        try:
            MinioImageURL(**result["results"][0])
        except ValidationError as e:
            pytest.fail(f"Pydantic validation error: {str(e)}")
    else:
        assert_response(response, expected_status, MinioImageURL, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, file_type, project_id_param",
    [
        (200, None, "image/jpeg", None),
        (400, "Uploaded file is not an image", "text/plain", None),
        (403, "denied", "image/jpeg", None),
        (404, "not found", "image/jpeg", 1e9),
    ],
    ids=["success", "bad_request", "forbidden", "not_found"],
)
async def test_upload_project_main_image(
    urban_api_host: str,
    project: dict[str, Any],
    project_image: bytes,
    valid_token: str,
    superuser_token: str,
    expected_status: int,
    error_message: str | None,
    file_type: str,
    project_id_param: int | None,
):
    """Test PUT /projects/{project_id}/image method."""

    # Arrange
    project_id = project_id_param or project["project_id"]
    headers = {"Authorization": f"Bearer {valid_token if expected_status == 403 else superuser_token}"}
    files = {"file": ("image.jpg", BytesIO(project_image), file_type)}

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.put(
            f"/projects/{project_id}/image",
            headers=headers,
            files=files,
        )

    # Assert
    if expected_status == 200:
        assert isinstance(response.json(), str), "Response should be a string."
    else:
        assert_response(response, expected_status, ..., error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, file_type, project_id_param",
    [
        (201, None, "image/jpeg", None),
        (400, "Uploaded file is not an image", "text/plain", None),
        (403, "denied", "image/jpeg", None),
        (404, "not found", "image/jpeg", 1e9),
    ],
    ids=["success", "bad_request", "forbidden", "not_found"],
)
async def test_upload_gallery_image(
    urban_api_host: str,
    project: dict[str, Any],
    project_image: bytes,
    valid_token: str,
    superuser_token: str,
    expected_status: int,
    error_message: str | None,
    file_type: str,
    project_id_param: int | None,
):
    """Test POST /projects/{project_id}/gallery method."""

    # Arrange
    project_id = project_id_param or project["project_id"]
    headers = {"Authorization": f"Bearer {valid_token if expected_status == 403 else superuser_token}"}
    files = {"file": ("image.jpg", BytesIO(project_image), file_type)}

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.post(
            f"/projects/{project_id}/gallery",
            headers=headers,
            files=files,
        )

    # Assert
    if expected_status == 201:
        assert isinstance(response.json(), str), "Response should be a string."
    else:
        assert_response(response, expected_status, ..., error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, project_id_param",
    [
        (200, None, None),
        (403, "denied", None),
        (404, "not found", 1e9),
        (404, "not found", None),
    ],
    ids=["success", "forbidden", "not_found_project", "not_found_image"],
)
async def test_set_project_main_image(
    urban_api_host: str,
    project: dict[str, Any],
    project_image: bytes,
    valid_token: str,
    superuser_token: str,
    config: UrbanAPIConfig,
    expected_status: int,
    error_message: str | None,
    project_id_param: int | None,
):
    """Test PUT /projects/{project_id}/gallery/{image_id} method."""

    # Arrange
    project_id = project_id_param or project["project_id"]
    headers = {"Authorization": f"Bearer {valid_token if expected_status == 403 else superuser_token}"}

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        if expected_status == 200:
            files = {"file": ("image.jpg", BytesIO(project_image), "image/jpeg")}
            response = await client.post(
                f"/projects/{project_id}/gallery",
                headers=headers,
                files=files,
            )
            image_id = response.json().split("?")[0].split("/")[-1].split(".")[0]
        else:
            image_id = "fake_id"
        response = await client.put(f"/projects/{project_id}/gallery/{image_id}", headers=headers)

    # Assert
    assert_response(response, expected_status, OkResponse, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, project_id_param",
    [
        (200, None, None),
        (403, "denied", None),
        (404, "not found", 1e9),
    ],
    ids=["success", "forbidden", "not_found"],
)
async def test_get_project_gallery_images_urls(
    urban_api_host: str,
    project: dict[str, Any],
    valid_token: str,
    superuser_token: str,
    config: UrbanAPIConfig,
    expected_status: int,
    error_message: str | None,
    project_id_param: int | None,
):
    """Test GET /projects/{project_id}/gallery method."""

    # Arrange
    project_id = project_id_param or project["project_id"]
    headers = {"Authorization": f"Bearer {valid_token if expected_status == 403 else superuser_token}"}

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get(f"/projects/{project_id}/gallery", headers=headers)

    # Assert
    if expected_status == 200:
        assert isinstance(response.json(), list), "Response should be a list."
        assert all(isinstance(item, str) for item in response.json())
    else:
        assert_response(response, expected_status, str, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, project_id_param",
    [
        (200, None, None),
        (403, "denied", None),
        (404, "not found", 1e9),
        (404, "not found", None),
    ],
    ids=["success", "forbidden", "not_found_project", "not_found_image"],
)
async def test_delete_project_gallery_image(
    urban_api_host: str,
    project: dict[str, Any],
    project_image: bytes,
    valid_token: str,
    superuser_token: str,
    config: UrbanAPIConfig,
    expected_status: int,
    error_message: str | None,
    project_id_param: int | None,
):
    """Test DELETE /projects/{project_id}/gallery/{image_id} method."""

    # Arrange
    project_id = project_id_param or project["project_id"]
    headers = {"Authorization": f"Bearer {valid_token if expected_status == 403 else superuser_token}"}

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        if expected_status == 200:
            files = {"file": ("image.jpg", BytesIO(project_image), "image/jpeg")}
            response = await client.post(
                f"/projects/{project_id}/gallery",
                headers=headers,
                files=files,
            )
            image_id = response.json().split("?")[0].split("/")[-1].split(".")[0]
        else:
            image_id = "fake_id"
        response = await client.delete(f"/projects/{project_id}/gallery/{image_id}", headers=headers)

    # Assert
    assert_response(response, expected_status, OkResponse, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, project_id_param",
    [
        (200, None, None),
        (403, "denied", None),
        (404, "not found", 1e9),
    ],
    ids=["success", "forbidden", "not_found"],
)
async def test_get_full_project_image(
    urban_api_host: str,
    project: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    valid_token: str,
    superuser_token: str,
    project_id_param: int | None,
):
    """Test GET /projects/{project_id}/image method."""

    # Arrange
    project_id = project_id_param or project["project_id"]
    headers = {"Authorization": f"Bearer {valid_token if expected_status == 403 else superuser_token}"}

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get(f"/projects/{project_id}/image", headers=headers)

    # Assert
    assert response.status_code == expected_status, f"Invalid status code: {response.status_code}."
    if expected_status == 200:
        assert response.headers["Content-Type"] == "image/jpeg"
    else:
        result = response.json()
        assert isinstance(result, dict), "Result should be a dict."
        assert "detail" in result, "Response should contain a 'detail' field with the error message"
        assert error_message in result["detail"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, project_id_param",
    [
        (200, None, None),
        (403, "denied", None),
        (404, "not found", 1e9),
    ],
    ids=["success", "forbidden", "not_found"],
)
async def test_get_preview_project_image(
    urban_api_host: str,
    project: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    valid_token: str,
    superuser_token: str,
    project_id_param: int | None,
):
    """Test GET /projects/{project_id}/preview method."""

    # Arrange
    project_id = project_id_param or project["project_id"]
    headers = {"Authorization": f"Bearer {valid_token if expected_status == 403 else superuser_token}"}

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get(f"/projects/{project_id}/preview", headers=headers)

    # Assert
    assert response.status_code == expected_status, f"Invalid status code: {response.status_code}."
    if expected_status == 200:
        assert response.headers["Content-Type"] == "image/jpeg"
    else:
        result = response.json()
        assert isinstance(result, dict), "Result should be a dict."
        assert "detail" in result, "Response should contain a 'detail' field with the error message"
        assert error_message in result["detail"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, project_id_param",
    [
        (200, None, None),
        (403, "denied", None),
        (404, "not found", 1e9),
    ],
    ids=["success", "forbidden", "not_found"],
)
async def test_get_full_project_image_url(
    urban_api_host: str,
    project: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    valid_token: str,
    superuser_token: str,
    project_id_param: int | None,
):
    """Test GET /projects/{project_id}/image_url method."""

    # Arrange
    project_id = project_id_param or project["project_id"]
    headers = {"Authorization": f"Bearer {valid_token if expected_status == 403 else superuser_token}"}

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get(f"/projects/{project_id}/image_url", headers=headers)

    # Assert
    assert response.status_code == expected_status, f"Invalid status code: {response.status_code}."
    if expected_status == 200:
        assert isinstance(response.json(), str), "Result should be a string."
    else:
        result = response.json()
        assert isinstance(result, dict), "Result should be a dict."
        assert "detail" in result, "Response should contain a 'detail' field with the error message"
        assert error_message in result["detail"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, project_id_param",
    [
        (200, None, None),
        (403, "denied", None),
        (404, "not found", 1e9),
    ],
    ids=["success", "forbidden", "not_found"],
)
async def test_get_preview_project_image_url(
    urban_api_host: str,
    project: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    valid_token: str,
    superuser_token: str,
    project_id_param: int | None,
):
    """Test GET /projects/{project_id}/preview_url method."""

    # Arrange
    project_id = project_id_param or project["project_id"]
    headers = {"Authorization": f"Bearer {valid_token if expected_status == 403 else superuser_token}"}

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get(f"/projects/{project_id}/preview_url", headers=headers)

    # Assert
    assert response.status_code == expected_status, f"Invalid status code: {response.status_code}."
    if expected_status == 200:
        assert isinstance(response.json(), str), "Result should be a string."
    else:
        result = response.json()
        assert isinstance(result, dict), "Result should be a dict."
        assert "detail" in result, "Response should contain a 'detail' field with the error message"
        assert error_message in result["detail"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, file_type, project_id_param",
    [
        (200, None, "image/jpeg", None),
        (400, "Uploaded file is not an image", "text/plain", None),
        (403, "denied", "image/jpeg", None),
        (404, "not found", "image/jpeg", 1e9),
    ],
    ids=["success", "bad_request", "forbidden", "not_found"],
)
async def test_upload_project_logo(
    urban_api_host: str,
    project: dict[str, Any],
    project_image: bytes,
    valid_token: str,
    superuser_token: str,
    expected_status: int,
    error_message: str | None,
    file_type: str,
    project_id_param: int | None,
):
    """Test PUT /projects/{project_id}/logo method."""

    # Arrange
    project_id = project_id_param or project["project_id"]
    headers = {"Authorization": f"Bearer {valid_token if expected_status == 403 else superuser_token}"}
    files = {"file": ("image.jpg", BytesIO(project_image), file_type)}

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.put(
            f"/projects/{project_id}/logo",
            headers=headers,
            files=files,
        )

    # Assert
    if expected_status == 200:
        assert isinstance(response.json(), str), "Response should be a string."
    else:
        assert_response(response, expected_status, ..., error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, project_id_param",
    [
        (200, None, None),
        (403, "denied", None),
        (404, "not found", 1e9),
    ],
    ids=["success", "forbidden", "not_found"],
)
async def test_get_project_logo_url(
    urban_api_host: str,
    project: dict[str, Any],
    valid_token: str,
    superuser_token: str,
    expected_status: int,
    error_message: str | None,
    project_id_param: int | None,
):
    """Test GET /projects/{project_id}/logo method."""

    # Arrange
    project_id = project_id_param or project["project_id"]
    headers = {"Authorization": f"Bearer {valid_token if expected_status == 403 else superuser_token}"}

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get(f"/projects/{project_id}/logo", headers=headers)

    # Assert
    if expected_status == 200:
        assert isinstance(response.json(), str), "Response should be a string."
    else:
        assert_response(response, expected_status, ..., error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, project_id_param",
    [
        (200, None, None),
        (403, "denied", None),
        (404, "not found", 1e9),
    ],
    ids=["success", "forbidden", "not_found"],
)
async def test_delete_project_logo(
    urban_api_host: str,
    project: dict[str, Any],
    valid_token: str,
    superuser_token: str,
    expected_status: int,
    error_message: str | None,
    project_id_param: int | None,
):
    """Test DELETE /projects/{project_id}/logo method."""

    # Arrange
    project_id = project_id_param or project["project_id"]
    headers = {"Authorization": f"Bearer {valid_token if expected_status == 403 else superuser_token}"}

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.delete(f"/projects/{project_id}/logo", headers=headers)

    # Assert
    assert_response(response, expected_status, OkResponse, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, project_id_param",
    [
        (200, None, None),
        (403, "denied", None),
        (404, "not found", 1e9),
    ],
    ids=["success", "forbidden", "not_found"],
)
async def test_get_project_phase_documents_urls(
    urban_api_host: str,
    project: dict[str, Any],
    valid_token: str,
    superuser_token: str,
    expected_status: int,
    error_message: str | None,
    project_id_param: int | None,
):
    """Test GET /projects/{project_id}/phases/documents method."""

    # Arrange
    project_id = project_id_param or project["project_id"]
    headers = {"Authorization": f"Bearer {valid_token if expected_status == 403 else superuser_token}"}
    params = {"phase": "construction"}

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get(f"/projects/{project_id}/phases/documents", headers=headers, params=params)

    # Assert
    if expected_status == 200:
        assert_response(response, expected_status, str, error_message, result_type="list")
    else:
        assert_response(response, expected_status, str, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, project_id_param",
    [
        (201, None, None),
        (403, "denied", None),
        (404, "not found", 1e9),
    ],
    ids=["success", "forbidden", "not_found"],
)
async def test_upload_phase_document(
    urban_api_host: str,
    project: dict[str, Any],
    project_image: bytes,
    valid_token: str,
    superuser_token: str,
    expected_status: int,
    error_message: str | None,
    project_id_param: int | None,
):
    """Test POST /projects/{project_id}/phases/documents method."""

    # Arrange
    project_id = project_id_param or project["project_id"]
    headers = {"Authorization": f"Bearer {valid_token if expected_status == 403 else superuser_token}"}
    files = {"file": ("image.jpg", BytesIO(project_image), "image/jpeg")}
    params = {"phase": "construction"}

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.post(
            f"/projects/{project_id}/phases/documents",
            headers=headers,
            files=files,
            params=params,
        )

    # Assert
    if expected_status == 201:
        assert isinstance(response.json(), str), "Response should be a string."
    else:
        assert_response(response, expected_status, ..., error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, project_id_param",
    [
        (200, None, None),
        (403, "denied", None),
        (404, "not found", 1e9),
        (404, "not found", None),
    ],
    ids=["success", "forbidden", "not_found_project", "not_found_file"],
)
async def test_rename_phase_document(
    urban_api_host: str,
    project: dict[str, Any],
    valid_token: str,
    superuser_token: str,
    expected_status: int,
    error_message: str | None,
    project_id_param: int | None,
):
    """Test PATCH /projects/{project_id}/phases/documents method."""

    # Arrange
    project_id = project_id_param or project["project_id"]
    headers = {"Authorization": f"Bearer {valid_token if expected_status == 403 else superuser_token}"}
    params = {"phase": "construction", "old_key": "image.jpg", "new_key": "renamed_image.jpg"}

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.patch(
            f"/projects/{project_id}/phases/documents",
            headers=headers,
            params=params,
        )

    # Assert
    if expected_status == 200:
        assert isinstance(response.json(), str), "Response should be a string."
        assert params["new_key"] in response.json(), "File was not renamed."
    else:
        assert_response(response, expected_status, ..., error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, project_id_param",
    [
        (200, None, None),
        (403, "denied", None),
        (404, "not found", 1e9),
    ],
    ids=["success", "forbidden", "not_found_project"],
)
async def test_delete_project_phase_document(
    urban_api_host: str,
    project: dict[str, Any],
    valid_token: str,
    superuser_token: str,
    expected_status: int,
    error_message: str | None,
    project_id_param: int | None,
):
    """Test DELETE /projects/{project_id}/logo method."""

    # Arrange
    project_id = project_id_param or project["project_id"]
    headers = {"Authorization": f"Bearer {valid_token if expected_status == 403 else superuser_token}"}
    if expected_status != 404:
        filename = "renamed_image.jpg"
    else:
        filename = "fake.file"
    params = {"phase": "construction", "filename": filename}

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.delete(f"/projects/{project_id}/phases/documents", headers=headers, params=params)

    # Assert
    assert_response(response, expected_status, OkResponse, error_message)
