"""Integration tests for projects are defined here."""

from io import BytesIO
from typing import Any

import httpx
import pytest
from pydantic import ValidationError

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
from tests.urban_api.helpers.utils import assert_response

####################################################################################
#                           Default use-case tests                                 #
####################################################################################


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
async def test_get_project_by_id(
    urban_api_host: str,
    project: dict[str, Any],
    valid_token: str,
    superuser_token: str,
    expected_status: int,
    error_message: str | None,
    project_id_param: int | None,
):
    """Test GET /projects/{project_id} method."""

    # Arrange
    project_id = project_id_param or project["project_id"]
    headers = {"Authorization": f"Bearer {valid_token if expected_status == 403 else superuser_token}"}

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get(f"/projects/{project_id}", headers=headers)
        result = response.json()

    # Assert
    assert_response(response, expected_status, Project, error_message)
    if response.status_code == 200:
        for k, v in project.items():
            if k in result:
                assert result[k] == v, f"Mismatch for {k}: {result[k]} != {v}."


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
async def test_get_project_territory_by_id(
    urban_api_host: str,
    project: dict[str, Any],
    valid_token: str,
    superuser_token: str,
    expected_status: int,
    error_message: str | None,
    project_id_param: int | None,
):
    """Test GET /projects/{project_id}/territory method."""

    # Arrange
    project_id = project_id_param or project["project_id"]
    headers = {"Authorization": f"Bearer {valid_token if expected_status == 403 else superuser_token}"}

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
    "expected_status, error_message, project_id_param",
    [
        (200, None, None),
        (403, "denied", None),
        (404, "not found", 1e9),
    ],
    ids=["success", "forbidden", "not_found"],
)
async def test_get_scenarios_by_project_id(
    urban_api_host: str,
    project: dict[str, Any],
    valid_token: str,
    superuser_token: str,
    expected_status: int,
    error_message: str | None,
    project_id_param: int | None,
):
    """Test GET /projects/{project_id}/scenarios method."""

    # Arrange
    project_id = project_id_param or project["project_id"]
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
    "expected_status, error_message",
    [
        (200, None),
        (400, "Please, choose either regional projects or certain project type"),
        (401, "Authentication required to view own projects"),
    ],
    ids=["success", "bad_request", "unauthorized"],
)
async def test_get_projects(
    urban_api_host: str,
    project: dict[str, Any],
    superuser_token: str,
    expected_status: int,
    error_message: str | None,
):
    """Test GET /projects method."""

    # Arrange
    headers = {"Authorization": f"Bearer {superuser_token}"} if expected_status != 401 else {}
    params = {
        "only_own": True,
        "is_regional": expected_status == 400,
        "project_type": "common",
        "territory_id": project["territory"]["id"],
        "name": project["name"],
        "created_at": project["created_at"][:10],
        "page_size": 1,
    }

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get("/projects", headers=headers, params=params)
        result = response.json()

    print(headers, params)

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
    "expected_status, error_message",
    [
        (200, None),
        (400, "Please, choose either regional projects or certain project type"),
        (401, "Authentication required to view own projects"),
    ],
    ids=["success", "bad_request", "unauthorized"],
)
async def test_get_preview_project_images(
    urban_api_host: str,
    project: dict[str, Any],
    superuser_token: str,
    expected_status: int,
    error_message: str | None,
):
    """Test GET /projects_preview method."""

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
        response = await client.get("/projects_preview", headers=headers, params=params)

    # Assert
    assert response.status_code == expected_status, f"Invalid status code: {response.status_code}."
    if expected_status == 200:
        assert response.headers["Content-Type"] == "application/zip"
        assert "attachment; filename=project_previews.zip" in response.headers["Content-Disposition"]
    else:
        result = response.json()
        assert isinstance(result, dict), "Result should be a dict."
        assert "detail" in result, "Response should contain a 'detail' field with the error message"
        assert error_message in result["detail"]


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
async def test_get_project_previews_url(
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

    # Assert
    if expected_status == 200:
        assert_response(response, expected_status, MinioImageURL, error_message, result_type="list")
        assert len(response.json()) == 1, "Response should contain one url."
    else:
        assert_response(response, expected_status, MinioImageURL, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, territory_id_param",
    [
        (201, None, None),
        (403, "not authenticated", None),
        (404, "not found", 1e9),
    ],
    ids=["success", "forbidden", "not_found"],
)
async def test_add_project(
    urban_api_host: str,
    project_post_req: ProjectPost,
    region: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    superuser_token: str,
    territory_id_param: int | None,
):
    """Test POST /projects method."""

    # Arrange
    new_project = project_post_req.model_dump()
    new_project["territory_id"] = territory_id_param or region["territory_id"]
    headers = {"Authorization": f"Bearer {superuser_token}"} if expected_status != 403 else {}

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.post("/projects", json=new_project, headers=headers)

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
    "expected_status, error_message, file_type, project_id_param",
    [
        (200, None, "image/jpeg", None),
        (400, "Uploaded file is not an image", "text/plain", None),
        (403, "denied", "image/jpeg", None),
        (404, "not found", "image/jpeg", 1e9),
    ],
    ids=["success", "bad_request", "forbidden", "not_found"],
)
async def test_upload_project_image(
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
    assert_response(response, expected_status, MinioImagesURL, error_message)


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
        assert response.headers["Content-Type"] == "image/png"
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
