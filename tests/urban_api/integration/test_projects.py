import aiohttp
import pytest

from idu_api.urban_api.schemas import Project


####################################################################################
#                         Default use-case tests                                   #
####################################################################################


@pytest.mark.asyncio
async def test_get_project_by_id_200(urban_api_host: str, expired_auth_token: str, common_project: Project):
    """Test GET /projects/{project_id} returns a project with status code 200."""

    # Arrange
    headers = {"Authorization": f"Bearer {expired_auth_token}"}
    expected_project = common_project.model_dump()
    project_id = expected_project["project_id"]

    # Act
    async with aiohttp.ClientSession(base_url=f"{urban_api_host}/api/v1/") as session:
        response = await session.get(f"projects/{project_id}", headers=headers)
        result = await response.json()

    # Assert
    assert response.status == 200, "Expected status code 200 (OK)"
    assert isinstance(result, dict), "Response should be a dictionary"
    assert result.keys() == expected_project.keys(), "Expected set of keys not found."
    for key in expected_project.keys():
        if key not in ("geometry", "centre_point", "created_at", "updated_at"):
            assert result[key] == expected_project[key], f"Expected {key} mismatch."


@pytest.mark.asyncio
async def test_get_projects_200(urban_api_host: str, expired_auth_token: str):
    """Test GET /projects returns a paginated list of projects with status code 200."""

    # Arrange
    headers = {"Authorization": f"Bearer {expired_auth_token}"}
    params = {
        "territory_id": 1,
        "project_type": "common",
        "name": "test",
        "created_at": "2023-01-01",
        "page": 1,
        "page_size": 5
    }

    # Act
    async with aiohttp.ClientSession(base_url=f"{urban_api_host}/api/v1/") as session:
        response = await session.get("projects", headers=headers, params=params)
        result = await response.json()

    # Assert
    assert response.status == 200, "Expected status code 200 (OK)"
    assert isinstance(result, dict), "Response should be a dictionary"
    assert result.keys() == {"count", "prev", "next", "results"}, "Response should contain all expected keys"
    assert len(result["results"]) <= 5, "Number of results should not exceed page_size"


####################################################################################
#                        Bad Request use-case tests                                #
####################################################################################


@pytest.mark.asyncio
async def test_get_projects_400(urban_api_host: str, expired_auth_token: str):
    """Test GET /projects returns status code 400 when invalid parameters are provided."""

    # Arrange
    headers = {"Authorization": f"Bearer {expired_auth_token}"}
    params = {
        "is_regional": "true",
        "project_type": "common",
    }

    # Act
    async with aiohttp.ClientSession(base_url=f"{urban_api_host}/api/v1/") as session:
        response = await session.get("projects", headers=headers, params=params)
        result = await response.json()

    # Assert
    assert response.status == 400, "Expected status code 400 (Bad Request)"
    assert isinstance(result, dict), "Response should be a dictionary"
    assert "detail" in result, "Response should contain 'detail' key with error message"
    assert "Please, choose either regional projects or certain project type" in result["detail"], (
        "Error message should indicate the conflict between is_regional and project_type"
    )


####################################################################################
#                       Unauthorized use-case tests                                #
####################################################################################


@pytest.mark.asyncio
async def test_get_projects_401(urban_api_host: str):
    """Test GET /projects returns status code 401 when requesting own projects without authentication."""

    # Arrange
    params = {"only_own": "true"}

    # Act
    async with aiohttp.ClientSession(base_url=f"{urban_api_host}/api/v1/") as session:
        response = await session.get("projects", params=params)
        result = await response.json()

    # Assert
    assert response.status == 401, "Expected status code 401 (Unauthorized)"
    assert isinstance(result, dict), "Response should be a dictionary"
    assert "detail" in result, "Response should contain 'detail' key with error message"
    assert "Authentication required to view own projects" in result["detail"], (
        "Error message should indicate that authentication is required"
    )
