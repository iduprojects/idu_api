from copy import deepcopy

import httpx
import pytest

####################################################################################
#                               Authentication tests                               #
####################################################################################


@pytest.mark.asyncio
async def test_not_authorized_post_request(urban_api_host, project_post_req):
    """Test POST request's status code is 403 when not authorized."""

    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.post("/projects", json=project_post_req)

    assert response.status_code == 403


@pytest.mark.asyncio
async def test_invalid_auth_token_post_request(urban_api_host, project_post_req):
    """Test POST request's status code is 401 when token is invalid."""

    headers = {"Authorization": f"Bearer invalid_token"}

    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.post("/projects", json=project_post_req, headers=headers)

    assert response.status_code == 401


####################################################################################
#                              Default use-case tests                              #
####################################################################################


@pytest.mark.asyncio
async def test_get_all_projects(urban_api_host, expired_auth_token):
    """Test GET to return list and status code 200."""

    headers = {"Authorization": f"Bearer {expired_auth_token}"}

    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get("/projects", headers=headers)

    assert response.status_code == 200
    assert isinstance(response.json(), list)


@pytest.mark.asyncio
async def test_get_user_projects(urban_api_host, expired_auth_token):
    """Test GET to return list and status code 200."""

    headers = {"Authorization": f"Bearer {expired_auth_token}"}

    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get("/user_projects", headers=headers)

    assert response.status_code == 200
    assert isinstance(response.json(), list)


@pytest.mark.asyncio
async def test_post_project(urban_api_host, expired_auth_token, project_post_req):
    """Test POST to return correct json of project and status code 201."""

    headers = {"Authorization": f"Bearer {expired_auth_token}"}

    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1", timeout=10000) as client:
        response = await client.post("/projects", json=project_post_req, headers=headers)

    assert response.status_code == 201
    body: dict = response.json()
    assert body["name"] == project_post_req["name"]
    assert body["description"] == project_post_req["description"]
    assert body["public"] is project_post_req["public"]
    assert body.get("project_id") is not None


####################################################################################
#                               Invalid data tests                                 #
####################################################################################


@pytest.mark.asyncio
async def test_invalid_data_post_request(urban_api_host, expired_auth_token, project_post_req):
    """Test POST requests' status code is 422 when invalid data is passed."""

    headers = {"Authorization": f"Bearer {expired_auth_token}"}

    invalid_project_territory_info = deepcopy(project_post_req)
    invalid_geometry_type = deepcopy(project_post_req)
    invalid_geometry_coordinates = deepcopy(project_post_req)

    invalid_project_territory_info["territory"] = "invalid"
    invalid_geometry_type["territory"]["geometry"]["type"] = "invalid"
    invalid_geometry_coordinates["territory"]["geometry"]["coordinates"] = "invalid"

    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        invalid_project_territory_info_response = await client.post(
            "/projects", json=invalid_project_territory_info, headers=headers
        )
        invalid_geometry_type_response = await client.post("/projects", json=invalid_geometry_type, headers=headers)
        invalid_geometry_coordinates_response = await client.post(
            "/projects", json=invalid_geometry_coordinates, headers=headers
        )


    assert invalid_project_territory_info_response.status_code == 422
    assert invalid_geometry_type_response.status_code == 422
    assert invalid_geometry_coordinates_response.status_code == 422
