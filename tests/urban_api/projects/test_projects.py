from copy import deepcopy

import httpx
import pytest

####################################################################################
#                               Authentication tests                               #
####################################################################################


@pytest.mark.asyncio
async def test_not_authorized_get_requests(urban_api_host, user_project):
    """Test GET requests' status code is 403 when not authorized."""

    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response_projects = await client.get("/projects")
        response_user_projects = await client.get("/user_projects")
        response_projects_project_id = await client.get(f"/projects/{user_project['project_id']}")
        response_projects_project_id_territory_info = await client.get(
            f"/projects/{user_project['project_id']}/territory_info"
        )

    assert response_projects.status_code == 403
    assert response_user_projects.status_code == 403
    assert response_projects_project_id.status_code == 403
    assert response_projects_project_id_territory_info.status_code == 403


@pytest.mark.asyncio
async def test_not_authorized_post_request(urban_api_host, project_post_req):
    """Test POST request's status code is 403 when not authorized."""

    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.post("/projects", json=project_post_req)

    assert response.status_code == 403


@pytest.mark.asyncio
async def test_not_authorized_put_request(urban_api_host, user_project, project_put_req):
    """Test PUT request's status code is 403 when not authorized."""

    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.put(f"/projects/{user_project['project_id']}", json=project_put_req)

    assert response.status_code == 403


@pytest.mark.asyncio
async def test_not_authorized_patch_request(urban_api_host, user_project, project_patch_req):
    """Test PATCH request's status code is 403 when not authorized."""

    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.patch(f"/projects/{user_project['project_id']}", json=project_patch_req)

    assert response.status_code == 403


@pytest.mark.asyncio
async def test_not_authorized_delete_request(urban_api_host, user_project):
    """Test delete request's status code is 403 when not authorized."""

    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.delete(f"/projects/{user_project['project_id']}")

    assert response.status_code == 403


@pytest.mark.asyncio
async def test_invalid_auth_token_get_requests(urban_api_host, user_project):
    """Test GET requests' status code is 401 when token is invalid."""

    headers = {"Authorization": f"Bearer invalid_token"}

    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response_projects = await client.get("/projects", headers=headers)
        response_user_projects = await client.get("/user_projects", headers=headers)
        response_projects_project_id = await client.get(f"/projects/{user_project['project_id']}", headers=headers)
        response_projects_project_id_territory_info = await client.get(
            f"/projects/{user_project['project_id']}/territory_info", headers=headers
        )

    assert response_projects.status_code == 401
    assert response_user_projects.status_code == 401
    assert response_projects_project_id.status_code == 401
    assert response_projects_project_id_territory_info.status_code == 401


@pytest.mark.asyncio
async def test_invalid_auth_token_post_request(urban_api_host, project_post_req):
    """Test POST request's status code is 401 when token is invalid."""

    headers = {"Authorization": f"Bearer invalid_token"}

    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.post("/projects", json=project_post_req, headers=headers)

    assert response.status_code == 401


@pytest.mark.asyncio
async def test_invalid_auth_token_put_request(urban_api_host, user_project, project_put_req):
    """Test PUT request's status code is 401 when token is invalid."""

    headers = {"Authorization": f"Bearer invalid_token"}

    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.put(f"/projects/{user_project['project_id']}", json=project_put_req, headers=headers)

    assert response.status_code == 401


@pytest.mark.asyncio
async def test_invalid_auth_token_patch_request(urban_api_host, user_project, project_patch_req):
    """Test PATCH request's status code is 401 when token is invalid."""

    headers = {"Authorization": f"Bearer invalid_token"}

    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.patch(
            f"/projects/{user_project['project_id']}", json=project_patch_req, headers=headers
        )

    assert response.status_code == 401


@pytest.mark.asyncio
async def test_invalid_auth_token_delete_request(urban_api_host, user_project):
    """Test delete request's status code is 401 when token is invalid."""

    headers = {"Authorization": f"Bearer invalid_token"}

    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.delete(f"/projects/{user_project['project_id']}", headers=headers)

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

    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.post("/projects", json=project_post_req, headers=headers)

    assert response.status_code == 201
    body: dict = response.json()
    assert body["name"] == project_post_req["name"]
    assert body["description"] == project_post_req["description"]
    assert body["public"] is project_post_req["public"]
    assert body["image_url"] == project_post_req["image_url"]
    assert body.get("project_id") is not None


@pytest.mark.asyncio
async def test_get_project_by_id(urban_api_host, user_project, expired_auth_token, project_post_req):
    """Test GET to return correct json of project and status code 200."""

    headers = {"Authorization": f"Bearer {expired_auth_token}"}

    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get(f"/projects/{user_project['project_id']}", headers=headers)

    assert response.status_code == 200
    body: dict = response.json()
    assert body["project_id"] == user_project["project_id"]
    assert body["name"] == project_post_req["name"]
    assert body["description"] == project_post_req["description"]
    assert body["public"] is project_post_req["public"]
    assert body["image_url"] == project_post_req["image_url"]


@pytest.mark.asyncio
async def test_get_projects_territory_info(urban_api_host, user_project, expired_auth_token, project_post_req):
    """Test GET to return correct json of territory info and status code 200."""

    headers = {"Authorization": f"Bearer {expired_auth_token}"}

    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get(f"/projects/{user_project['project_id']}/territory_info", headers=headers)

    assert response.status_code == 200
    body: dict = response.json()
    assert body["geometry"]["type"] == project_post_req["project_territory_info"]["geometry"]["type"]
    assert body["geometry"]["coordinates"] == project_post_req["project_territory_info"]["geometry"]["coordinates"]
    assert body["centre_point"]["type"] == project_post_req["project_territory_info"]["centre_point"]["type"]
    assert (
        body["centre_point"]["coordinates"] == project_post_req["project_territory_info"]["centre_point"]["coordinates"]
    )
    assert body["properties"] == project_post_req["project_territory_info"]["properties"]


@pytest.mark.asyncio
async def test_put_project(urban_api_host, user_project, expired_auth_token, project_put_req):
    """Test PUT to return correct json of project and status code 200."""

    headers = {"Authorization": f"Bearer {expired_auth_token}"}

    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.put(f"/projects/{user_project['project_id']}", json=project_put_req, headers=headers)

    assert response.status_code == 200
    body: dict = response.json()
    assert body["name"] == project_put_req["name"]
    assert body["description"] == project_put_req["description"]
    assert body["public"] is project_put_req["public"]
    assert body["image_url"] == project_put_req["image_url"]


@pytest.mark.asyncio
async def test_patch_project(urban_api_host, user_project, expired_auth_token, project_patch_req, project_post_req):
    """Test PATCH to return correct json of project and status code 200."""

    headers = {"Authorization": f"Bearer {expired_auth_token}"}

    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.patch(
            f"/projects/{user_project['project_id']}", json=project_patch_req, headers=headers
        )

    assert response.status_code == 200
    body: dict = response.json()
    assert body["name"] == project_patch_req["name"]
    assert body["description"] == project_post_req["description"]
    assert body["public"] is project_post_req["public"]
    assert body["image_url"] == project_post_req["image_url"]


@pytest.mark.asyncio
async def test_delete_project(urban_api_host, user_project, expired_auth_token):
    """Test existing project to be unavailable after deletion."""

    headers = {"Authorization": f"Bearer {expired_auth_token}"}

    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.delete(f"/projects/{user_project['project_id']}", headers=headers)

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}

    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get(f"/projects/{user_project['project_id']}", headers=headers)

    assert response.status_code == 404


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
    invalid_centre_point_type = deepcopy(project_post_req)
    invalid_centre_point_coordinates = deepcopy(project_post_req)
    invalid_properties = deepcopy(project_post_req)

    invalid_project_territory_info["project_territory_info"] = "invalid"
    invalid_geometry_type["project_territory_info"]["geometry"]["type"] = "invalid"
    invalid_geometry_coordinates["project_territory_info"]["geometry"]["coordinates"] = "invalid"
    invalid_centre_point_type["project_territory_info"]["centre_point"]["type"] = "invalid"
    invalid_centre_point_coordinates["project_territory_info"]["centre_point"]["coordinates"] = "invalid"
    invalid_properties["project_territory_info"]["properties"] = "invalid"

    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        invalid_project_territory_info_response = await client.post(
            "/projects", json=invalid_project_territory_info, headers=headers
        )
        invalid_geometry_type_response = await client.post("/projects", json=invalid_geometry_type, headers=headers)
        invalid_geometry_coordinates_response = await client.post(
            "/projects", json=invalid_geometry_coordinates, headers=headers
        )
        invalid_centre_point_type_response = await client.post(
            "/projects", json=invalid_centre_point_type, headers=headers
        )
        invalid_centre_point_coordinates_response = await client.post(
            "/projects", json=invalid_centre_point_coordinates, headers=headers
        )
        invalid_properties_response = await client.post("/projects", json=invalid_properties, headers=headers)

    assert invalid_project_territory_info_response.status_code == 422
    assert invalid_geometry_type_response.status_code == 422
    assert invalid_geometry_coordinates_response.status_code == 422
    assert invalid_centre_point_type_response.status_code == 422
    assert invalid_centre_point_coordinates_response.status_code == 422
    assert invalid_properties_response.status_code == 422


@pytest.mark.asyncio
async def test_invalid_data_put_request(urban_api_host, user_project, expired_auth_token, project_put_req):
    """Test PUT requests' status code is 422 when invalid data is passed."""

    headers = {"Authorization": f"Bearer {expired_auth_token}"}

    invalid_project_territory_info = deepcopy(project_put_req)
    invalid_geometry_type = deepcopy(project_put_req)
    invalid_geometry_coordinates = deepcopy(project_put_req)
    invalid_centre_point_type = deepcopy(project_put_req)
    invalid_centre_point_coordinates = deepcopy(project_put_req)
    invalid_properties = deepcopy(project_put_req)

    invalid_project_territory_info["project_territory_info"] = "invalid"
    invalid_geometry_type["project_territory_info"]["geometry"]["type"] = "invalid"
    invalid_geometry_coordinates["project_territory_info"]["geometry"]["coordinates"] = "invalid"
    invalid_centre_point_type["project_territory_info"]["centre_point"]["type"] = "invalid"
    invalid_centre_point_coordinates["project_territory_info"]["centre_point"]["coordinates"] = "invalid"
    invalid_properties["project_territory_info"]["properties"] = "invalid"

    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        invalid_project_territory_info_response = await client.put(
            f"/projects/{user_project['project_id']}", json=invalid_project_territory_info, headers=headers
        )
        invalid_geometry_type_response = await client.put(
            f"/projects/{user_project['project_id']}", json=invalid_geometry_type, headers=headers
        )
        invalid_geometry_coordinates_response = await client.put(
            f"/projects/{user_project['project_id']}", json=invalid_geometry_coordinates, headers=headers
        )
        invalid_centre_point_type_response = await client.put(
            f"/projects/{user_project['project_id']}", json=invalid_centre_point_type, headers=headers
        )
        invalid_centre_point_coordinates_response = await client.put(
            f"/projects/{user_project['project_id']}", json=invalid_centre_point_coordinates, headers=headers
        )
        invalid_properties_response = await client.put(
            f"/projects/{user_project['project_id']}", json=invalid_properties, headers=headers
        )

    assert invalid_project_territory_info_response.status_code == 422
    assert invalid_geometry_type_response.status_code == 422
    assert invalid_geometry_coordinates_response.status_code == 422
    assert invalid_centre_point_type_response.status_code == 422
    assert invalid_centre_point_coordinates_response.status_code == 422
    assert invalid_properties_response.status_code == 422


@pytest.mark.asyncio
async def test_invalid_data_patch_request(urban_api_host, user_project, expired_auth_token, project_put_req):
    """Test PATCH requests' status code is 422 when invalid data is passed."""

    headers = {"Authorization": f"Bearer {expired_auth_token}"}

    invalid_project_territory_info = deepcopy(project_put_req)
    invalid_geometry_type = deepcopy(project_put_req)
    invalid_geometry_coordinates = deepcopy(project_put_req)
    invalid_centre_point_type = deepcopy(project_put_req)
    invalid_centre_point_coordinates = deepcopy(project_put_req)
    invalid_properties = deepcopy(project_put_req)

    invalid_project_territory_info["project_territory_info"] = "invalid"
    invalid_geometry_type["project_territory_info"]["geometry"]["type"] = "invalid"
    invalid_geometry_coordinates["project_territory_info"]["geometry"]["coordinates"] = "invalid"
    invalid_centre_point_type["project_territory_info"]["centre_point"]["type"] = "invalid"
    invalid_centre_point_coordinates["project_territory_info"]["centre_point"]["coordinates"] = "invalid"
    invalid_properties["project_territory_info"]["properties"] = "invalid"

    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        invalid_project_territory_info_response = await client.patch(
            f"/projects/{user_project['project_id']}", json=invalid_project_territory_info, headers=headers
        )
        invalid_geometry_type_response = await client.patch(
            f"/projects/{user_project['project_id']}", json=invalid_geometry_type, headers=headers
        )
        invalid_geometry_coordinates_response = await client.patch(
            f"/projects/{user_project['project_id']}", json=invalid_geometry_coordinates, headers=headers
        )
        invalid_centre_point_type_response = await client.patch(
            f"/projects/{user_project['project_id']}", json=invalid_centre_point_type, headers=headers
        )
        invalid_centre_point_coordinates_response = await client.patch(
            f"/projects/{user_project['project_id']}", json=invalid_centre_point_coordinates, headers=headers
        )
        invalid_properties_response = await client.patch(
            f"/projects/{user_project['project_id']}", json=invalid_properties, headers=headers
        )

    assert invalid_project_territory_info_response.status_code == 422
    assert invalid_geometry_type_response.status_code == 422
    assert invalid_geometry_coordinates_response.status_code == 422
    assert invalid_centre_point_type_response.status_code == 422
    assert invalid_centre_point_coordinates_response.status_code == 422
    assert invalid_properties_response.status_code == 422


@pytest.mark.asyncio
async def test_empty_patch_request(urban_api_host, user_project, expired_auth_token):
    """Test PATCH request's status code is 422 when empty data is passed."""

    headers = {"Authorization": f"Bearer {expired_auth_token}"}

    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.patch(f"/projects/{user_project['project_id']}", json={}, headers=headers)

    assert response.status_code == 422
