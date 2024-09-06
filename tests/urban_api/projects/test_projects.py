import httpx
import pytest


@pytest.mark.asyncio
async def test_get_all_projects(urban_api_host, expired_auth_token):
    headers = {"Authorization": f"Bearer {expired_auth_token}"}

    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get("/projects", headers=headers)

    assert response.status_code == 200
    assert isinstance(response.json(), list)


@pytest.mark.asyncio
async def test_get_user_projects(urban_api_host, expired_auth_token):
    headers = {"Authorization": f"Bearer {expired_auth_token}"}

    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get("/user_projects", headers=headers)

    assert response.status_code == 200
    assert isinstance(response.json(), list)


@pytest.mark.asyncio
async def test_post_project(urban_api_host, expired_auth_token, project_post_req):
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
    headers = {"Authorization": f"Bearer {expired_auth_token}"}

    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.patch(f"/projects/{user_project['project_id']}", json=project_patch_req, headers=headers)

    assert response.status_code == 200
    body: dict = response.json()
    assert body["name"] == project_patch_req["name"]
    assert body["description"] == project_post_req["description"]
    assert body["public"] is project_post_req["public"]
    assert body["image_url"] == project_post_req["image_url"]


@pytest.mark.asyncio
async def test_delete_project(urban_api_host, user_project, expired_auth_token):
    headers = {"Authorization": f"Bearer {expired_auth_token}"}

    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.delete(f"/projects/{user_project['project_id']}", headers=headers)

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}

    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get(f"/projects/{user_project['project_id']}", headers=headers)

    assert response.status_code == 404
