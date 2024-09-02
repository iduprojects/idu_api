import httpx
import pytest

from tests.urban_api.projects import APP_PATH
from tests.urban_api.projects.helpers.projects import auth_token, project_patch, project_post, project_put

TEST_PROJECT_ID: int


@pytest.mark.asyncio
async def test_get_all_projects(auth_token):
    headers = {"Authorization": f"Bearer {await auth_token}"}

    async with httpx.AsyncClient(base_url=f"{APP_PATH}/api/v1") as client:
        response = await client.get("/projects", headers=headers)

    assert response.status_code == 200
    assert isinstance(response.json(), list)


@pytest.mark.asyncio
async def test_get_user_projects(auth_token):
    headers = {"Authorization": f"Bearer {await auth_token}"}

    async with httpx.AsyncClient(base_url=f"{APP_PATH}/api/v1") as client:
        response = await client.get("/user_projects", headers=headers)

    assert response.status_code == 200
    assert isinstance(response.json(), list)


@pytest.mark.asyncio
async def test_post_project(auth_token, project_post):
    headers = {"Authorization": f"Bearer {await auth_token}"}

    async with httpx.AsyncClient(base_url=f"{APP_PATH}/api/v1") as client:
        response = await client.post("/projects", json=project_post, headers=headers)

    assert response.status_code == 201
    assert response.json()["name"] == project_post["name"]
    assert response.json()["description"] == project_post["description"]
    assert response.json()["public"] is project_post["public"]
    assert response.json()["image_url"] == project_post["image_url"]

    global TEST_PROJECT_ID
    TEST_PROJECT_ID = response.json()["project_id"]


@pytest.mark.asyncio
async def test_get_project_by_id(auth_token, project_post):
    headers = {"Authorization": f"Bearer {await auth_token}"}

    async with httpx.AsyncClient(base_url=f"{APP_PATH}/api/v1") as client:
        response = await client.get(f"/projects/{TEST_PROJECT_ID}", headers=headers)

    assert response.status_code == 200
    assert response.json()["project_id"] == TEST_PROJECT_ID
    assert response.json()["name"] == project_post["name"]
    assert response.json()["description"] == project_post["description"]
    assert response.json()["public"] is project_post["public"]
    assert response.json()["image_url"] == project_post["image_url"]


@pytest.mark.asyncio
async def test_get_projects_territory_info(auth_token, project_post):
    headers = {"Authorization": f"Bearer {await auth_token}"}

    async with httpx.AsyncClient(base_url=f"{APP_PATH}/api/v1") as client:
        response = await client.get(f"/projects/{TEST_PROJECT_ID}/territory_info", headers=headers)

    assert response.status_code == 200
    assert response.json()["geometry"]["type"] == project_post["project_territory_info"]["geometry"]["type"]
    assert (
        response.json()["geometry"]["coordinates"] == project_post["project_territory_info"]["geometry"]["coordinates"]
    )
    assert response.json()["centre_point"]["type"] == project_post["project_territory_info"]["centre_point"]["type"]
    assert (
        response.json()["centre_point"]["coordinates"]
        == project_post["project_territory_info"]["centre_point"]["coordinates"]
    )
    assert response.json()["properties"] == project_post["project_territory_info"]["properties"]


@pytest.mark.asyncio
async def test_put_project(auth_token, project_put):
    headers = {"Authorization": f"Bearer {await auth_token}"}

    async with httpx.AsyncClient(base_url=f"{APP_PATH}/api/v1") as client:
        response = await client.put(f"/projects/{TEST_PROJECT_ID}", json=project_put, headers=headers)

    assert response.status_code == 200
    assert response.json()["name"] == project_put["name"]
    assert response.json()["description"] == project_put["description"]
    assert response.json()["public"] is project_put["public"]
    assert response.json()["image_url"] == project_put["image_url"]


@pytest.mark.asyncio
async def test_patch_project(auth_token, project_patch, project_put):
    headers = {"Authorization": f"Bearer {await auth_token}"}

    async with httpx.AsyncClient(base_url=f"{APP_PATH}/api/v1") as client:
        response = await client.patch(f"/projects/{TEST_PROJECT_ID}", json=project_patch, headers=headers)

    assert response.status_code == 200
    assert response.json()["name"] == project_patch["name"]
    assert response.json()["description"] == project_put["description"]
    assert response.json()["public"] is project_put["public"]
    assert response.json()["image_url"] == project_put["image_url"]


@pytest.mark.asyncio
async def test_delete_project(auth_token):
    headers = {"Authorization": f"Bearer {await auth_token}"}

    async with httpx.AsyncClient(base_url=f"{APP_PATH}/api/v1") as client:
        response = await client.delete(f"/projects/{TEST_PROJECT_ID}", headers=headers)

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}

    async with httpx.AsyncClient(base_url=f"{APP_PATH}/api/v1") as client:
        response = await client.get(f"/projects/{TEST_PROJECT_ID}", headers=headers)

    assert response.status_code == 404
