import subprocess
import time

import httpx
import pytest

from .config import APP_PATH, AUTH_DATA, AUTH_PATH

TEST_PROJECT_ID: int


@pytest.fixture(scope="session", autouse=True)
def start_app():
    process = subprocess.Popen(["poetry", "run", "launch_urban_api"])
    time.sleep(1)
    yield
    process.terminate()
    process.wait()


@pytest.fixture()
async def auth_token():
    async with httpx.AsyncClient(base_url=AUTH_PATH) as client:
        auth_response = await client.post(
            "token",
            data=AUTH_DATA,
            follow_redirects=True,
        )
        tokens = auth_response.json()
        introspect_response = await client.post(
            f"introspect",
            data={"token": tokens["access_token"], "token_type_hint": "access_token"},
            follow_redirects=True,
        )

        if introspect_response.json().get("active"):
            return tokens["access_token"]
        else:
            pytest.fail("Failed to authenticate")


@pytest.fixture()
def project_post():
    return {
        "name": "Test Project Name",
        "project_territory_info": {
            "geometry": {
                "type": "Polygon",
                "coordinates": [[[30.22, 59.86], [30.22, 59.85], [30.25, 59.85], [30.25, 59.86], [30.22, 59.86]]]
            },
            "centre_point": {"type": "Point", "coordinates": [30.22, 59.86]},
            "properties": {"attribute_name": "attribute_value"}
        },
        "description": "Test Project Description",
        "public": True,
        "image_url": "url"
    }


@pytest.fixture()
def project_put():
    return {
        "name": "New Project Name",
        "project_territory_info": {
            "geometry": {
                "type": "Polygon",
                "coordinates": [[[30.22, 59.86], [30.22, 59.85], [30.25, 59.85], [30.25, 59.86], [30.22, 59.86]]]
            },
            "centre_point": {"type": "Point", "coordinates": [30.22, 59.86]},
            "properties": {"new_attribute_name": "new_attribute_value"}
        },
        "description": "New Project Description",
        "public": False,
        "image_url": "new_url"
    }


@pytest.fixture()
def project_patch():
    return {
        "name": "New Patched Project Name"
    }


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
