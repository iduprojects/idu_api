"""All fixtures for projects tests are defined here."""

import io

import httpx
import pytest
from PIL import Image
from sqlalchemy import insert
from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.common.db.entities import projects_data, scenarios_data
from idu_api.urban_api.schemas import Project, ProjectPatch, ProjectPost, ProjectPut, ProjectTerritoryPost, Territory
from idu_api.urban_api.schemas.geometries import Geometry

__all__ = [
    "regional_project",
    "common_project",
    "project_image",
    "project_patch_req",
    "project_post_req",
    "project_put_req",
]

@pytest.fixture
async def regional_project(urban_api_host, region: Territory, connection: AsyncConnection, expired_auth_token: str) -> Project:
    headers = {"Authorization": f"Bearer {expired_auth_token}"}
    project = {
        "name": "Test Regional Project Name",
        "territory_id": region.territory_id,
        "description": "Test Regional Project Description",
        "public": True,
        "is_city": False,
        "is_regional": True,
    }


    statement = insert(projects_data).values(**project).returning(projects_data.c.project_id)
    project_id = (await connection.execute(statement)).scalar().one()

    base_scenario = {
        "name": "Исходный региональный проект",
        "parent_id": None,
        "is_based": True,
        "functional_zone_type_id": None,
        "project_id": project_id,
    }

    statement = insert(scenarios_data).values(**base_scenario)
    await connection.execute(statement)


    with httpx.Client(base_url=f"{urban_api_host}/api/v1") as client:
        response = client.get(f"/projects/{project_id}", headers=headers)

    return Project(**response.json())



@pytest.fixture
def common_project(urban_api_host, expired_auth_token, region: Territory, regional_project: Project) -> Project:
    # Arrange
    print(regional_project)
    headers = {"Authorization": f"Bearer {expired_auth_token}"}
    project = {
        "name": "Test Project Name",
        "territory_id": region.territory_id,
        "description": "Test Project Description",
        "public": True,
        "is_city": False,
        "territory": {
            "geometry": {
                "type": "Polygon",
                "coordinates": [[[30.22, 59.86], [30.22, 59.85], [30.25, 59.85], [30.25, 59.86], [30.22, 59.86]]],
            }
        }
    }

    # Act
    with httpx.Client(base_url=f"{urban_api_host}/api/v1") as client:
        response = client.post("/projects", json=project, headers=headers)

    # Assert
    assert response.status_code == 201, f"Error on creating project:\n{str(response.json())}"

    return Project(**response.json())


@pytest.fixture
def non_public_project(urban_api_host, expired_auth_token, region: Territory) -> Project:
    # Arrange
    headers = {"Authorization": f"Bearer {expired_auth_token}"}
    project = {
        "user_id": "admin@test.ru",
        "name": "Test Project Name",
        "territory_id": region.territory_id,
        "description": "Test Project Description",
        "public": False,
        "is_city": False,
        "territory": {
            "geometry": {
                "type": "Polygon",
                "coordinates": [[[30.22, 59.86], [30.22, 59.85], [30.25, 59.85], [30.25, 59.86], [30.22, 59.86]]],
            }
        }
    }

    with httpx.Client(base_url=f"{urban_api_host}/api/v1") as client:
        response = client.post("/projects", json=project, headers=headers)

    assert response.status_code == 201, "Error on creating project"

    return Project(**response.json())


@pytest.fixture
def project_post_req() -> ProjectPost:
    """POST request template for user projects data."""

    return ProjectPost(
        name="Test Project Name",
        territory_id=1,
        description="Test Project Description",
        public=True,
        is_city=False,
        territory=ProjectTerritoryPost(
            geometry=Geometry(
                type="Polygon",
                coordinates=[[[30.22, 59.86], [30.22, 59.85], [30.25, 59.85], [30.25, 59.86], [30.22, 59.86]]],
            ),
        ),
    )


@pytest.fixture
def project_put_req() -> ProjectPut:
    """PUT request template for user projects data."""

    return ProjectPut(
        name="Updated Test Project Name",
        description="Updated Test Project Description",
        public=True,
        properties={},
    )


@pytest.fixture
def project_patch_req() -> ProjectPatch:
    """PATCH request template for user projects data."""

    return ProjectPatch(name="New Patched Project Name")


@pytest.fixture
def project_image() -> io.BytesIO:
    """Get simple project image bytes array."""

    img = Image.new("RGB", (60, 30), color="red")
    img_byte_arr = io.BytesIO()
    img.save(img_byte_arr, format="PNG")

    return img_byte_arr.getvalue()
