from typing import Any

import httpx
import pytest


@pytest.fixture
def user_project(urban_api_host, expired_auth_token, project_post_req) -> dict[str, Any]:
    headers = {"Authorization": f"Bearer {expired_auth_token}"}

    with httpx.Client(base_url=f"{urban_api_host}/api/v1") as client:
        response = client.post("/projects", json=project_post_req, headers=headers)

    assert response.status_code == 201
    return response.json()


@pytest.fixture
def expired_auth_token() -> str:
    """Fixture to get expired auth token. Useful when expiration check is disabled in API."""

    return (
        "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
        "eyJzdWIiOiJhZG1pbkB0ZXN0LnJ1IiwiaWF0IjoxNzI5OTIyMTA1LCJleHAiOjE3Mjk5MjM5MDUsImNpdGllc19pZCI6WzEsMiw1LDEwL"
        "DEzLDE0LDE3LDE5LDIwLDIxLDIyLDIzLDI0LDE4LDI1LDI2LDI4LDI5XSwic2NvcGVzIjpbInNlcnZpY2VzLmhpZXJhcmNoeV9vYmplY3"
        "RzOmRhdGFfZWRpdCIsImRhdGEuY2l0eV9vYmplY3RzOmRhdGFfZWRpdCIsInBvcHVsYXRpb24ubGl2aW5nX21vZGVsOmRhdGFfZWRpdCJ"
        "dLCJpc19zdXBlcnVzZXIiOnRydWV9."
        "bpIr04RCQFOLu283dYR6kAGe8eKT1YCaAS-UQtD25Gk"
    )


@pytest.fixture
def project_post_req() -> dict[str, Any]:
    """POST request template for user projects data."""

    return {
        "name": "Test Project Name",
        "territory_id": 1,
        "description": "Test Project Description",
        "public": True,
        "is_regional": False,
        "territory": {
            "geometry": {
                "type": "Polygon",
                "coordinates": [[[30.22, 59.86], [30.22, 59.85], [30.25, 59.85], [30.25, 59.86], [30.22, 59.86]]],
            },
        },
    }


@pytest.fixture
def project_put_req() -> dict[str, Any]:
    """POST request template for user projects data."""

    return {
        "name": "Updated Test Project Name",
        "description": "Updated Test Project Description",
        "public": True,
        "properties": {},
    }


@pytest.fixture
def project_patch_req() -> dict[str, Any]:
    """POST request template for user projects data."""

    return {
        "name": "New Patched Project Name",
    }
