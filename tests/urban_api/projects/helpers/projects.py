from typing import Any

import httpx
import pytest


@pytest.fixture
def user_project(urban_api_host, expired_auth_token, project_post_req) -> dict[str, Any]: # pylint: disable=redefined-outer-name
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
        "eyJzdWIiOiJhZG1pbkB0ZXN0LnJ1IiwiaWF0IjoxNzI1MzU0MDAwLCJleHAiOjE3MjUzNTUwMDAsImNpdGllc19pZCI6WzEsMiw1LDEwLDEzLD"
        "E0LDE3LDE5LDIwLDIxLDIyLDIzLDI0LDE4LDI1LDI2LDI4LDI5XSwic2NvcGVzIjpbInNlcnZpY2VzLmhpZXJhcmNoeV9vYmplY3RzOmRhdGFf"
        "ZWRpdCIsImRhdGEuY2l0eV9vYmplY3RzOmRhdGFfZWRpdCIsInBvcHVsYXRpb24ubGl2aW5nX21vZGVsOmRhdGFfZWRpdCJdLCJpc19zdXBlcn"
        "VzZXIiOnRydWV9."
        "djlrYQ8mG3r_FH9k6NTJL2swAovePPj9ZzvMlDxDH-g"
    )


@pytest.fixture
def project_post_req() -> dict[str, Any]:
    """POST request template for user projects data"""
    return {
        "name": "Test Project Name",
        "project_territory_info": {
            "geometry": {
                "type": "Polygon",
                "coordinates": [[[30.22, 59.86], [30.22, 59.85], [30.25, 59.85], [30.25, 59.86], [30.22, 59.86]]],
            },
            "centre_point": {"type": "Point", "coordinates": [30.22, 59.86]},
            "properties": {"attribute_name": "attribute_value"},
        },
        "description": "Test Project Description",
        "public": True,
        "image_url": "url",
    }


@pytest.fixture
def project_put_req() -> dict[str, Any]:
    """PUT request template for user projects data"""
    return {
        "name": "New Project Name",
        "project_territory_info": {
            "geometry": {
                "type": "Polygon",
                "coordinates": [[[30.22, 59.86], [30.22, 59.85], [30.25, 59.85], [30.25, 59.86], [30.22, 59.86]]],
            },
            "centre_point": {"type": "Point", "coordinates": [30.22, 59.86]},
            "properties": {"new_attribute_name": "new_attribute_value"},
        },
        "description": "New Project Description",
        "public": False,
        "image_url": "new_url",
    }


@pytest.fixture
def project_patch_req() -> dict[str, str]:
    """PATCH request template for user projects data"""
    return {
        "name": "New Patched Project Name",
    }
