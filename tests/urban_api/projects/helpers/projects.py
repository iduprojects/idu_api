import subprocess
import time

import httpx
import pytest

from tests.urban_api.projects import AUTH_DATA, AUTH_PATH


@pytest.fixture(scope="session", autouse=True)
def start_app():
    """Fixture to start the application on default root via poetry command."""

    process = subprocess.Popen(["poetry", "run", "launch_urban_api"])
    time.sleep(5)
    yield
    process.terminate()
    process.wait()


@pytest.fixture()
async def auth_token():
    """Fixture to get an auth token. VPN is needed."""

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
