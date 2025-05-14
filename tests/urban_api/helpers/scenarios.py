"""All fixtures for scenarios tests are defined here."""

from datetime import datetime, timezone
from typing import Any

import httpx
import pytest

from idu_api.urban_api.schemas import Scenario, ScenarioPatch, ScenarioPost, ScenarioPut
from idu_api.urban_api.schemas.short_models import FunctionalZoneTypeBasic, ShortProject, ShortScenario, ShortTerritory

__all__ = [
    "scenario",
    "scenario_req",
    "scenario_patch_req",
    "scenario_post_req",
    "scenario_put_req",
]

####################################################################################
#                        Integration tests helpers                                 #
####################################################################################


@pytest.fixture(scope="session")
def scenario(urban_api_host, project, functional_zone_type, superuser_token) -> dict[str, Any]:
    """Returns created scenario."""
    scenario_post_req = ScenarioPost(
        project_id=project["project_id"],
        name="Test Scenario Name",
        functional_zone_type_id=functional_zone_type["functional_zone_type_id"],
        phase="pre_design",
        phase_percentage=100,
    )
    headers = {"Authorization": f"Bearer {superuser_token}"}

    with httpx.Client(base_url=f"{urban_api_host}/api/v1") as client:
        response = client.post(
            f"/scenarios/{project['base_scenario']['id']}",
            json=scenario_post_req.model_dump(),
            headers=headers,
        )

    assert response.status_code == 201, f"Invalid status code was returned: {response.status_code}.\n{response.json()}"
    return response.json()


####################################################################################
#                                 Models                                           #
####################################################################################


@pytest.fixture
def scenario_req() -> Scenario:
    """GET request template for user scenarios data."""

    return Scenario(
        scenario_id=1,
        parent_scenario=(ShortScenario(id=1, name="Test Regional Scenario")),
        project=ShortProject(
            project_id=1,
            name="Test Project",
            user_id="Test User",
            region=ShortTerritory(id=1, name="Test Territory"),
        ),
        functional_zone_type=(
            FunctionalZoneTypeBasic(
                id=1,
                name="Test Functional Zone Type",
                nickname="Test Functional Zone Type Nickname",
                description="Test Functional Zone Type Description",
            )
        ),
        name="Test Scenario",
        is_based=True,
        phase="pre_design",
        phase_percentage=100,
        properties={},
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
    )


@pytest.fixture
def scenario_post_req() -> ScenarioPost:
    """POST request template for user scenarios data."""

    return ScenarioPost(
        project_id=1,
        name="Test Scenario Name",
        functional_zone_type_id=1,
        phase="pre_design",
        phase_percentage=100,
        properties={},
    )


@pytest.fixture
def scenario_put_req() -> ScenarioPut:
    """POST request template for user scenarios data."""

    return ScenarioPut(
        name="Updated Test Scenario Name",
        functional_zone_type_id=1,
        is_based=True,
        phase="pre_design",
        phase_percentage=100,
        properties={},
    )


@pytest.fixture
def scenario_patch_req() -> ScenarioPatch:
    """POST request template for user scenarios data."""

    return ScenarioPatch(
        name="New Patched Scenario Name",
        functional_zone_type_id=1,
        is_based=True,
        phase="pre_design",
        phase_percentage=100,
    )
