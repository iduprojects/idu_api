"""All fixtures for scenarios tests are defined here."""

from datetime import datetime, timezone

import pytest

from idu_api.urban_api.schemas import Scenario, ScenarioPatch, ScenarioPost, ScenarioPut
from idu_api.urban_api.schemas.short_models import FunctionalZoneTypeBasic, ShortProject, ShortScenario, ShortTerritory

__all__ = [
    "scenario_req",
    "scenario_patch_req",
    "scenario_post_req",
    "scenario_put_req",
]


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
            )
        ),
        name="Test Scenario",
        is_based=True,
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
        properties={},
    )


@pytest.fixture
def scenario_put_req() -> ScenarioPut:
    """POST request template for user scenarios data."""

    return ScenarioPut(
        name="Updated Test Scenario Name",
        functional_zone_type_id=1,
        is_based=True,
        properties={},
    )


@pytest.fixture
def scenario_patch_req() -> ScenarioPatch:
    """POST request template for user scenarios data."""

    return ScenarioPatch(
        name="New Patched Scenario Name",
        functional_zone_type_id=1,
        is_based=True,
    )
