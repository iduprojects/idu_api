"""All fixtures for scenarios tests are defined here."""

from datetime import datetime, timezone
from typing import Any

import httpx
import pytest
import pytest_asyncio
import structlog
from sqlalchemy import insert

from idu_api.common.db.connection import PostgresConnectionManager
from idu_api.common.db.entities import scenarios_data
from idu_api.urban_api.schemas import PhysicalObjectWithGeometryPost, Scenario, ScenarioPatch, ScenarioPost, ScenarioPut
from idu_api.urban_api.schemas.geometries import Geometry
from idu_api.urban_api.schemas.short_models import FunctionalZoneTypeBasic, ShortProject, ShortScenario, ShortTerritory

__all__ = [
    "base_regional_scenario",
    "regional_scenario",
    "scenario",
    "scenario_req",
    "scenario_patch_req",
    "scenario_post_req",
    "scenario_put_req",
]

####################################################################################
#                        Integration tests helpers                                 #
####################################################################################


@pytest_asyncio.fixture(scope="session")
async def base_regional_scenario(database, regional_project, urban_api_host, superuser_token) -> dict[str, Any]:
    """Returns created base regional scenario."""
    logger = structlog.getLogger("test")
    connection_manager: PostgresConnectionManager = PostgresConnectionManager(
        master=database.master,
        replicas=[],
        logger=logger,
        application_name="duty_fix_geometry",
    )

    statement = (
        insert(scenarios_data)
        .values(
            project_id=regional_project["project_id"],
            name="Исходный региональный сценарий",
            parent_id=None,
            is_based=True,
            phase=None,
            phase_percentage=None,
        )
        .returning(scenarios_data.c.scenario_id)
    )

    async with connection_manager.get_connection() as conn:
        scenario_id = (await conn.execute(statement)).scalar_one_or_none()
        await conn.commit()

    headers = {"Authorization": f"Bearer {superuser_token}"}
    with httpx.Client(base_url=f"{urban_api_host}/api/v1") as client:
        response = client.get(f"/scenarios/{scenario_id}", headers=headers)

    assert response.status_code == 200, f"Invalid status code was returned: {response.status_code}.\n{response.json()}"
    return response.json()


@pytest.fixture(scope="session")
def regional_scenario(urban_api_host, regional_project, base_regional_scenario, superuser_token) -> dict[str, Any]:
    """Returns created regional scenario."""
    scenario_post_req = ScenarioPost(
        project_id=regional_project["project_id"],
        name="Test Scenario Name",
        functional_zone_type_id=None,
        phase=None,
        phase_percentage=None,
    )
    physical_object_post_req = PhysicalObjectWithGeometryPost(
        territory_id=1,
        geometry=Geometry(
            type="Polygon",
            coordinates=[
                [
                    [30.22, 59.86],
                    [30.22, 59.85],
                    [30.25, 59.85],
                    [30.25, 59.86],
                    [30.22, 59.86],
                ]
            ],
        ),
        address="Test Address",
        osm_id="12345",
        physical_object_type_id=1,
        name="Test Object",
        properties={"key": "value"},
    )
    headers = {"Authorization": f"Bearer {superuser_token}"}

    with httpx.Client(base_url=f"{urban_api_host}/api/v1") as client:
        response = client.post(
            f"/scenarios/{base_regional_scenario['scenario_id']}",
            json=scenario_post_req.model_dump(),
            headers=headers,
        )
        scenario_id = response.json()["scenario_id"]
        client.post(
            f"/scenarios/{scenario_id}/physical_objects",
            json=physical_object_post_req.model_dump(),
            headers=headers,
        )

    assert response.status_code == 201, f"Invalid status code was returned: {response.status_code}.\n{response.json()}"
    return response.json()


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
