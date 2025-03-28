"""Integration tests for scenarios-related scenarios are defined here."""

from typing import Any

import httpx
import pytest

from idu_api.urban_api.schemas import OkResponse, Scenario, ScenarioPost, ScenarioPut
from tests.urban_api.helpers.utils import assert_response

####################################################################################
#                           Default use-case tests                                 #
####################################################################################


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, scenario_id_param",
    [
        (200, None, None),
        (403, "denied", None),
        (404, "not found", 1e9),
    ],
    ids=["success", "forbidden", "not_found"],
)
async def test_get_scenario_by_id(
    urban_api_host: str,
    scenario: dict[str, Any],
    valid_token: str,
    superuser_token: str,
    expected_status: int,
    error_message: str | None,
    scenario_id_param: int | None,
):
    """Test GET /scenarios/{scenario_id} method."""

    # Arrange
    scenario_id = scenario_id_param or scenario["scenario_id"]
    headers = {"Authorization": f"Bearer {valid_token if expected_status == 403 else superuser_token}"}

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get(f"/scenarios/{scenario_id}", headers=headers)
        result = response.json()

    # Assert
    assert_response(response, expected_status, Scenario, error_message)
    if response.status_code == 200:
        for k, v in scenario.items():
            if k in result:
                assert result[k] == v, f"Mismatch for {k}: {result[k]} != {v}."


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, project_id_param",
    [
        (201, None, None),
        (403, "denied", None),
        (404, "not found", 1e9),
    ],
    ids=["success", "forbidden", "not_found"],
)
async def test_add_scenario(
    urban_api_host: str,
    scenario_post_req: ScenarioPost,
    project: dict[str, Any],
    functional_zone_type: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    valid_token: str,
    superuser_token: str,
    project_id_param: int | None,
):
    """Test POST /scenarios method."""

    # Arrange
    new_scenario = scenario_post_req.model_dump()
    new_scenario["project_id"] = project_id_param or project["project_id"]
    new_scenario["functional_zone_type_id"] = functional_zone_type["functional_zone_type_id"]
    headers = {"Authorization": f"Bearer {valid_token if expected_status == 403 else superuser_token}"}

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.post("/scenarios", json=new_scenario, headers=headers)

    # Assert
    assert_response(response, expected_status, Scenario, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, scenario_id_param",
    [
        (201, None, None),
        (403, "denied", None),
        (404, "not found", 1e9),
    ],
    ids=["success", "forbidden", "not_found"],
)
async def test_copy_scenario(
    urban_api_host: str,
    scenario_post_req: ScenarioPost,
    project: dict[str, Any],
    functional_zone_type: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    valid_token: str,
    superuser_token: str,
    scenario_id_param: int | None,
):
    """Test POST /scenarios/{scenario_id} method."""

    # Arrange
    new_scenario = scenario_post_req.model_dump()
    scenario_id = scenario_id_param or project["base_scenario"]["id"]
    new_scenario["project_id"] = project["project_id"]
    new_scenario["functional_zone_type_id"] = functional_zone_type["functional_zone_type_id"]
    headers = {"Authorization": f"Bearer {valid_token if expected_status == 403 else superuser_token}"}

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.post(f"/scenarios/{scenario_id}", json=new_scenario, headers=headers)

    # Assert
    assert_response(response, expected_status, Scenario, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, scenario_id_param",
    [
        (200, None, None),
        (400, "change the one that should become the base, not the current one", None),
        (403, "denied", None),
        (404, "not found", 1e9),
    ],
    ids=["success", "bad_request", "forbidden", "not_found"],
)
async def test_put_scenario(
    urban_api_host: str,
    scenario_put_req: ScenarioPut,
    project: dict[str, Any],
    functional_zone_type: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    valid_token: str,
    superuser_token: str,
    scenario_id_param: int | None,
):
    """Test PUT /scenarios/{scenario_id} method."""

    # Arrange
    scenario_id = scenario_id_param or project["base_scenario"]["id"]
    new_scenario = scenario_put_req.model_dump()
    new_scenario["is_based"] = expected_status != 400
    new_scenario["functional_zone_type_id"] = functional_zone_type["functional_zone_type_id"]
    headers = {"Authorization": f"Bearer {valid_token if expected_status == 403 else superuser_token}"}

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.put(f"/scenarios/{scenario_id}", json=new_scenario, headers=headers)

    # Assert
    assert_response(response, expected_status, Scenario, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, scenario_id_param",
    [
        (200, None, None),
        (400, "change the one that should become the base, not the current one", None),
        (403, "denied", None),
        (404, "not found", 1e9),
    ],
    ids=["success", "bad_request", "forbidden", "not_found"],
)
async def test_patch_scenario(
    urban_api_host: str,
    scenario_put_req: ScenarioPut,
    project: dict[str, Any],
    functional_zone_type: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    valid_token: str,
    superuser_token: str,
    scenario_id_param: int | None,
):
    """Test PATCH /scenarios/{scenario_id} method."""

    # Arrange
    scenario_id = scenario_id_param or project["base_scenario"]["id"]
    new_scenario = scenario_put_req.model_dump()
    new_scenario["is_based"] = expected_status != 400
    new_scenario["functional_zone_type_id"] = functional_zone_type["functional_zone_type_id"]
    headers = {"Authorization": f"Bearer {valid_token if expected_status == 403 else superuser_token}"}

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.patch(f"/scenarios/{scenario_id}", json=new_scenario, headers=headers)

    # Assert
    assert_response(response, expected_status, Scenario, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, scenario_id_param",
    [
        (200, None, None),
        (403, "denied", None),
        (404, "not found", 1e9),
    ],
    ids=["success", "not_authenticated", "not_found"],
)
async def test_delete_scenario(
    urban_api_host: str,
    scenario_post_req: ScenarioPost,
    project: dict[str, Any],
    functional_zone_type: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    valid_token: str,
    superuser_token: str,
    scenario_id_param: int | None,
):
    """Test DELETE /scenarios/{scenario_id} method."""

    # Arrange
    new_scenario = scenario_post_req.model_dump()
    new_scenario["project_id"] = project["project_id"]
    new_scenario["functional_zone_type_id"] = functional_zone_type["functional_zone_type_id"]
    headers = {"Authorization": f"Bearer {valid_token if expected_status == 403 else superuser_token}"}

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        if scenario_id_param is None:
            response = await client.post(
                "/scenarios", json=new_scenario, headers={"Authorization": f"Bearer {superuser_token}"}
            )
            scenario_id = response.json()["scenario_id"]
            response = await client.delete(f"/scenarios/{scenario_id}", headers=headers)
        else:
            response = await client.delete(f"/scenarios/{scenario_id_param}", headers=headers)

    # Assert
    assert_response(response, expected_status, OkResponse, error_message)
