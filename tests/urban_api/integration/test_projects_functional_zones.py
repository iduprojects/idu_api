"""Integration tests for project-related functional zones are defined here."""

from typing import Any

import httpx
import pytest
from pydantic import ValidationError

from idu_api.urban_api.schemas import (
    FunctionalZoneSource,
    FunctionalZoneWithoutGeometry,
    OkResponse,
    ScenarioFunctionalZone,
    ScenarioFunctionalZonePost,
    ScenarioFunctionalZonePut,
    ScenarioFunctionalZoneWithoutGeometry,
)
from idu_api.urban_api.schemas.geometries import GeoJSONResponse
from tests.urban_api.helpers.utils import assert_response

####################################################################################
#                           Default use-case tests                                 #
####################################################################################


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, scenario_id_param, is_regional_param",
    [
        (200, None, None, False),
        (400, "this method cannot be accessed in a regional scenario", None, True),
        (403, "denied", None, False),
        (404, "not found", 1e9, False),
    ],
    ids=["success", "regional_scenario", "forbidden", "not_found"],
)
async def test_get_functional_zone_sources_by_scenario_id(
    urban_api_host: str,
    scenario: dict[str, Any],
    regional_scenario: dict[str, Any],
    scenario_functional_zone: dict[str, Any],
    valid_token: str,
    superuser_token: str,
    expected_status: int,
    error_message: str | None,
    scenario_id_param: int | None,
    is_regional_param: bool,
):
    """Test GET /scenarios/{scenario_id}/functional_zone_sources method."""

    # Arrange
    scenario_id = scenario_id_param or (
        regional_scenario["scenario_id"] if is_regional_param else scenario["scenario_id"]
    )
    headers = {"Authorization": f"Bearer {valid_token if expected_status == 403 else superuser_token}"}

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get(f"/scenarios/{scenario_id}/functional_zone_sources", headers=headers)
        result = response.json()

    # Assert
    if response.status_code == 200:
        assert_response(response, expected_status, FunctionalZoneSource, error_message, result_type="list")
        assert any(
            scenario_functional_zone["year"] == item["year"] for item in result
        ), "Response should contain created year."
        assert any(
            scenario_functional_zone["source"] == item["source"] for item in result
        ), "Response should contain created source."
    else:
        assert_response(response, expected_status, FunctionalZoneSource, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, scenario_id_param, is_regional_param",
    [
        (200, None, None, False),
        (400, "this method cannot be accessed in a regional scenario", None, True),
        (403, "denied", None, False),
        (404, "not found", 1e9, False),
    ],
    ids=["success", "regional_scenario", "forbidden", "not_found"],
)
async def test_get_functional_zones_by_scenario_id(
    urban_api_host: str,
    scenario: dict[str, Any],
    regional_scenario: dict[str, Any],
    scenario_functional_zone: dict[str, Any],
    valid_token: str,
    superuser_token: str,
    expected_status: int,
    error_message: str | None,
    scenario_id_param: int | None,
    is_regional_param: bool,
):
    """Test GET /scenarios/{scenario_id}/functional_zones method."""

    # Arrange
    scenario_id = scenario_id_param or (
        regional_scenario["scenario_id"] if is_regional_param else scenario["scenario_id"]
    )
    headers = {"Authorization": f"Bearer {valid_token if expected_status == 403 else superuser_token}"}
    params = {"year": scenario_functional_zone["year"], "source": scenario_functional_zone["source"]}

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get(f"/scenarios/{scenario_id}/functional_zones", headers=headers, params=params)
        result = response.json()

    # Assert
    assert_response(response, expected_status, GeoJSONResponse, error_message)
    if response.status_code == 200:
        assert len(result["features"]) > 0, "Response should contain at least one feature."
        try:
            ScenarioFunctionalZoneWithoutGeometry(**result["features"][0]["properties"])
        except ValidationError as e:
            pytest.fail(f"Pydantic validation error: {str(e)}")
        assert any(
            scenario_functional_zone["functional_zone_id"] == item["properties"]["functional_zone_id"]
            for item in result["features"]
        ), "Response should contain created functional zone."


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, scenario_id_param, is_regional_param",
    [
        (200, None, None, False),
        (400, "this method cannot be accessed in a regional scenario", None, True),
        (403, "denied", None, False),
        (404, "not found", 1e9, False),
    ],
    ids=["success", "regional_scenario", "forbidden", "not_found"],
)
async def test_get_context_functional_zone_sources(
    urban_api_host: str,
    scenario: dict[str, Any],
    regional_scenario: dict[str, Any],
    functional_zone: dict[str, Any],
    valid_token: str,
    superuser_token: str,
    expected_status: int,
    error_message: str | None,
    scenario_id_param: int | None,
    is_regional_param: bool,
):
    """Test GET /scenarios/{scenario_id}/context/functional_zone_sources method."""

    # Arrange
    scenario_id = scenario_id_param or (
        regional_scenario["scenario_id"] if is_regional_param else scenario["scenario_id"]
    )
    headers = {"Authorization": f"Bearer {valid_token if expected_status == 403 else superuser_token}"}

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get(f"/scenarios/{scenario_id}/context/functional_zone_sources", headers=headers)
        result = response.json()

    # Assert
    if response.status_code == 200:
        assert_response(response, expected_status, FunctionalZoneSource, error_message, result_type="list")
        assert any(functional_zone["year"] == item["year"] for item in result), "Response should contain created year."
        assert any(
            functional_zone["source"] == item["source"] for item in result
        ), "Response should contain created source."
    else:
        assert_response(response, expected_status, FunctionalZoneSource, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, scenario_id_param, is_regional_param",
    [
        (200, None, None, False),
        (400, "this method cannot be accessed in a regional scenario", None, True),
        (403, "denied", None, False),
        (404, "not found", 1e9, False),
    ],
    ids=["success", "regional_scenario", "forbidden", "not_found"],
)
async def test_get_context_functional_zones(
    urban_api_host: str,
    scenario: dict[str, Any],
    regional_scenario: dict[str, Any],
    functional_zone: dict[str, Any],
    valid_token: str,
    superuser_token: str,
    expected_status: int,
    error_message: str | None,
    scenario_id_param: int | None,
    is_regional_param: bool,
):
    """Test GET /scenarios/{scenario_id}/context/functional_zones method."""

    # Arrange
    scenario_id = scenario_id_param or (
        regional_scenario["scenario_id"] if is_regional_param else scenario["scenario_id"]
    )
    headers = {"Authorization": f"Bearer {valid_token if expected_status == 403 else superuser_token}"}
    params = {"year": functional_zone["year"], "source": functional_zone["source"]}

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get(
            f"/scenarios/{scenario_id}/context/functional_zones", headers=headers, params=params
        )
        result = response.json()

    # Assert
    assert_response(response, expected_status, GeoJSONResponse, error_message)
    if response.status_code == 200:
        assert len(result["features"]) > 0, "Response should contain at least one feature."
        try:
            FunctionalZoneWithoutGeometry(**result["features"][0]["properties"])
        except ValidationError as e:
            pytest.fail(f"Pydantic validation error: {str(e)}")
        assert any(
            functional_zone["functional_zone_id"] == item["properties"]["functional_zone_id"]
            for item in result["features"]
        ), "Response should contain created functional zone."


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, scenario_id_param, is_regional_param",
    [
        (201, None, None, False),
        (400, "this method cannot be accessed in a regional scenario", None, True),
        (403, "denied", None, False),
        (404, "not found", 1e9, False),
    ],
    ids=["success", "regional_scenario", "forbidden", "not_found"],
)
async def test_add_scenario_functional_zones(
    urban_api_host: str,
    scenario_functional_zone_post_req: ScenarioFunctionalZonePost,
    project: dict[str, Any],
    base_regional_scenario: dict[str, Any],
    regional_project: dict[str, Any],
    functional_zone_type: dict[str, Any],
    valid_token: str,
    superuser_token: str,
    expected_status: int,
    error_message: str | None,
    scenario_id_param: int | None,
    is_regional_param: bool,
):
    """Test POST /scenarios/{scenario_id}/functional_zones method."""

    # Arrange
    if scenario_id_param is None:
        base_scenario_id = (
            project["base_scenario"]["id"] if not is_regional_param else base_regional_scenario["scenario_id"]
        )
        headers = {"Authorization": f"Bearer {superuser_token}"}
        new_scenario = {
            "project_id": project["project_id"] if not is_regional_param else regional_project["project_id"],
            "name": "Test Scenario Name",
            "functional_zone_type_id": functional_zone_type["functional_zone_type_id"],
        }
        async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
            response = await client.post(f"/scenarios/{base_scenario_id}", json=new_scenario, headers=headers)
            scenario_id = response.json()["scenario_id"]
    else:
        scenario_id = scenario_id_param
    new_functional_zone = scenario_functional_zone_post_req.model_dump()
    new_functional_zone["functional_zone_type_id"] = functional_zone_type["functional_zone_type_id"]
    headers = {"Authorization": f"Bearer {valid_token if expected_status == 403 else superuser_token}"}

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.post(
            f"/scenarios/{scenario_id}/functional_zones", json=[new_functional_zone], headers=headers
        )

    # Assert
    if response.status_code == 201:
        assert_response(response, expected_status, ScenarioFunctionalZone, error_message, result_type="list")
    else:
        assert_response(response, expected_status, ScenarioFunctionalZone, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, scenario_id_param, is_regional_param",
    [
        (200, None, None, False),
        (400, "this method cannot be accessed in a regional scenario", None, True),
        (403, "denied", None, False),
        (404, "not found", 1e9, False),
    ],
    ids=["success", "regional_scenario", "forbidden", "not_found"],
)
async def test_put_scenario_functional_zone(
    urban_api_host: str,
    scenario_functional_zone_put_req: ScenarioFunctionalZonePut,
    scenario: dict[str, Any],
    regional_scenario: dict[str, Any],
    scenario_functional_zone: dict[str, Any],
    valid_token: str,
    superuser_token: str,
    expected_status: int,
    error_message: str | None,
    scenario_id_param: int | None,
    is_regional_param: bool,
):
    """Test PUT /scenarios/{scenario_id}/functional_zones method."""

    # Arrange
    scenario_id = scenario_id_param or (
        regional_scenario["scenario_id"] if is_regional_param else scenario["scenario_id"]
    )
    functional_zone_id = scenario_functional_zone["functional_zone_id"]
    new_functional_zone = scenario_functional_zone_put_req.model_dump()
    new_functional_zone["functional_zone_type_id"] = scenario_functional_zone["functional_zone_type"]["id"]
    headers = {"Authorization": f"Bearer {valid_token if expected_status == 403 else superuser_token}"}

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.put(
            f"/scenarios/{scenario_id}/functional_zones/{functional_zone_id}",
            json=new_functional_zone,
            headers=headers,
        )

    # Assert
    assert_response(response, expected_status, ScenarioFunctionalZone, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, scenario_id_param, is_regional_param",
    [
        (200, None, None, False),
        (400, "this method cannot be accessed in a regional scenario", None, True),
        (403, "denied", None, False),
        (404, "not found", 1e9, False),
    ],
    ids=["success", "regional_scenario", "forbidden", "not_found"],
)
async def test_patch_scenario_functional_zone(
    urban_api_host: str,
    scenario_functional_zone_put_req: ScenarioFunctionalZonePut,
    scenario: dict[str, Any],
    regional_scenario: dict[str, Any],
    scenario_functional_zone: dict[str, Any],
    valid_token: str,
    superuser_token: str,
    expected_status: int,
    error_message: str | None,
    scenario_id_param: int | None,
    is_regional_param: bool,
):
    """Test PATCH /scenarios/{scenario_id}/functional_zones method."""

    # Arrange
    scenario_id = scenario_id_param or (
        regional_scenario["scenario_id"] if is_regional_param else scenario["scenario_id"]
    )
    functional_zone_id = scenario_functional_zone["functional_zone_id"]
    new_functional_zone = scenario_functional_zone_put_req.model_dump()
    new_functional_zone["functional_zone_type_id"] = scenario_functional_zone["functional_zone_type"]["id"]
    headers = {"Authorization": f"Bearer {valid_token if expected_status == 403 else superuser_token}"}

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.patch(
            f"/scenarios/{scenario_id}/functional_zones/{functional_zone_id}",
            json=new_functional_zone,
            headers=headers,
        )

    # Assert
    assert_response(response, expected_status, ScenarioFunctionalZone, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, scenario_id_param, is_regional_param",
    [
        (200, None, None, False),
        (400, "this method cannot be accessed in a regional scenario", None, True),
        (403, "denied", None, False),
        (404, "not found", 1e9, False),
    ],
    ids=["success", "regional_scenario", "forbidden", "not_found"],
)
async def test_delete_functional_zones_by_scenario_id(
    urban_api_host: str,
    project: dict[str, Any],
    base_regional_scenario: dict[str, Any],
    regional_project: dict[str, Any],
    functional_zone_type: dict[str, Any],
    valid_token: str,
    superuser_token: str,
    expected_status: int,
    error_message: str | None,
    scenario_id_param: int | None,
    is_regional_param: bool,
):
    """Test DELETE /scenarios/{scenario_id}/functional_zones method."""

    # Arrange
    if scenario_id_param is None:
        base_scenario_id = (
            project["base_scenario"]["id"] if not is_regional_param else base_regional_scenario["scenario_id"]
        )
        headers = {"Authorization": f"Bearer {superuser_token}"}
        new_scenario = {
            "project_id": project["project_id"] if not is_regional_param else regional_project["project_id"],
            "name": "Test Scenario Name",
            "functional_zone_type_id": functional_zone_type["functional_zone_type_id"],
        }
        async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
            response = await client.post(f"/scenarios/{base_scenario_id}", json=new_scenario, headers=headers)
            scenario_id = response.json()["scenario_id"]
    else:
        scenario_id = scenario_id_param
    headers = {"Authorization": f"Bearer {valid_token if expected_status == 403 else superuser_token}"}

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.delete(f"/scenarios/{scenario_id}/functional_zones", headers=headers)

    # Assert
    assert_response(response, expected_status, OkResponse, error_message)
