"""Integration tests for project-related indicators values are defined here."""

import asyncio
from typing import Any

import httpx
import pytest
from otteroad import KafkaConsumerService
from otteroad.models import RegionalScenarioIndicatorsUpdated, ScenarioIndicatorsUpdated
from pydantic import ValidationError

from idu_api.urban_api.schemas import (
    HexagonWithIndicators,
    OkResponse,
    ScenarioIndicatorValue,
    ScenarioIndicatorValuePatch,
    ScenarioIndicatorValuePost,
    ScenarioIndicatorValuePut,
)
from idu_api.urban_api.schemas.geometries import GeoJSONResponse
from tests.urban_api.helpers.broker import mock_handler
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
async def test_get_indicators_values_by_scenario_id(
    urban_api_host: str,
    scenario_indicator_value: dict[str, Any],
    valid_token: str,
    superuser_token: str,
    expected_status: int,
    error_message: str | None,
    scenario_id_param: int | None,
):
    """Test GET /scenarios/{scenario_id}/indicators_values method."""

    # Arrange
    scenario_id = scenario_id_param or scenario_indicator_value["scenario"]["id"]
    headers = {"Authorization": f"Bearer {valid_token if expected_status == 403 else superuser_token}"}
    params = {
        "indicator_ids": str(scenario_indicator_value["indicator"]["indicator_id"]),
        "territory_id": scenario_indicator_value["territory"]["id"],
    }

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get(f"/scenarios/{scenario_id}/indicators_values", headers=headers, params=params)
        result = response.json()

    # Assert
    if response.status_code == 200:
        assert_response(response, expected_status, ScenarioIndicatorValue, error_message, result_type="list")
        assert any(
            scenario_indicator_value["indicator_value_id"] == item["indicator_value_id"] for item in result
        ), "Response should contain created indicator value."
    else:
        assert_response(response, expected_status, ScenarioIndicatorValue, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, scenario_id_param, is_regional_param",
    [
        (201, None, None, False),
        (201, None, None, True),
        (403, "denied", None, False),
        (404, "not found", 1e9, False),
        (409, "already exists", None, False),
    ],
    ids=["success_common", "success_regional", "forbidden", "not_found", "conflict"],
)
async def test_add_scenario_indicator_value(
    urban_api_host: str,
    scenario_indicator_value_post_req: ScenarioIndicatorValuePost,
    scenario: dict[str, Any],
    regional_scenario: dict[str, Any],
    scenario_indicator_value: dict[str, Any],
    kafka_consumer: KafkaConsumerService,
    valid_token: str,
    superuser_token: str,
    expected_status: int,
    error_message: str | None,
    scenario_id_param: int | None,
    is_regional_param: bool,
):
    """Test POST /scenarios/{scenario_id}/indicators_values method."""

    # Arrange
    model = RegionalScenarioIndicatorsUpdated if is_regional_param else ScenarioIndicatorsUpdated
    new_handler = mock_handler(model)
    kafka_consumer.register_handler(new_handler)
    scenario_id = scenario_id_param or (
        regional_scenario["scenario_id"] if is_regional_param else scenario["scenario_id"]
    )
    new_value = scenario_indicator_value_post_req.model_dump()
    new_value["indicator_id"] = scenario_indicator_value["indicator"]["indicator_id"]
    new_value["scenario_id"] = scenario_id
    new_value["territory_id"] = scenario_indicator_value["territory"]["id"] if expected_status == 409 else None
    new_value["hexagon_id"] = None
    headers = {"Authorization": f"Bearer {valid_token if expected_status == 403 else superuser_token}"}

    # Act
    if expected_status == 201:
        await asyncio.sleep(5)
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.post(f"/scenarios/{scenario_id}/indicators_values", headers=headers, json=new_value)
    if expected_status == 201:
        await asyncio.sleep(5)

    # Assert
    assert_response(response, expected_status, ScenarioIndicatorValue, error_message)
    if expected_status == 201:
        assert len(new_handler.received_events) == 1, "No one event was received"
        assert isinstance(new_handler.received_events[0], model), f"Received event is not {model.__name__}"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, scenario_id_param, is_regional_param",
    [
        (200, None, None, False),
        (200, None, None, True),
        (403, "denied", None, False),
        (404, "not found", 1e9, False),
    ],
    ids=["success_common", "success_regional", "forbidden", "not_found"],
)
async def test_put_scenario_indicator_value(
    urban_api_host: str,
    scenario_indicator_value_put_req: ScenarioIndicatorValuePut,
    scenario: dict[str, Any],
    regional_scenario: dict[str, Any],
    scenario_indicator_value: dict[str, Any],
    kafka_consumer: KafkaConsumerService,
    valid_token: str,
    superuser_token: str,
    expected_status: int,
    error_message: str | None,
    scenario_id_param: int | None,
    is_regional_param: bool,
):
    """Test PUT /scenarios/{scenario_id}/indicators_values method."""

    # Arrange
    model = RegionalScenarioIndicatorsUpdated if is_regional_param else ScenarioIndicatorsUpdated
    new_handler = mock_handler(model)
    kafka_consumer.register_handler(new_handler)
    scenario_id = scenario_id_param or (
        regional_scenario["scenario_id"] if is_regional_param else scenario["scenario_id"]
    )
    new_value = scenario_indicator_value_put_req.model_dump()
    new_value["indicator_id"] = scenario_indicator_value["indicator"]["indicator_id"]
    new_value["scenario_id"] = scenario_id
    new_value["territory_id"] = scenario_indicator_value["territory"]["id"]
    new_value["hexagon_id"] = None
    headers = {"Authorization": f"Bearer {valid_token if expected_status == 403 else superuser_token}"}

    # Act
    if expected_status == 200:
        await asyncio.sleep(5)
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.put(f"/scenarios/{scenario_id}/indicators_values", headers=headers, json=new_value)
    if expected_status == 200:
        await asyncio.sleep(5)

    # Assert
    assert_response(response, expected_status, ScenarioIndicatorValue, error_message)
    if expected_status == 200:
        assert len(new_handler.received_events) == 1, "No one event was received"
        assert isinstance(new_handler.received_events[0], model), f"Received event is not {model.__name__}"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, scenario_id_param, is_regional_param",
    [
        (200, None, None, False),
        (200, None, None, True),
        (403, "denied", None, False),
        (404, "not found", 1e9, False),
    ],
    ids=["success_common", "success_regional", "forbidden", "not_found"],
)
async def test_patch_scenario_indicator_value(
    urban_api_host: str,
    scenario_indicator_value_patch_req: ScenarioIndicatorValuePatch,
    scenario: dict[str, Any],
    regional_scenario: dict[str, Any],
    scenario_indicator_value: dict[str, Any],
    kafka_consumer: KafkaConsumerService,
    valid_token: str,
    superuser_token: str,
    expected_status: int,
    error_message: str | None,
    scenario_id_param: int | None,
    is_regional_param: bool,
):
    """Test PATCH /scenarios/{scenario_id}/indicators_values/{indicator_value_id} method."""

    # Arrange
    model = RegionalScenarioIndicatorsUpdated if is_regional_param else ScenarioIndicatorsUpdated
    new_handler = mock_handler(model)
    kafka_consumer.register_handler(new_handler)
    scenario_id = scenario_id_param or (
        regional_scenario["scenario_id"] if is_regional_param else scenario["scenario_id"]
    )
    indicator_value_id = scenario_indicator_value["indicator_value_id"]
    new_value = scenario_indicator_value_patch_req.model_dump()
    headers = {"Authorization": f"Bearer {valid_token if expected_status == 403 else superuser_token}"}

    # Act
    if expected_status == 200:
        await asyncio.sleep(5)
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.patch(
            f"/scenarios/{scenario_id}/indicators_values/{indicator_value_id}",
            headers=headers,
            json=new_value,
        )
    if expected_status == 200:
        await asyncio.sleep(5)

    # Assert
    assert_response(response, expected_status, ScenarioIndicatorValue, error_message)
    if expected_status == 200:
        assert len(new_handler.received_events) == 1, "No one event was received"
        assert isinstance(new_handler.received_events[0], model), f"Received event is not {model.__name__}"


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
async def test_delete_indicators_values_by_scenario_id(
    urban_api_host: str,
    project: dict[str, Any],
    functional_zone_type: dict[str, Any],
    valid_token: str,
    superuser_token: str,
    expected_status: int,
    error_message: str | None,
    scenario_id_param: int | None,
):
    """Test DELETE /scenarios/{scenario_id}/indicators_values method."""

    # Arrange
    if scenario_id_param is None:
        base_scenario_id = project["base_scenario"]["id"]
        headers = {"Authorization": f"Bearer {superuser_token}"}
        new_scenario = {
            "project_id": project["project_id"],
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
        response = await client.delete(f"/scenarios/{scenario_id}/indicators_values", headers=headers)

    # Assert
    assert_response(response, expected_status, OkResponse, error_message)


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
async def test_delete_scenario_indicator_value_by_id(
    urban_api_host: str,
    scenario_indicator_value_post_req: ScenarioIndicatorValuePost,
    scenario_indicator_value: dict[str, Any],
    valid_token: str,
    superuser_token: str,
    expected_status: int,
    error_message: str | None,
    scenario_id_param: int | None,
):
    """Test DELETE /scenarios/{scenario_id}/indicators_values/{indicator_value_id} method."""

    # Arrange
    scenario_id = scenario_indicator_value["scenario"]["id"]
    new_value = scenario_indicator_value_post_req.model_dump()
    new_value["indicator_id"] = scenario_indicator_value["indicator"]["indicator_id"]
    new_value["territory_id"] = None
    new_value["hexagon_id"] = None
    headers = {"Authorization": f"Bearer {valid_token if expected_status == 403 else superuser_token}"}

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        if scenario_id_param is None:
            response = await client.post(
                f"/scenarios/{scenario_id}/indicators_values",
                headers={"Authorization": f"Bearer {superuser_token}"},
                json=new_value,
            )
            indicator_value_id = response.json()["indicator_value_id"]
            response = await client.delete(
                f"/scenarios/{scenario_id}/indicators_values/{indicator_value_id}", headers=headers
            )
        else:
            response = await client.delete(f"/scenarios/{scenario_id_param}/indicators_values/1", headers=headers)

    # Assert
    assert_response(response, expected_status, OkResponse, error_message)


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
async def test_get_hexagons_with_indicators_values_by_scenario_id(
    urban_api_host: str,
    scenario_hexagon_indicator_value: dict[str, Any],
    valid_token: str,
    superuser_token: str,
    expected_status: int,
    error_message: str | None,
    scenario_id_param: int | None,
):
    """Test GET /scenarios/{scenario_id}/indicators_values/hexagons method."""

    # Arrange
    scenario_id = scenario_id_param or scenario_hexagon_indicator_value["scenario"]["id"]
    headers = {"Authorization": f"Bearer {valid_token if expected_status == 403 else superuser_token}"}
    params = {"indicator_ids": scenario_hexagon_indicator_value["indicator"]["indicator_id"]}

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get(
            f"/scenarios/{scenario_id}/indicators_values/hexagons",
            headers=headers,
            params=params,
        )
        result = response.json()

    # Assert
    assert_response(response, expected_status, GeoJSONResponse, error_message)
    if response.status_code == 200:
        assert len(result["features"]) > 0, "Response should contain at least one feature."
        try:
            HexagonWithIndicators(**result["features"][0]["properties"])
        except ValidationError as e:
            pytest.fail(f"Pydantic validation error: {str(e)}")
        assert any(
            scenario_hexagon_indicator_value["hexagon_id"] == item["properties"]["hexagon_id"]
            for item in result["features"]
        ), "Response should contain created hexagon."
