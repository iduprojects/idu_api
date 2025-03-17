"""Integration tests for territory-related indicators are defined here."""

from typing import Any

import httpx
import pytest
from pydantic import ValidationError

from idu_api.urban_api.schemas import Indicator, IndicatorValue, TerritoryWithIndicators
from idu_api.urban_api.schemas.geometries import GeoJSONResponse
from idu_api.urban_api.schemas.short_models import ShortIndicatorValueInfo
from tests.urban_api.helpers.utils import assert_response

####################################################################################
#                           Default use-case tests                                 #
####################################################################################


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, territory_id_param",
    [
        (200, None, None),
        (404, "not found", 1e9),
    ],
    ids=["success", "not_found"],
)
async def test_get_indicators_by_territory_id(
    urban_api_host: str,
    region: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    territory_id_param: int | None,
):
    """Test GET /territory/{territory_id}/indicators method."""

    # Arrange
    territory_id = territory_id_param or region["territory_id"]

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get(f"/territory/{territory_id}/indicators")
        result = response.json()

    # Assert
    if response.status_code == 200:
        assert_response(response, expected_status, Indicator, error_message, result_type="list")
        assert len(result) > 0, "Response should contain at least one indicator."
    else:
        assert_response(response, expected_status, Indicator, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, territory_id_param",
    [
        (200, None, None),
        (400, "You can use cities_only parameter only with including child territories", None),
        (404, "not found", 1e9),
    ],
    ids=["success", "bad_request", "not_found"],
)
async def test_get_indicator_values_by_territory_id(
    urban_api_host: str,
    indicator_value: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    territory_id_param: int | None,
):
    """Test GET /territory/{territory_id}/indicator_values method."""

    # Arrange
    territory_id = territory_id_param or indicator_value["territory"]["id"]
    params = {
        "indicator_ids": str(indicator_value["indicator"]["indicator_id"]),
        "start_date": indicator_value["date_value"],
        "end_date": indicator_value["date_value"],
        "value_type": indicator_value["value_type"],
        "information_source": indicator_value["information_source"],
        "last_only": True,
        "include_child_territories": expected_status != 400,
        "cities_only": expected_status == 400,
    }

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get(f"/territory/{territory_id}/indicator_values", params=params)
        result = response.json()

    # Assert
    if response.status_code == 200:
        assert_response(response, expected_status, IndicatorValue, error_message, result_type="list")
        assert len(result) > 0, "Response should contain at least one indicator value."
    else:
        assert_response(response, expected_status, IndicatorValue, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, territory_id_param",
    [
        (200, None, None),
        (404, "not found", 1e9),
    ],
    ids=["success", "not_found"],
)
async def test_get_indicator_values_by_parent_id(
    urban_api_host: str,
    country: dict[str, Any],
    indicator_value: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    territory_id_param: int | None,
):
    """Test GET /territory/indicator_values method."""

    # Arrange
    params = {
        "parent_id": territory_id_param or country["territory_id"],
        "indicator_ids": str(indicator_value["indicator"]["indicator_id"]),
        "start_date": indicator_value["date_value"],
        "end_date": indicator_value["date_value"],
        "value_type": indicator_value["value_type"],
        "information_source": indicator_value["information_source"],
        "last_only": True,
    }

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get("/territory/indicator_values", params=params)
        result = response.json()

    # Assert
    assert_response(response, expected_status, GeoJSONResponse, error_message)
    if response.status_code == 200:
        assert len(result["features"]) > 0, "Response should contain at least one territory."
        try:
            TerritoryWithIndicators(**result["features"][0]["properties"])
        except ValidationError as e:
            pytest.fail(f"Pydantic validation error: {str(e)}")
        assert (
            len(result["features"][0]["properties"]["indicators"]) > 0
        ), "Response should contain at least one indicator value."
        try:
            ShortIndicatorValueInfo(**result["features"][0]["properties"]["indicators"][0])
        except ValidationError as e:
            pytest.fail(f"Pydantic validation error: {str(e)}")
