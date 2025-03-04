"""Integration tests for territory-related functional zones are defined here."""

from typing import Any

import httpx
import pytest
from pydantic import ValidationError

from idu_api.urban_api.schemas import FunctionalZone, FunctionalZoneSource, FunctionalZoneWithoutGeometry, OkResponse
from idu_api.urban_api.schemas.geometries import GeoJSONResponse
from tests.urban_api.helpers.utils import assert_response

####################################################################################
#                           Default use-case tests                                 #
####################################################################################


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
async def test_get_functional_zone_sources_by_territory_id(
    urban_api_host: str,
    region: dict[str, Any],
    functional_zone: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    territory_id_param: int | None,
):
    """Test GET /territory/{territory_id}/functional_zone_sources method."""

    # Arrange
    territory_id = territory_id_param or region["territory_id"]
    if expected_status == 400:
        params = {"include_child_territories": False, "cities_only": True}
    else:
        params = {"include_child_territories": True, "cities_only": False}

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get(f"/territory/{territory_id}/functional_zone_sources", params=params)
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
    "expected_status, error_message, territory_id_param",
    [
        (200, None, None),
        (400, "You can use cities_only parameter only with including child territories", None),
        (404, "not found", 1e9),
    ],
    ids=["success", "bad_request", "not_found"],
)
async def test_get_functional_zones_by_territory_id(
    urban_api_host: str,
    region: dict[str, Any],
    functional_zone: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    territory_id_param: int | None,
):
    """Test GET /territory/{territory_id}/functional_zones method."""

    # Arrange
    territory_id = territory_id_param or region["territory_id"]
    params = {
        "include_child_territories": expected_status != 400,
        "cities_only": expected_status == 400,
        "year": functional_zone["year"],
        "source": functional_zone["source"],
    }

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get(f"/territory/{territory_id}/functional_zones", params=params)
        result = response.json()

    # Assert
    if response.status_code == 200:
        assert_response(response, expected_status, FunctionalZone, error_message, result_type="list")
        assert any(
            functional_zone["functional_zone_id"] == item["functional_zone_id"] for item in result
        ), "Response should contain created functional zone."
    else:
        assert_response(response, expected_status, FunctionalZone, error_message)


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
async def test_get_functional_zones_geojson_by_territory_id(
    urban_api_host: str,
    region: dict[str, Any],
    functional_zone: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    territory_id_param: int | None,
):
    """Test GET /territory/{territory_id}/functional_zones_geojson method."""

    # Arrange
    territory_id = territory_id_param or region["territory_id"]
    params = {
        "include_child_territories": expected_status != 400,
        "cities_only": expected_status == 400,
        "year": functional_zone["year"],
        "source": functional_zone["source"],
    }

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get(f"/territory/{territory_id}/functional_zones_geojson", params=params)
        result = response.json()

    # Assert
    assert_response(response, expected_status, GeoJSONResponse, error_message)
    if response.status_code == 200:
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
    "expected_status, error_message, territory_id_param",
    [
        (200, None, None),
        (400, "You can use cities_only parameter only with including child territories", None),
        (404, "not found", 1e9),
    ],
    ids=["success", "bad_request", "not_found"],
)
async def test_delete_all_functional_zones_by_territory_id(
    urban_api_host: str,
    region: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    territory_id_param: int | None,
):
    """Test DELETE /territory/{territory_id}/functional_zones method."""

    # Arrange
    territory_id = territory_id_param or region["territory_id"]
    if expected_status == 400:
        params = {"include_child_territories": False, "cities_only": True}
    else:
        params = {"include_child_territories": False, "cities_only": False}

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.delete(f"/territory/{territory_id}/functional_zones", params=params)

    # Assert
    assert_response(response, expected_status, OkResponse, error_message)
