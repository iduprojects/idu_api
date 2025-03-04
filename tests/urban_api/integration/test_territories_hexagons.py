"""Integration tests for territory-related hexagons are defined here."""

from typing import Any

import httpx
import pytest
from pydantic import ValidationError

from idu_api.urban_api.schemas import Hexagon, HexagonAttributes, HexagonPost, OkResponse
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
        (404, "not found", 1e9),
    ],
    ids=["success", "not_found"],
)
async def test_get_hexagons_by_territory_id(
    urban_api_host: str,
    region: dict[str, Any],
    hexagon: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    territory_id_param: int | None,
):
    """Test GET /territory/{territory_id}/hexagons method."""

    # Arrange
    territory_id = territory_id_param or region["territory_id"]

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get(f"/territory/{territory_id}/hexagons")
        result = response.json()

    # Assert
    assert_response(response, expected_status, GeoJSONResponse, error_message)
    if response.status_code == 200:
        try:
            HexagonAttributes(**result["features"][0]["properties"])
        except ValidationError as e:
            pytest.fail(f"Pydantic validation error: {str(e)}")
        assert any(
            hexagon["hexagon_id"] == item["properties"]["hexagon_id"] for item in result["features"]
        ), "Response should contain created hexagon."


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, territory_id_param",
    [
        (201, None, None),
        (404, "not found", 1e9),
        (409, "already exists", None),
    ],
    ids=["success", "not_found", "conflict"],
)
async def test_add_hexagons_by_territory_id(
    urban_api_host: str,
    hexagon_post_req: HexagonPost,
    region: dict[str, Any],
    country: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    territory_id_param: int | None,
):
    """Test POST /territory/{territory_id}/hexagons method."""

    # Arrange
    territory = region if expected_status == 409 else country
    territory_id = territory_id_param or territory["territory_id"]

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.post(f"/territory/{territory_id}/hexagons", json=[hexagon_post_req.model_dump()])

    # Assert
    if response.status_code == 201:
        assert_response(response, expected_status, Hexagon, error_message, result_type="list")
    else:
        assert_response(response, expected_status, Hexagon, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, territory_id_param",
    [
        (200, None, None),
        (404, "not found", 1e9),
    ],
    ids=["success", "not_found"],
)
async def test_delete_hexagons_by_territory_id(
    urban_api_host: str,
    country: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    territory_id_param: int | None,
):
    """Test DELETE /territory/{territory_id}/hexagons method."""

    # Arrange
    territory_id = territory_id_param or country["territory_id"]

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.delete(f"/territory/{territory_id}/hexagons")

    # Assert
    assert_response(response, expected_status, OkResponse, error_message)
