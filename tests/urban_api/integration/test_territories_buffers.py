from typing import Any

import httpx
import pytest
from pydantic import ValidationError

from idu_api.urban_api.schemas import BufferAttributes
from idu_api.urban_api.schemas.geometries import GeoJSONResponse
from tests.urban_api.helpers.utils import assert_response


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, territory_id_param, version",
    [
        (200, None, None, None),
        (400, "You can use cities_only parameter only with including child territories", None, 1),
        (400, "Please, choose either physical_object_type_id or service_type_id", None, 2),
        (404, "not found", 1e9, None),
    ],
    ids=["success", "bad_request_1", "bad_request_2", "not_found"],
)
async def test_get_buffers_geojson_by_territory_id(
    urban_api_host: str,
    buffer: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    territory_id_param: int | None,
    version: int | None,
):
    """Test GET /territory/{territory_id}/buffers_geojson method."""

    # Arrange
    territory_id = territory_id_param or buffer["urban_object"]["object_geometry"]["territory"]["id"]
    params = {
        "physical_object_type_id": buffer["urban_object"]["physical_object"]["type"]["id"],
        "include_child_territories": expected_status != 400 and version == 1,
        "cities_only": expected_status == 400 and version == 1,
    }
    if expected_status == 400 and version == 2:
        params["service_type_id"] = 1

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get(f"/territory/{territory_id}/buffers_geojson", params=params)
        result = response.json()

    # Assert
    assert_response(response, expected_status, GeoJSONResponse, error_message)
    if response.status_code == 200:
        assert len(result["features"]) > 0, "Response should contain at least one feature."
        try:
            BufferAttributes(**result["features"][0]["properties"])
        except ValidationError as e:
            pytest.fail(f"Pydantic validation error: {str(e)}")
