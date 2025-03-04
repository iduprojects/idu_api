"""Integration tests for territory-related physical objects are defined here."""

from typing import Any

import httpx
import pytest
from pydantic import ValidationError

from idu_api.urban_api.schemas import Page, PhysicalObject, PhysicalObjectType, PhysicalObjectWithGeometry
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
async def test_get_physical_object_types_by_territory_id(
    urban_api_host: str,
    urban_object: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    territory_id_param: int | None,
):
    """Test GET /territory/{territory_id}/physical_object_types method."""

    # Arrange
    territory_id = territory_id_param or urban_object["object_geometry"]["territory"]["id"]
    params = {
        "include_child_territories": expected_status != 400,
        "cities_only": expected_status == 400,
    }

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get(f"/territory/{territory_id}/physical_object_types", params=params)

    # Assert
    if response.status_code == 200:
        assert_response(response, expected_status, PhysicalObjectType, error_message, result_type="list")
    else:
        assert_response(response, expected_status, PhysicalObjectType, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, territory_id_param, version",
    [
        (200, None, None, "v1"),
        (200, None, None, "v2"),
        (400, "You can use cities_only parameter only with including child territories", None, "v1"),
        (400, "Please, choose either physical_object_type_id or physical_object_function_id", None, "v2"),
        (404, "not found", 1e9, "v1"),
    ],
    ids=["success_v1", "success_v2", "bad_request_1", "bad_request_2", "not_found"],
)
async def test_get_physical_objects_by_territory_id(
    urban_api_host: str,
    urban_object: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    territory_id_param: int | None,
    version: str,
):
    """Test GET /territory/{territory_id}/physical_objects method."""

    # Arrange
    territory_id = territory_id_param or urban_object["object_geometry"]["territory"]["id"]
    params = {
        "physical_object_type_id": urban_object["physical_object"]["physical_object_type"][
            "physical_object_type_id"
        ],
        "name": urban_object["physical_object"]["name"],
        "include_child_territories": expected_status != 400 and version == "v1",
        "cities_only": expected_status == 400 and version == "v1",
        "page_size": 1,
    }
    if expected_status == 400 and version == "v2":
        params["physical_object_function_id"] = urban_object["physical_object"][
            "physical_object_type"
        ]["physical_object_function"]["id"]

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/{version}") as client:
        response = await client.get(f"/territory/{territory_id}/physical_objects", params=params)
        result = response.json()

    # Assert
    assert_response(response, expected_status, Page, error_message)
    if response.status_code == 200:
        assert len(result["results"]) > 0, "Response should contain at least one item."
        assert (
            len(result["results"]) <= params["page_size"]
        ), f"Response should contain no more than {params['page_size']} items."
        try:
            PhysicalObject(**result["results"][0])
        except ValidationError as e:
            pytest.fail(f"Pydantic validation error: {str(e)}")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, territory_id_param, version",
    [
        (200, None, None, "v1"),
        (200, None, None, "v2"),
        (400, "You can use cities_only parameter only with including child territories", None, "v1"),
        (400, "Please, choose either physical_object_type_id or physical_object_function_id", None, "v2"),
        (404, "not found", 1e9, "v1"),
    ],
    ids=["success_v1", "success_v2", "bad_request_1", "bad_request_2", "not_found"],
)
async def test_get_physical_objects_with_geometry_by_territory_id(
    urban_api_host: str,
    urban_object: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    territory_id_param: int | None,
    version: str,
):
    """Test GET /territory/{territory_id}/physical_objects_with_geometry method."""

    # Arrange
    territory_id = territory_id_param or urban_object["object_geometry"]["territory"]["id"]
    params = {
        "physical_object_type_id": urban_object["physical_object"]["physical_object_type"][
            "physical_object_type_id"
        ],
        "name": urban_object["physical_object"]["name"],
        "include_child_territories": expected_status != 400 and version == "v1",
        "cities_only": expected_status == 400 and version == "v1",
        "page_size": 1,
    }
    if expected_status == 400 and version == "v2":
        params["physical_object_function_id"] = urban_object["physical_object"][
            "physical_object_type"
        ]["physical_object_function"]["id"]

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/{version}") as client:
        response = await client.get(f"/territory/{territory_id}/physical_objects_with_geometry", params=params)
        result = response.json()

    # Assert
    assert_response(response, expected_status, Page, error_message)
    if response.status_code == 200:
        assert len(result["results"]) > 0, "Response should contain at least one item."
        assert (
            len(result["results"]) <= params["page_size"]
        ), f"Response should contain no more than {params['page_size']} items."
        try:
            PhysicalObjectWithGeometry(**result["results"][0])
        except ValidationError as e:
            pytest.fail(f"Pydantic validation error: {str(e)}")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, territory_id_param, version",
    [
        (200, None, None, None),
        (400, "You can use cities_only parameter only with including child territories", None, 1),
        (400, "Please, choose either physical_object_type_id or physical_object_function_id", None, 2),
        (404, "not found", 1e9, None),
    ],
    ids=["success", "bad_request_1", "bad_request_2", "not_found"],
)
async def test_get_physical_objects_geojson_by_territory_id(
    urban_api_host: str,
    urban_object: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    territory_id_param: int | None,
    version: int | None,
):
    """Test GET /territory/{territory_id}/physical_objects_geojson method."""

    # Arrange
    territory_id = territory_id_param or urban_object["object_geometry"]["territory"]["id"]
    params = {
        "physical_object_type_id": urban_object["physical_object"]["physical_object_type"][
            "physical_object_type_id"
        ],
        "name": urban_object["physical_object"]["name"],
        "include_child_territories": expected_status != 400 and version == 1,
        "cities_only": expected_status == 400 and version == 1,
    }
    if expected_status == 400 and version == 2:
        params["physical_object_function_id"] = urban_object["physical_object"][
            "physical_object_type"
        ]["physical_object_function"]["id"]

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get(f"/territory/{territory_id}/physical_objects_geojson", params=params)
        result = response.json()

    # Assert
    assert_response(response, expected_status, GeoJSONResponse, error_message)
    if response.status_code == 200:
        assert len(result["features"]) > 0, "Response should contain at least one feature."
        try:
            PhysicalObject(**result["features"][0]["properties"])
        except ValidationError as e:
            pytest.fail(f"Pydantic validation error: {str(e)}")
