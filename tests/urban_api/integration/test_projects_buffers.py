"""Integration tests for project-related buffers are defined here."""

from typing import Any

import httpx
import pytest
from pydantic import ValidationError

from idu_api.urban_api.schemas import (
    OkResponse,
    PhysicalObjectWithGeometryPost,
    ScenarioBuffer,
    ScenarioBufferAttributes,
    ScenarioBufferDelete,
    ScenarioBufferPut,
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
        (400, "this method cannot be accessed in a REGIONAL scenario", None, True),
        (403, "denied", None, False),
        (404, "not found", 1e9, False),
    ],
    ids=["success_common", "regional", "forbidden", "not_found"],
)
async def test_get_buffers_by_scenario_id(
    urban_api_host: str,
    scenario: dict[str, Any],
    regional_scenario: dict[str, Any],
    scenario_buffer: dict[str, Any],
    valid_token: str,
    superuser_token: str,
    expected_status: int,
    error_message: str | None,
    scenario_id_param: int | None,
    is_regional_param: bool,
):
    """Test GET /scenarios/{scenario_id}/buffers method."""

    # Arrange
    scenario_id = scenario_id_param or (
        regional_scenario["scenario_id"] if is_regional_param else scenario["scenario_id"]
    )
    headers = {"Authorization": f"Bearer {valid_token if expected_status == 403 else superuser_token}"}

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get(f"/scenarios/{scenario_id}/buffers", headers=headers)
        result = response.json()

    # Assert
    assert_response(response, expected_status, GeoJSONResponse, error_message)
    if response.status_code == 200:
        assert len(result["features"]) > 0, "Response should contain at least one feature."
        try:
            ScenarioBufferAttributes(**result["features"][0]["properties"])
        except ValidationError as e:
            pytest.fail(f"Pydantic validation error: {str(e)}")


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
async def test_get_context_buffers(
    urban_api_host: str,
    scenario: dict[str, Any],
    regional_scenario: dict[str, Any],
    buffer: dict[str, Any],
    valid_token: str,
    superuser_token: str,
    expected_status: int,
    error_message: str | None,
    scenario_id_param: int | None,
    is_regional_param: bool,
):
    """Test GET /scenarios/{scenario_id}/context/buffers method."""

    # Arrange
    scenario_id = scenario_id_param or (
        regional_scenario["scenario_id"] if is_regional_param else scenario["scenario_id"]
    )
    headers = {"Authorization": f"Bearer {valid_token if expected_status == 403 else superuser_token}"}

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get(f"/scenarios/{scenario_id}/context/buffers", headers=headers)
        result = response.json()

    # Assert
    assert_response(response, expected_status, GeoJSONResponse, error_message)
    if response.status_code == 200:
        assert len(result["features"]) > 0, "Response should contain at least one feature."
        try:
            ScenarioBufferAttributes(**result["features"][0]["properties"])
        except ValidationError as e:
            pytest.fail(f"Pydantic validation error: {str(e)}")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, scenario_id_param, is_scenario_param",
    [
        (200, None, None, True),
        (200, None, None, False),
        (403, "denied", None, True),
        (404, "not found", 1e9, True),
        (409, "has already been edited or deleted for the scenario", None, False),
    ],
    ids=["success_1", "success_2", "forbidden", "not_found", "conflict"],
)
async def test_put_scenario_buffer(
    urban_api_host: str,
    scenario_buffer_put_req: ScenarioBufferPut,
    physical_object_with_geometry_post_req: PhysicalObjectWithGeometryPost,
    scenario: dict[str, Any],
    scenario_urban_object: dict[str, Any],
    buffer: dict[str, Any],
    buffer_type: dict[str, Any],
    default_buffer_value: dict[str, Any],
    city: dict[str, Any],
    valid_token: str,
    superuser_token: str,
    expected_status: int,
    error_message: str | None,
    scenario_id_param: int | None,
    is_scenario_param: bool,
):
    """Test PUT /scenarios/{scenario_id}/buffers method."""

    # Arrange
    scenario_id = scenario_id_param or scenario["scenario_id"]
    new_buffer = scenario_buffer_put_req.model_dump()
    new_buffer["buffer_type_id"] = buffer_type["buffer_type_id"]
    if is_scenario_param:
        new_buffer["physical_object_id"] = scenario_urban_object["physical_object"]["physical_object_id"]
        new_buffer["is_scenario_physical_object"] = scenario_urban_object["physical_object"]["is_scenario_object"]
        new_buffer["object_geometry_id"] = scenario_urban_object["object_geometry"]["object_geometry_id"]
        new_buffer["is_scenario_geometry"] = scenario_urban_object["object_geometry"]["is_scenario_object"]
        new_buffer["service_id"] = scenario_urban_object["service"]["service_id"]
        new_buffer["is_scenario_service"] = scenario_urban_object["service"]["is_scenario_object"]
    else:
        new_buffer["physical_object_id"] = buffer["urban_object"]["physical_object"]["id"]
        new_buffer["is_scenario_physical_object"] = False
        new_buffer["object_geometry_id"] = buffer["urban_object"]["object_geometry"]["id"]
        new_buffer["is_scenario_geometry"] = False
        new_buffer["service_id"] = buffer["urban_object"]["service"]["id"]
        new_buffer["is_scenario_service"] = False
    headers = {"Authorization": f"Bearer {valid_token if expected_status == 403 else superuser_token}"}

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.put(
            f"/scenarios/{scenario_id}/buffers",
            json=new_buffer,
            headers=headers,
        )

    # Assert
    assert_response(response, expected_status, ScenarioBuffer, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, scenario_id_param, is_scenario_param",
    [
        (200, None, None, True),
        (200, None, None, False),
        (403, "denied", None, True),
        (404, "not found", 1e9, True),
        (409, "has already been edited or deleted for the scenario", None, False),
    ],
    ids=["success_1", "success_2", "forbidden", "not_found", "conflict"],
)
async def test_delete_scenario_buffer(
    urban_api_host: str,
    scenario_buffer_delete_req: ScenarioBufferDelete,
    physical_object_with_geometry_post_req: PhysicalObjectWithGeometryPost,
    scenario: dict[str, Any],
    default_buffer_value: dict[str, Any],
    city: dict[str, Any],
    buffer_type: dict[str, Any],
    buffer: dict[str, Any],
    valid_token: str,
    superuser_token: str,
    expected_status: int,
    error_message: str | None,
    scenario_id_param: int | None,
    is_scenario_param: bool,
):
    """Test DELETE /scenarios/{scenario_id}/buffers method."""

    # Arrange
    scenario_id = scenario_id_param or scenario["scenario_id"]
    headers = {"Authorization": f"Bearer {valid_token if expected_status == 403 else superuser_token}"}
    new_buffer = scenario_buffer_delete_req.model_dump()
    new_buffer["buffer_type_id"] = buffer_type["buffer_type_id"]
    if is_scenario_param:
        new_object = physical_object_with_geometry_post_req.model_dump()
        new_object["physical_object_type_id"] = default_buffer_value["physical_object_type"]["id"]
        new_object["territory_id"] = city["territory_id"]
        async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
            response = await client.post(
                f"/scenarios/{scenario['scenario_id']}/physical_objects",
                headers={"Authorization": f"Bearer {superuser_token}"},
                json=new_object,
            )
            result = response.json()
        new_buffer["physical_object_id"] = result["physical_object"]["physical_object_id"]
        new_buffer["is_scenario_physical_object"] = True
        new_buffer["object_geometry_id"] = result["object_geometry"]["object_geometry_id"]
        new_buffer["is_scenario_geometry"] = True
        new_buffer["service_id"] = None
        new_buffer["is_scenario_service"] = True
    elif expected_status == 409:
        new_buffer["physical_object_id"] = buffer["urban_object"]["physical_object"]["id"]
        new_buffer["is_scenario_physical_object"] = False
        new_buffer["object_geometry_id"] = buffer["urban_object"]["object_geometry"]["id"]
        new_buffer["is_scenario_geometry"] = False
        new_buffer["service_id"] = buffer["urban_object"]["service"]["id"]
        new_buffer["is_scenario_service"] = False
    else:
        new_object = physical_object_with_geometry_post_req.model_dump()
        new_object["physical_object_type_id"] = default_buffer_value["physical_object_type"]["id"]
        new_object["territory_id"] = city["territory_id"]
        async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
            response = await client.post("/physical_objects", json=new_object)
            result = response.json()
        new_buffer["physical_object_id"] = result["physical_object"]["physical_object_id"]
        new_buffer["is_scenario_physical_object"] = False
        new_buffer["object_geometry_id"] = result["object_geometry"]["object_geometry_id"]
        new_buffer["is_scenario_geometry"] = False
        new_buffer["service_id"] = None
        new_buffer["is_scenario_service"] = False

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.request(
            "DELETE",
            f"/scenarios/{scenario_id}/buffers",
            headers=headers,
            json=new_buffer,
        )

    # Assert
    assert_response(response, expected_status, OkResponse, error_message)
