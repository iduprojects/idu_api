"""Integration tests for urban objects are defined here."""

from typing import Any

import httpx
import pytest

from idu_api.urban_api.schemas import OkResponse, PhysicalObjectWithGeometryPost, UrbanObject
from tests.urban_api.helpers.utils import assert_response

####################################################################################
#                           Default use-case tests                                 #
####################################################################################


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, object_id_param",
    [
        (200, None, None),
        (404, "not found", 1e9),
    ],
    ids=["success", "not_found"],
)
async def test_get_urban_object_by_id(
    urban_api_host: str,
    urban_object: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    object_id_param: int | None,
):
    """Test GET /urban_objects method."""

    # Arrange
    urban_object_id = object_id_param or urban_object["urban_object_id"]

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get(f"/urban_objects/{urban_object_id}")
        result = response.json()

    # Assert
    assert_response(response, expected_status, UrbanObject, error_message)
    if response.status_code == 200:
        assert result["urban_object_id"] == urban_object_id, "Response did not match expected identifier."


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, object_id_param",
    [
        (200, None, None),
        (404, "not found", 1e9),
    ],
    ids=["success", "not_found"],
)
async def test_get_urban_objects_by_physical_object_id(
    urban_api_host: str,
    physical_object: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    object_id_param: int | None,
):
    """Test GET /urban_objects_by_physical_object method."""

    # Arrange
    physical_object_id = object_id_param or physical_object["physical_object_id"]

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get(
            "/urban_objects_by_physical_object", params={"physical_object_id": physical_object_id}
        )

    # Assert
    if response.status_code == 200:
        assert_response(response, expected_status, UrbanObject, error_message, result_type="list")
        assert len(response.json()) > 0, "Response should not be empty."
    else:
        assert_response(response, expected_status, UrbanObject, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, object_id_param",
    [
        (200, None, None),
        (404, "not found", 1e9),
    ],
    ids=["success", "not_found"],
)
async def test_get_urban_objects_by_object_geometry_id(
    urban_api_host: str,
    object_geometry: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    object_id_param: int | None,
):
    """Test GET /urban_objects_by_object_geometry method."""

    # Arrange
    object_geometry_id = object_id_param or object_geometry["object_geometry_id"]

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get(
            "/urban_objects_by_object_geometry", params={"object_geometry_id": object_geometry_id}
        )

    # Assert
    if response.status_code == 200:
        assert_response(response, expected_status, UrbanObject, error_message, result_type="list")
        assert len(response.json()) > 0, "Response should not be empty."
    else:
        assert_response(response, expected_status, UrbanObject, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, service_id_param",
    [
        (200, None, None),
        (404, "not found", 1e9),
    ],
    ids=["success", "not_found"],
)
async def test_get_urban_objects_by_service_id(
    urban_api_host: str,
    service: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    service_id_param: int | None,
):
    """Test GET /urban_objects_by_service_id method."""

    # Arrange
    service_id = service_id_param or service["service_id"]

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get("/urban_objects_by_service_id", params={"service_id": service_id})

    # Assert
    if response.status_code == 200:
        assert_response(response, expected_status, UrbanObject, error_message, result_type="list")
        assert len(response.json()) > 0, "Response should not be empty."
    else:
        assert_response(response, expected_status, UrbanObject, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, object_id_param",
    [
        (200, None, None),
        (404, "not found", 1e9),
    ],
    ids=["success", "not_found"],
)
async def test_delete_urban_object(
    urban_api_host: str,
    physical_object_with_geometry_post_req: PhysicalObjectWithGeometryPost,
    physical_object_type: dict[str, Any],
    city: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    object_id_param: int | None,
):
    """Test DELETE /urban_objects method."""

    # Arrange
    new_object = physical_object_with_geometry_post_req.model_dump()
    new_object["physical_object_type_id"] = physical_object_type["physical_object_type_id"]
    new_object["territory_id"] = city["territory_id"]

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        if object_id_param is None:
            response = await client.post("/physical_objects", json=new_object)
            urban_object_id = response.json()["urban_object_id"]
            response = await client.delete(f"/urban_objects/{urban_object_id}")
        else:
            response = await client.delete(f"/urban_objects/{object_id_param}")

    # Assert
    assert_response(response, expected_status, OkResponse, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, territory_id_param",
    [
        (200, None, None),
        (404, "not found", 1e9),
    ],
    ids=["success", "not_found"],
)
async def test_get_urban_objects_by_territory_id(
    urban_api_host: str,
    city: dict[str, Any],
    physical_object_type: dict[str, Any],
    service_type: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    territory_id_param: int | None,
):
    """Test GET /urban_objects_by_territory_id method."""

    # Arrange
    params = {
        "territory_id": territory_id_param or city["territory_id"],
        "physical_object_type_id": physical_object_type["physical_object_type_id"],
        "service_type_id": service_type["service_type_id"],
    }

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get("/urban_objects_by_territory_id", params=params)

    # Assert
    if response.status_code == 200:
        assert_response(response, expected_status, UrbanObject, error_message, result_type="list")
        assert len(response.json()) > 0, "Response should not be empty."
    else:
        assert_response(response, expected_status, UrbanObject, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, object_id_param",
    [
        (200, None, None),
        (404, "not found", 1e9),
        (409, "already exists", None),
    ],
    ids=["success", "not_found", "conflict"],
)
async def test_patch_urban_object(
    urban_api_host: str,
    physical_object_with_geometry_post_req: PhysicalObjectWithGeometryPost,
    physical_object: dict[str, Any],
    object_geometry: dict[str, Any],
    physical_object_type: dict[str, Any],
    city: dict[str, Any],
    service: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    object_id_param: int | None,
):
    """Test PATCH /urban_objects/{urban_object_id} method."""

    # Arrange
    new_object = physical_object_with_geometry_post_req.model_dump()
    new_object["physical_object_type_id"] = physical_object_type["physical_object_type_id"]
    new_object["territory_id"] = city["territory_id"]
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.post("/physical_objects", json=new_object)
    urban_object_id = object_id_param or response.json()["urban_object_id"]
    if expected_status != 409:
        json_data = {
            "physical_object_id": response.json()["physical_object"]["physical_object_id"],
            "object_geometry_id": response.json()["object_geometry"]["object_geometry_id"],
            "service_id": service["service_id"],
        }
    else:
        json_data = {
            "physical_object_id": physical_object["physical_object_id"],
            "object_geometry_id": object_geometry["object_geometry_id"],
            "service_id": service["service_id"],
        }

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.patch(f"/urban_objects/{urban_object_id}", json=json_data)

    # Assert
    assert_response(response, expected_status, UrbanObject, error_message)
