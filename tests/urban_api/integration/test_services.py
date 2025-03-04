"""Integration tests for services are defined here."""

from typing import Any

import httpx
import pytest

from idu_api.urban_api.schemas import (
    OkResponse,
    PhysicalObjectWithGeometryPost,
    Service,
    ServicePost,
    ServicePut,
    UrbanObject,
)
from tests.urban_api.helpers.utils import assert_response

####################################################################################
#                           Default use-case tests                                 #
####################################################################################


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, service_id_param",
    [
        (200, None, None),
        (404, "not found", 1e9),
    ],
    ids=["success", "not_found"],
)
async def test_get_service_by_id(
    urban_api_host: str,
    service: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    service_id_param: int | None,
):
    """Test GET /services method."""

    # Arrange
    service_id = service_id_param or service["service_id"]

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get(f"/services/{service_id}")
        result = response.json()

    # Assert
    assert_response(response, expected_status, Service, error_message)
    if response.status_code == 200:
        for k, v in service.items():
            if k in result and k not in ("service_type", "territory_type", "territories"):
                assert result[k] == v, f"Mismatch for {k}: {result[k]} != {v}."


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, service_type_id_param, territory_type_id_param",
    [
        (201, None, None, None),
        (404, "not found", 1e9, 1e9),
    ],
    ids=["success", "not_found"],
)
async def test_add_service(
    urban_api_host: str,
    service_post_req: ServicePost,
    service_type: dict[str, Any],
    urban_object: dict[str, Any],
    territory_type: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    service_type_id_param: int | None,
    territory_type_id_param: int | None,
):
    """Test POST /services method."""

    # Arrange
    new_service = service_post_req.model_dump()
    new_service["physical_object_id"] = urban_object["physical_object"]["physical_object_id"]
    new_service["object_geometry_id"] = urban_object["object_geometry"]["object_geometry_id"]
    new_service["service_type_id"] = service_type_id_param or service_type["service_type_id"]
    new_service["territory_type_id"] = territory_type_id_param or territory_type["territory_type_id"]

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.post("/services", json=new_service)

    # Assert
    assert_response(response, expected_status, Service, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, service_id_param",
    [
        (200, None, None),
        (404, "not found", 1e9),
    ],
    ids=["success", "not_found"],
)
async def test_put_service(
    urban_api_host: str,
    service_put_req: ServicePut,
    service: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    service_id_param: int | None,
):
    """Test PUT /services method."""

    # Arrange
    new_service = service_put_req.model_dump()
    new_service["service_type_id"] = service["service_type"]["service_type_id"]
    new_service["territory_type_id"] = service["territory_type"]["territory_type_id"]
    service_id = service_id_param or service["service_id"]

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.put(f"/services/{service_id}", json=new_service)

    # Assert
    assert_response(response, expected_status, Service, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, service_id_param",
    [
        (200, None, None),
        (404, "not found", 1e9),
    ],
    ids=["success", "not_found"],
)
async def test_patch_service(
    urban_api_host: str,
    service_put_req: ServicePut,
    service: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    service_id_param: int | None,
):
    """Test PATCH /services method."""

    # Arrange
    new_service = service_put_req.model_dump()
    new_service["service_type_id"] = service["service_type"]["service_type_id"]
    new_service["territory_type_id"] = service["territory_type"]["territory_type_id"]
    service_id = service_id_param or service["service_id"]

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.patch(f"/services/{service_id}", json=new_service)

    # Assert
    assert_response(response, expected_status, Service, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, service_id_param",
    [
        (200, None, None),
        (404, "not found", 1e9),
    ],
    ids=["success", "not_found"],
)
async def test_delete_service(
    urban_api_host: str,
    physical_object_with_geometry_post_req: PhysicalObjectWithGeometryPost,
    service_post_req: ServicePost,
    physical_object_type: dict[str, Any],
    city: dict[str, Any],
    service_type: dict[str, Any],
    territory_type: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    service_id_param: int | None,
):
    """Test DELETE /services method."""

    # Arrange
    new_object = physical_object_with_geometry_post_req.model_dump()
    new_object["physical_object_type_id"] = physical_object_type["physical_object_type_id"]
    new_object["territory_id"] = city["territory_id"]
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.post("/physical_objects", json=new_object)
    physical_object_id = response.json()["physical_object"]["physical_object_id"]
    object_geometry_id = response.json()["object_geometry"]["object_geometry_id"]
    new_service = service_post_req.model_dump()
    new_service["physical_object_id"] = physical_object_id
    new_service["object_geometry_id"] = object_geometry_id
    new_service["service_type_id"] = service_type["service_type_id"]
    new_service["territory_type_id"] = territory_type["territory_type_id"]

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        if service_id_param is None:
            response = await client.post("/services", json=new_service)
            service_id = response.json()["service_id"]
            response = await client.delete(f"/services/{service_id}")
        else:
            response = await client.delete(f"/services/{service_id_param}")

    # Assert
    assert_response(response, expected_status, OkResponse, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, service_id_param",
    [
        (201, None, None),
        (404, "not found", 1e9),
        (409, "already exists", None),
    ],
    ids=["success", "not_found", "conflict"],
)
async def test_add_service_to_objects(
    urban_api_host: str,
    physical_object_with_geometry_post_req: PhysicalObjectWithGeometryPost,
    urban_object: dict[str, Any],
    physical_object_type: dict[str, Any],
    city: dict[str, Any],
    service: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    service_id_param: int | None,
):
    """Test POST /services/{service_id} method."""

    # Arrange
    new_object = physical_object_with_geometry_post_req.model_dump()
    new_object["physical_object_type_id"] = physical_object_type["physical_object_type_id"]
    new_object["territory_id"] = city["territory_id"]
    service_id = service_id_param or service["service_id"]

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        if expected_status != 409:
            response = await client.post("/physical_objects", json=new_object)
            params = {
                "physical_object_id": response.json()["physical_object"]["physical_object_id"],
                "object_geometry_id": response.json()["object_geometry"]["object_geometry_id"],
            }
            response = await client.post(f"/services/{service_id}", params=params)
        else:
            params = {
                "physical_object_id": urban_object["physical_object"]["physical_object_id"],
                "object_geometry_id": urban_object["object_geometry"]["object_geometry_id"],
            }
            response = await client.post(f"/services/{service_id}", params=params)

    # Assert
    assert_response(response, expected_status, UrbanObject, error_message)
