"""Integration tests for buffer objects are defined here."""

from typing import Any

import httpx
import pytest

from idu_api.urban_api.schemas import (
    Buffer,
    BufferPut,
    BufferType,
    BufferTypePost,
    DefaultBufferValue,
    DefaultBufferValuePost,
    DefaultBufferValuePut,
    OkResponse,
    PhysicalObjectWithGeometryPost,
)
from idu_api.urban_api.schemas.geometries import Geometry
from tests.urban_api.helpers.utils import assert_response

####################################################################################
#                           Default use-case tests                                 #
####################################################################################


@pytest.mark.asyncio
async def test_get_buffer_types(urban_api_host: str, buffer_type: dict[str, Any]):
    """Test GET /buffer_types method."""

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get("/buffer_types")

    # Assert
    assert_response(response, 200, BufferType, result_type="list")
    for res in response.json():
        for k, v in buffer_type.items():
            if k in res:
                assert res[k] == v, f"Mismatch for {k}: {res[k]} != {v}."


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message",
    [
        (201, None),
        (409, "already exists"),
    ],
    ids=["success", "conflict"],
)
async def test_add_buffer_type(
    urban_api_host: str,
    buffer_type_post_req: BufferTypePost,
    expected_status: int,
    error_message: str | None,
):
    """Test POST /buffer_types method."""

    # Arrange
    new_zone_type = buffer_type_post_req.model_dump()
    new_zone_type["name"] = "new name"

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.post("/buffer_types", json=new_zone_type)

    # Assert
    assert_response(response, expected_status, BufferType, error_message)


@pytest.mark.asyncio
async def test_get_all_default_buffer_values(urban_api_host: str, default_buffer_value: dict[str, Any]):
    """Test GET /buffer_types/defaults method."""

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get("/buffer_types/defaults")

    # Assert
    assert_response(response, 200, DefaultBufferValue, result_type="list")
    for res in response.json():
        for k, v in default_buffer_value.items():
            if k in res:
                assert res[k] == v, f"Mismatch for {k}: {res[k]} != {v}."


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, type_id_param",
    [
        (201, None, None),
        (404, "not found", 1e9),
        (409, "already exists", None),
    ],
    ids=["success", "not_found", "conflict"],
)
async def test_add_default_buffer_values(
    urban_api_host,
    default_buffer_value_post_req: DefaultBufferValuePost,
    buffer_type: dict[str, Any],
    service_type: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    type_id_param: int | None,
):
    """Test POST /buffer_types/defaults method."""

    # Arrange
    new_default_value = default_buffer_value_post_req.model_dump()
    new_default_value["buffer_type_id"] = type_id_param or buffer_type["buffer_type_id"]
    new_default_value["physical_object_type_id"] = None if expected_status != 422 else 1
    new_default_value["service_type_id"] = type_id_param or service_type["service_type_id"]

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.post("/buffer_types/defaults", json=new_default_value)

    # Assert
    assert_response(response, expected_status, DefaultBufferValue, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, type_id_param",
    [
        (200, None, None),
        (404, "not found", 1e9),
    ],
    ids=["success", "not_found"],
)
async def test_put_default_buffer_values(
    urban_api_host,
    default_buffer_value_put_req: DefaultBufferValuePut,
    buffer_type: dict[str, Any],
    service_type: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    type_id_param: int | None,
):
    """Test PUT /profiles_reclamation method."""

    # Arrange
    new_default_value = default_buffer_value_put_req.model_dump()
    new_default_value["buffer_type_id"] = type_id_param or buffer_type["buffer_type_id"]
    new_default_value["physical_object_type_id"] = None if expected_status != 422 else 1
    new_default_value["service_type_id"] = type_id_param or service_type["service_type_id"]

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.put("/buffer_types/defaults", json=new_default_value)

    # Assert
    assert_response(response, expected_status, DefaultBufferValue, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, geom_param, type_id_param",
    [
        (200, None, Geometry(type="Point", coordinates=[30.22, 59.86]), None),
        (200, None, None, None),
        (404, "not found", None, 1e9),
    ],
    ids=["custom_success", "default_success", "not_found"],
)
async def test_put_buffer(
    urban_api_host,
    buffer_put_req: BufferPut,
    buffer_type: dict[str, Any],
    urban_object: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    geom_param: Geometry | None,
    type_id_param: int | None,
):
    """Test PUT /buffers method."""

    # Arrange
    new_buffer = buffer_put_req.model_dump()
    new_buffer["buffer_type_id"] = type_id_param or buffer_type["buffer_type_id"]
    new_buffer["urban_object_id"] = urban_object["urban_object_id"]
    new_buffer["geometry"] = geom_param.model_dump() if geom_param else None

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.put("/buffers", json=new_buffer)

    # Assert
    assert_response(response, expected_status, Buffer, error_message)
    if expected_status == 201:
        assert (
            response.json()["is_custom"] == geom_param is not None
        ), "The buffer must be custom if the geometry is passed, and vice versa."


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, buffer_id_param",
    [
        (200, None, None),
        (404, "not found", 1e9),
    ],
    ids=["success", "not_found"],
)
async def test_delete_buffer(
    urban_api_host: str,
    buffer_put_req: BufferPut,
    physical_object_with_geometry_post_req: PhysicalObjectWithGeometryPost,
    buffer_type: dict[str, Any],
    physical_object_type: dict[str, Any],
    city: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    buffer_id_param: int | None,
):
    """Test DELETE /buffers method."""

    # Arrange
    new_object = physical_object_with_geometry_post_req.model_dump()
    new_object["physical_object_type_id"] = physical_object_type["physical_object_type_id"]
    new_object["territory_id"] = city["territory_id"]
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.post("/physical_objects", json=new_object)
    new_buffer = buffer_put_req.model_dump()
    new_buffer["buffer_type_id"] = buffer_type["buffer_type_id"]
    new_buffer["urban_object_id"] = response.json()["urban_object_id"]

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        if buffer_id_param is None:
            response = await client.put("/buffers", json=new_buffer)
            buffer_type_id, urban_object_id = (
                response.json()["buffer_type"]["id"],
                response.json()["urban_object"]["id"],
            )
            params = {"buffer_type_id": buffer_type_id, "urban_object_id": urban_object_id}
            response = await client.delete("/buffers", params=params)
        else:
            params = {"buffer_type_id": buffer_id_param, "urban_object_id": buffer_id_param}
            response = await client.delete("/buffers", params=params)

    # Assert
    assert_response(response, expected_status, OkResponse, error_message)
