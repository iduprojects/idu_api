"""Integration tests for indicators objects are defined here."""

from typing import Any

import httpx
import pytest

from idu_api.urban_api.schemas import (
    ObjectGeometry,
    ObjectGeometryPost,
    ObjectGeometryPut,
    OkResponse,
    PhysicalObject,
    UrbanObject,
)
from tests.urban_api.helpers.utils import assert_response

####################################################################################
#                           Default use-case tests                                 #
####################################################################################


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, ids_param",
    [
        (200, None, None),
        (400, None, "1.2.3"),
        (404, "not found", str(10**9)),
    ],
    ids=["success", "bad_request", "not_found"],
)
async def test_get_object_geometries_by_ids(
    urban_api_host: str,
    object_geometry: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    ids_param: str | None,
):
    """Test GET /object_geometries method."""

    # Arrange
    object_geometries_ids = ids_param or str(object_geometry["object_geometry_id"])

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get("/object_geometries", params={"object_geometries_ids": object_geometries_ids})

    # Assert
    if response.status_code == 200:
        assert_response(response, expected_status, ObjectGeometry, error_message, result_type="list")
        for res in response.json():
            for k, v in object_geometry.items():
                if k in res:
                    assert res[k] == v, f"Mismatch for {k}: {res[k]} != {v}."
    else:
        assert_response(response, expected_status, ObjectGeometry, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, physical_object_id_param",
    [
        (201, None, None),
        (404, "not found", 1e9),
    ],
    ids=["success", "not_found"],
)
async def test_add_object_geometry_to_physical_object(
    urban_api_host: str,
    object_geometries_post_req: ObjectGeometryPost,
    object_geometry: dict[str, Any],
    physical_object: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    physical_object_id_param: int | None,
):
    """Test POST /object_geometries/{physical_object_id} method."""

    # Arrange
    physical_object_id = physical_object_id_param or physical_object["physical_object_id"]
    json_data = object_geometries_post_req.model_dump()
    json_data["territory_id"] = object_geometry["territory"]["id"]

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.post(f"/object_geometries/{physical_object_id}", json=json_data)

    # Assert
    assert_response(response, expected_status, UrbanObject, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, object_geometry_id_param",
    [
        (200, None, None),
        (404, "not found", 1e9),
    ],
    ids=["success", "not_found"],
)
async def test_put_object_geometry(
    urban_api_host: str,
    object_geometries_put_req: ObjectGeometryPut,
    object_geometry: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    object_geometry_id_param: int | None,
):
    """Test PUT /object_geometries method."""

    # Arrange
    object_geometry_id = object_geometry_id_param or object_geometry["object_geometry_id"]
    json_data = object_geometries_put_req.model_dump()
    json_data["territory_id"] = object_geometry["territory"]["id"]

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.put(f"/object_geometries/{object_geometry_id}", json=json_data)

    # Assert
    assert_response(response, expected_status, ObjectGeometry, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, object_geometry_id_param",
    [
        (200, None, None),
        (404, "not found", 1e9),
    ],
    ids=["success", "not_found"],
)
async def test_patch_object_geometry(
    urban_api_host: str,
    object_geometries_put_req: ObjectGeometryPut,
    object_geometry: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    object_geometry_id_param: int | None,
):
    """Test PATCH /object_geometries method."""

    # Arrange
    object_geometry_id = object_geometry_id_param or object_geometry["object_geometry_id"]
    json_data = object_geometries_put_req.model_dump()
    json_data["territory_id"] = object_geometry["territory"]["id"]

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.patch(f"/object_geometries/{object_geometry_id}", json=json_data)

    # Assert
    assert_response(response, expected_status, ObjectGeometry, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, object_geometry_id_param",
    [
        (200, None, None),
        (404, "not found", 1e9),
    ],
    ids=["success", "not_found"],
)
async def test_delete_object_geometry(
    urban_api_host: str,
    object_geometries_post_req: ObjectGeometryPost,
    object_geometry: dict[str, Any],
    physical_object: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    object_geometry_id_param: int | None,
):
    """Test DELETE /object_geometries method."""

    # Arrange
    object_geometry_id = object_geometry_id_param or object_geometry["object_geometry_id"]
    physical_object_id = physical_object["physical_object_id"]
    json_data = object_geometries_post_req.model_dump()
    json_data["territory_id"] = object_geometry["territory"]["id"]

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        if object_geometry_id_param is None:
            response = await client.post(f"/object_geometries/{physical_object_id}", json=json_data)
            object_geometry_id = response.json()["object_geometry"]["object_geometry_id"]
            response = await client.delete(f"/object_geometries/{object_geometry_id}")
        else:
            response = await client.delete(f"/object_geometries/{object_geometry_id}")

    # Assert
    assert_response(response, expected_status, OkResponse, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, object_geometry_id_param",
    [
        (200, None, None),
        (404, "not found", 1e9),
    ],
    ids=["success", "not_found"],
)
async def test_get_physical_objects_by_geometry_id(
    urban_api_host: str,
    object_geometry: dict[str, Any],
    physical_object: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    object_geometry_id_param: int | None,
):
    """Test GET /object_geometries/{object_geometry_id}/physical_objects method."""

    # Arrange
    object_geometry_id = object_geometry_id_param or object_geometry["object_geometry_id"]

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get(f"/object_geometries/{object_geometry_id}/physical_objects")

    # Assert
    if response.status_code == 200:
        assert_response(response, expected_status, PhysicalObject, error_message, result_type="list")
        for res in response.json():
            for k, v in physical_object.items():
                if k in res and k != "territories":
                    assert res[k] == v, f"Mismatch for {k}: {res[k]} != {v}."
    else:
        assert_response(response, expected_status, PhysicalObject, error_message)
