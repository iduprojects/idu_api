"""Integration tests for physical objects are defined here."""

from typing import Any

import httpx
import pytest

from idu_api.urban_api.schemas import (
    Building,
    BuildingPost,
    BuildingPut,
    ObjectGeometry,
    OkResponse,
    PhysicalObject,
    PhysicalObjectPost,
    PhysicalObjectPut,
    PhysicalObjectWithGeometry,
    PhysicalObjectWithGeometryPost,
    Service,
    ServiceWithGeometry,
    UrbanObject,
)
from idu_api.urban_api.schemas.geometries import AllPossibleGeometry
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
async def test_get_physical_object_by_id(
    urban_api_host: str,
    physical_object: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    object_id_param: int | None,
):
    """Test GET /physical_object/{physical_object_id} method."""

    # Arrange
    physical_object_id = object_id_param or physical_object["physical_object_id"]

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get(f"/physical_object/{physical_object_id}")
        result = response.json()

    # Assert
    assert_response(response, expected_status, PhysicalObject, error_message)
    if response.status_code == 200:
        for k, v in physical_object.items():
            if k in result and k not in ("physical_object_type", "territories"):
                assert result[k] == v, f"Mismatch for {k}: {result[k]} != {v}."


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, type_id_param, territory_id_param",
    [
        (201, None, None, None),
        (404, "not found", 1e9, 1e9),
    ],
    ids=["success", "not_found"],
)
async def test_add_physical_object_with_geometry(
    urban_api_host: str,
    physical_object_with_geometry_post_req: PhysicalObjectWithGeometryPost,
    physical_object_type: dict[str, Any],
    city: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    type_id_param: int | None,
    territory_id_param: int | None,
):
    """Test POST /physical_objects method."""

    # Arrange
    new_object = physical_object_with_geometry_post_req.model_dump()
    new_object["physical_object_type_id"] = type_id_param or physical_object_type["physical_object_type_id"]
    new_object["territory_id"] = territory_id_param or city["territory_id"]

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.post("/physical_objects", json=new_object)

    # Assert
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
async def test_put_physical_object(
    urban_api_host: str,
    physical_object_put_req: PhysicalObjectPut,
    physical_object: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    object_id_param: int | None,
):
    """Test PUT /physical_objects method."""

    # Arrange
    new_object = physical_object_put_req.model_dump()
    new_object["physical_object_type_id"] = physical_object["physical_object_type"]["physical_object_type_id"]
    physical_object_id = object_id_param or physical_object["physical_object_id"]

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.put(f"/physical_objects/{physical_object_id}", json=new_object)

    # Assert
    assert_response(response, expected_status, PhysicalObject, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, object_id_param",
    [
        (200, None, None),
        (404, "not found", 1e9),
    ],
    ids=["success", "not_found"],
)
async def test_patch_physical_object(
    urban_api_host: str,
    physical_object_put_req: PhysicalObjectPut,
    physical_object: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    object_id_param: int | None,
):
    """Test PATCH /physical_objects method."""

    # Arrange
    new_object = physical_object_put_req.model_dump()
    new_object["physical_object_type_id"] = physical_object["physical_object_type"]["physical_object_type_id"]
    physical_object_id = object_id_param or physical_object["physical_object_id"]

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.patch(f"/physical_objects/{physical_object_id}", json=new_object)

    # Assert
    assert_response(response, expected_status, PhysicalObject, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, object_id_param",
    [
        (200, None, None),
        (404, "not found", 1e9),
    ],
    ids=["success", "not_found"],
)
async def test_delete_physical_object(
    urban_api_host: str,
    physical_object_with_geometry_post_req: PhysicalObjectWithGeometryPost,
    physical_object_type: dict[str, Any],
    city: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    object_id_param: int | None,
):
    """Test DELETE /physical_objects method."""

    # Arrange
    new_object = physical_object_with_geometry_post_req.model_dump()
    new_object["physical_object_type_id"] = physical_object_type["physical_object_type_id"]
    new_object["territory_id"] = city["territory_id"]

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        if object_id_param is None:
            response = await client.post("/physical_objects", json=new_object)
            physical_object_id = response.json()["physical_object"]["physical_object_id"]
            response = await client.delete(f"/physical_objects/{physical_object_id}")
        else:
            response = await client.delete(f"/physical_objects/{object_id_param}")

    # Assert
    assert_response(response, expected_status, OkResponse, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, object_id_param",
    [
        (201, None, None),
        (404, "not found", 1e9),
        (409, "already exists", None),
    ],
    ids=["success", "not_found", "conflict"],
)
async def test_add_building(
    urban_api_host: str,
    building_post_req: BuildingPost,
    physical_object: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    object_id_param: int | None,
):
    """Test POST /buildings method."""

    # Arrange
    new_object = building_post_req.model_dump()
    new_object["physical_object_id"] = object_id_param or physical_object["physical_object_id"]

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.post("/buildings", json=new_object)
        result = response.json()

    # Assert
    assert_response(response, expected_status, PhysicalObject, error_message)
    if response.status_code == 201:
        for k, v in new_object.items():
            if k in result["building"]:
                assert result["building"][k] == v, f"Mismatch for {k}: {result[k]} != {v}."


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, object_id_param",
    [
        (200, None, None),
        (404, "not found", 1e9),
    ],
    ids=["success", "not_found"],
)
async def test_put_building(
    urban_api_host: str,
    building_put_req: BuildingPut,
    physical_object: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    object_id_param: int | None,
):
    """Test PUT /buildings method."""

    # Arrange
    new_object = building_put_req.model_dump()
    new_object["physical_object_id"] = object_id_param or physical_object["physical_object_id"]

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.put("/buildings", json=new_object)
        result = response.json()

    # Assert
    assert_response(response, expected_status, PhysicalObject, error_message)
    if response.status_code == 200:
        for k, v in new_object.items():
            if k in result["building"]:
                assert result["building"][k] == v, f"Mismatch for {k}: {result[k]} != {v}."


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, building_id_param",
    [
        (200, None, None),
        (404, "not found", 1e9),
    ],
    ids=["success", "not_found"],
)
async def test_patch_building(
    urban_api_host: str,
    building_put_req: BuildingPut,
    physical_object: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    building_id_param: int | None,
):
    """Test PATCH /buildings method."""

    # Arrange
    new_object = building_put_req.model_dump()
    new_object["physical_object_id"] = physical_object["physical_object_id"]

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        if building_id_param is None:
            response = await client.get(f"/physical_object/{new_object['physical_object_id']}")
            building_id = response.json()["building"]["id"]
            response = await client.patch(f"/buildings/{building_id}", json=new_object)
        else:
            response = await client.patch(f"/buildings/{building_id_param}", json=new_object)

    # Assert
    assert_response(response, expected_status, PhysicalObject, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, building_id_param",
    [
        (200, None, None),
        (404, "not found", 1e9),
    ],
    ids=["success", "not_found"],
)
async def test_delete_building(
    urban_api_host: str,
    physical_object: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    building_id_param: int | None,
):
    """Test DELETE /buildings method."""

    # Arrange
    physical_object_id = physical_object["physical_object_id"]

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        if building_id_param is None:
            response = await client.get(f"/physical_object/{physical_object_id}")
            building_id = response.json()["building"]["id"]
            response = await client.delete(f"/buildings/{building_id}")
        else:
            response = await client.delete(f"/buildings/{building_id_param}")

    # Assert
    assert_response(response, expected_status, OkResponse, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, object_id_param",
    [
        (200, None, None),
        (404, "not found", 1e9),
    ],
    ids=["success", "not_found"],
)
async def test_get_buildings_by_physical_object_id(
    urban_api_host: str,
    physical_object: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    object_id_param: int | None,
):
    """Test GET /physical_objects/{physical_object_id}/living_buildings method."""

    # Arrange
    physical_object_id = object_id_param or physical_object["physical_object_id"]

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get(f"/physical_objects/{physical_object_id}/living_buildings")

    # Assert
    if response.status_code == 200:
        assert_response(response, expected_status, Building, error_message, result_type="list")
    else:
        assert_response(response, expected_status, Building, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, object_id_param",
    [
        (200, None, None),
        (404, "not found", 1e9),
    ],
    ids=["success", "not_found"],
)
async def test_get_services_by_physical_object_id(
    urban_api_host: str,
    physical_object: dict[str, Any],
    service: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    object_id_param: int | None,
):
    """Test GET /physical_objects/{physical_object_id}/services method."""

    # Arrange
    physical_object_id = object_id_param or physical_object["physical_object_id"]

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get(f"/physical_objects/{physical_object_id}/services")

    # Assert
    if response.status_code == 200:
        assert_response(response, expected_status, Service, error_message, result_type="list")
        assert any(
            service["service_id"] == item["service_id"] for item in response.json()
        ), "Expected service was not found in result."
    else:
        assert_response(response, expected_status, Service, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, object_id_param",
    [
        (200, None, None),
        (404, "not found", 1e9),
    ],
    ids=["success", "not_found"],
)
async def test_get_services_with_geometry_by_physical_object_id(
    urban_api_host: str,
    physical_object: dict[str, Any],
    service: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    object_id_param: int | None,
):
    """Test GET /physical_objects/{physical_object_id}/services_with_geometry method."""

    # Arrange
    physical_object_id = object_id_param or physical_object["physical_object_id"]

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get(f"/physical_objects/{physical_object_id}/services_with_geometry")

    # Assert
    if response.status_code == 200:
        assert_response(response, expected_status, ServiceWithGeometry, error_message, result_type="list")
        assert any(
            service["service_id"] == item["service_id"] for item in response.json()
        ), "Expected service was not found in result."
    else:
        assert_response(response, expected_status, ServiceWithGeometry, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, object_id_param",
    [
        (200, None, None),
        (404, "not found", 1e9),
    ],
    ids=["success", "not_found"],
)
async def test_get_physical_object_geometries(
    urban_api_host: str,
    physical_object: dict[str, Any],
    object_geometry: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    object_id_param: int | None,
):
    """Test GET /physical_objects/{physical_object_id}/geometries method."""

    # Arrange
    physical_object_id = object_id_param or physical_object["physical_object_id"]
    object_geometry_id = object_geometry["object_geometry_id"]

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get(f"/physical_objects/{physical_object_id}/geometries")

    # Assert
    if response.status_code == 200:
        assert_response(response, expected_status, ObjectGeometry, error_message, result_type="list")
        assert any(
            object_geometry_id == item["object_geometry_id"] for item in response.json()
        ), "Expected geometry was not found in result."
    else:
        assert_response(response, expected_status, ObjectGeometry, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, geometry_param",
    [
        (200, None, AllPossibleGeometry(type="Point", coordinates=[30.22, 59.86], geometries=None)),
        (400, None, AllPossibleGeometry(type="Polygon", coordinates=[30.22, 59.86], geometries=None)),
    ],
    ids=["success", "bad_request"],
)
async def test_get_physical_objects_around_geometry(
    urban_api_host: str,
    physical_object: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    geometry_param: AllPossibleGeometry,
):
    """Test POST /physical_objects/around."""

    # Arrange
    physical_object_id = physical_object["physical_object_id"]

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.post("/physical_objects/around", json=geometry_param.model_dump())

    # Assert
    if response.status_code == 200:
        assert_response(response, expected_status, PhysicalObjectWithGeometry, error_message, result_type="list")
        assert any(
            physical_object_id == item["physical_object_id"] for item in response.json()
        ), "Expected physical object was not found in result."
    else:
        assert_response(response, expected_status, PhysicalObjectWithGeometry, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, geometry_id_param",
    [
        (201, None, None),
        (404, "not found", 1e9),
    ],
    ids=["success", "not_found"],
)
async def test_add_physical_object_to_object_geometry(
    urban_api_host: str,
    physical_object_post_req: PhysicalObjectPost,
    physical_object: dict[str, Any],
    object_geometry: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    geometry_id_param: int | None,
):
    """Test POST /physical_objects/{object_geometry_id} method."""

    # Arrange
    object_geometry_id = geometry_id_param or object_geometry["object_geometry_id"]
    json_data = physical_object_post_req.model_dump()
    json_data["physical_object_type_id"] = physical_object["physical_object_type"]["physical_object_type_id"]

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.post(f"/physical_objects/{object_geometry_id}", json=json_data)

    # Assert
    assert_response(response, expected_status, UrbanObject, error_message)
