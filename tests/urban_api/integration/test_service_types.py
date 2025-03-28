"""Integration tests for services types are defined here."""

from typing import Any

import httpx
import pytest

from idu_api.urban_api.schemas import (
    OkResponse,
    PhysicalObjectType,
    ServiceType,
    ServiceTypePost,
    ServiceTypePut,
    ServiceTypesHierarchy,
    UrbanFunction,
    UrbanFunctionPost,
    UrbanFunctionPut, SocGroupWithServiceTypes,
)
from tests.urban_api.helpers.utils import assert_response

####################################################################################
#                           Default use-case tests                                 #
####################################################################################


@pytest.mark.asyncio
async def test_get_service_types(urban_api_host: str, service_type: dict[str, Any], urban_function: dict[str, Any]):
    """Test GET /service_types method."""

    # Arrange
    params = {
        "urban_function": urban_function["urban_function_id"],
        "name": service_type["name"],
    }

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get("/service_types", params=params)

    # Assert
    assert_response(response, 200, ServiceType, result_type="list")
    for res in response.json():
        for k, v in service_type.items():
            if k in res:
                assert res[k] == v, f"Mismatch for {k}: {res[k]} != {v}."


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, function_id_param",
    [
        (201, None, None),
        (404, "not found", 1e9),
        (409, "already exists", None),
    ],
    ids=["success", "not_found", "conflict"],
)
async def test_add_service_type(
    urban_api_host: str,
    service_type_post_req: ServiceTypePost,
    urban_function: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    function_id_param: int | None,
):
    """Test POST /service_types method."""

    # Arrange
    new_type = service_type_post_req.model_dump()
    new_type["name"] = "new name"
    new_type["urban_function_id"] = function_id_param or urban_function["urban_function_id"]

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.post("/service_types", json=new_type)

    # Assert
    assert_response(response, expected_status, ServiceType, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, function_id_param",
    [
        (200, None, None),
        (404, "not found", 1e9),
    ],
    ids=["success", "not_found"],
)
async def test_put_service_type(
    urban_api_host: str,
    service_type_put_req: ServiceTypePut,
    urban_function: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    function_id_param: int | None,
):
    """Test PUT /service_types method."""

    # Arrange
    new_type = service_type_put_req.model_dump()
    new_type["name"] = "new name"
    new_type["urban_function_id"] = function_id_param or urban_function["urban_function_id"]

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.put("/service_types", json=new_type)

    # Assert
    assert_response(response, expected_status, ServiceType, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, type_name_param, type_id_param",
    [
        (200, None, "updated name", None),
        (404, "not found", "updated name", 1e9),
        (409, "already exists", "new name", None),
    ],
    ids=["success", "not_found", "conflict"],
)
async def test_patch_service_type(
    urban_api_host: str,
    service_type_put_req: ServiceTypePut,
    service_type: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    type_name_param: str,
    type_id_param: int | None,
):
    """Test PATCH /service_types method."""

    # Arrange
    new_type = service_type_put_req.model_dump()
    new_type["name"] = type_name_param
    new_type["urban_function_id"] = service_type["urban_function"]["id"]
    service_type_id = type_id_param or service_type["service_type_id"]

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.patch(f"/service_types/{service_type_id}", json=new_type)

    # Assert
    assert_response(response, expected_status, ServiceType, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, type_id_param",
    [
        (200, None, None),
        (404, "not found", 1e9),
    ],
    ids=["success", "not_found"],
)
async def test_delete_service_type(
    urban_api_host: str,
    service_type_post_req: ServiceTypePost,
    urban_function: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    type_id_param: int | None,
):
    """Test DELETE /service_types method."""

    # Arrange
    new_type = service_type_post_req.model_dump()
    new_type["name"] = "type for deletion"
    new_type["urban_function_id"] = urban_function["urban_function_id"]

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        if type_id_param is None:
            response = await client.post("/service_types", json=new_type)
            service_type_id = response.json()["service_type_id"]
            response = await client.delete(f"/service_types/{service_type_id}")
        else:
            response = await client.delete(f"/service_types/{type_id_param}")

    # Assert
    assert_response(response, expected_status, OkResponse, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, parent_id_param",
    [
        (200, None, None),
        (404, "not found", 1e9),
    ],
    ids=["success", "not_found"],
)
async def test_get_urban_functions_by_parent_id(
    urban_api_host: str,
    urban_function: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    parent_id_param: int | None,
):
    """Test GET /urban_functions_by_parent method."""

    # Arrange
    params = {"get_all_subtree": True, "name": "test"}
    if parent_id_param is not None:
        params["parent_id"] = parent_id_param

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get("/urban_functions_by_parent", params=params)

    # Assert
    if response.status_code == 200:
        assert_response(response, expected_status, UrbanFunction, result_type="list")
        for res in response.json():
            for k, v in urban_function.items():
                if k in res:
                    assert res[k] == v, f"Mismatch for {k}: {res[k]} != {v}."
    else:
        assert_response(response, expected_status, UrbanFunction, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, parent_id_param",
    [
        (201, None, None),
        (404, "not found", 1e9),
        (409, "already exists", None),
    ],
    ids=["success", "not_found", "conflict"],
)
async def test_add_urban_function(
    urban_api_host: str,
    urban_function_post_req: UrbanFunctionPost,
    expected_status: int,
    error_message: str | None,
    parent_id_param: int | None,
):
    """Test POST /urban_functions method."""

    # Arrange
    new_function = urban_function_post_req.model_dump()
    new_function["name"] = "new name"
    new_function["parent_id"] = parent_id_param

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.post("/urban_functions", json=new_function)

    # Assert
    assert_response(response, expected_status, UrbanFunction, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, parent_id_param",
    [
        (200, None, None),
        (404, "not found", 1e9),
    ],
    ids=["success", "not_found"],
)
async def test_put_urban_function(
    urban_api_host: str,
    urban_function_put_req: UrbanFunctionPut,
    expected_status: int,
    error_message: str | None,
    parent_id_param: int | None,
):
    """Test POST /urban_functions method."""

    # Arrange
    new_function = urban_function_put_req.model_dump()
    new_function["name"] = "new name"
    new_function["parent_id"] = parent_id_param

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.put("/urban_functions", json=new_function)

    # Assert
    assert_response(response, expected_status, UrbanFunction, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, function_name_param, function_id_param",
    [
        (200, None, "updated name", None),
        (404, "not found", "updated name", 1e9),
        (409, "already exists", "new name", None),
    ],
    ids=["success", "not_found", "conflict"],
)
async def test_patch_urban_function(
    urban_api_host: str,
    urban_function_put_req: UrbanFunctionPut,
    urban_function: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    function_name_param: str | None,
    function_id_param: int | None,
):
    """Test PATCH /urban_functions method."""

    # Arrange
    new_function = urban_function_put_req.model_dump()
    new_function["name"] = function_name_param
    new_function["parent_id"] = None
    urban_function_id = function_id_param or urban_function["urban_function_id"]

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.patch(f"/urban_functions/{urban_function_id}", json=new_function)

    # Assert
    assert_response(response, expected_status, UrbanFunction, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, function_id_param",
    [
        (200, None, None),
        (404, "not found", 1e9),
    ],
    ids=["success", "not_found"],
)
async def test_delete_urban_function(
    urban_api_host: str,
    urban_function_post_req: UrbanFunctionPost,
    expected_status: int,
    error_message: str | None,
    function_id_param: int | None,
):
    """Test DELETE /urban_functions method."""

    # Arrange
    new_function = urban_function_post_req.model_dump()
    new_function["name"] = "function for deletion"
    new_function["parent_id"] = None

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        if function_id_param is None:
            response = await client.post("/urban_functions", json=new_function)
            urban_function_id = response.json()["urban_function_id"]
            response = await client.delete(f"/urban_functions/{urban_function_id}")
        else:
            response = await client.delete(f"/urban_functions/{function_id_param}")

    # Assert
    assert_response(response, expected_status, OkResponse, error_message)


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
async def test_get_service_types_hierarchy(
    urban_api_host: str,
    expected_status: int,
    error_message: str | None,
    ids_param: str | None,
):
    """Test GET /service_types/hierarchy method."""

    # Arrange
    params = {}
    if ids_param is not None:
        params["service_types_ids"] = ids_param

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get("/service_types/hierarchy", params=params)

    # Assert
    if expected_status == 200:
        assert_response(response, expected_status, ServiceTypesHierarchy, error_message, result_type="list")
        assert len(response.json()) > 0, "Hierarchy should contain at least one service type."
    else:
        assert_response(response, expected_status, ServiceTypesHierarchy, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, type_id_param",
    [
        (200, None, None),
        (404, "not found", 1e9),
    ],
    ids=["success", "not_found"],
)
async def test_get_physical_object_types(
    urban_api_host: str,
    service_type: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    type_id_param: str | None,
):
    """Test GET /service_types/{service_type_id}/physical_object_types method."""

    # Arrange
    service_type_id = type_id_param or service_type["service_type_id"]

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get(f"/service_types/{service_type_id}/physical_object_types")

    # Assert
    if response.status_code == 200:
        assert_response(response, expected_status, PhysicalObjectType, error_message, result_type="list")
    else:
        assert_response(response, expected_status, PhysicalObjectType, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, type_id_param",
    [
        (200, None, None),
        (404, "not found", 1e9),
    ],
    ids=["success", "not_found"],
)
async def test_get_social_groups(
    urban_api_host: str,
    service_type: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    type_id_param: str | None,
):
    """Test GET /service_types/{service_type_id}/social_groups method."""

    # Arrange
    service_type_id = type_id_param or service_type["service_type_id"]

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get(f"/service_types/{service_type_id}/social_groups")

    # Assert
    if response.status_code == 200:
        assert_response(response, expected_status, SocGroupWithServiceTypes, error_message, result_type="list")
    else:
        assert_response(response, expected_status, SocGroupWithServiceTypes, error_message)
