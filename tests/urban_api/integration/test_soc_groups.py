"""Integration tests for social groups and values are defined here."""

from typing import Any

import httpx
import pytest

from idu_api.urban_api.schemas import (
    OkResponse,
    SocGroup,
    SocGroupIndicatorValue,
    SocGroupIndicatorValuePost,
    SocGroupPost,
    SocGroupWithServiceTypes,
    SocValue,
    SocValuePost,
    SocValueWithSocGroups,
    ServiceType
)
from tests.urban_api.helpers.utils import assert_response

####################################################################################
#                           Default use-case tests                                 #
####################################################################################


@pytest.mark.asyncio
async def test_get_social_groups(urban_api_host: str, social_group: dict[str, Any]):
    """Test GET /social_groups method."""

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get("/social_groups")

    # Assert
    assert_response(response, 200, SocGroup, result_type="list")
    for res in response.json():
        for k, v in social_group.items():
            if k in res:
                assert res[k] == v, f"Mismatch for {k}: {res[k]} != {v}."


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, soc_group_id_param",
    [
        (200, None, None),
        (404, "not found", 1e9),
    ],
    ids=["success", "not_found"],
)
async def test_get_social_group_by_id(
    urban_api_host: str,
    social_group: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    soc_group_id_param: int | None,
):
    """Test GET /social_groups/{soc_group_id} method."""

    # Arrange
    soc_group_id = soc_group_id_param or social_group["soc_group_id"]

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get(f"/social_groups/{soc_group_id}")
        result = response.json()

    # Assert
    assert_response(response, expected_status, SocGroupWithServiceTypes, error_message)
    if expected_status == 200:
        for k, v in social_group.items():
            if k in result and k != "service_types":
                assert result[k] == v, f"Mismatch for {k}: {result[k]} != {v}."


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message",
    [
        (201, None),
        (409, "already exists"),
    ],
    ids=["success", "conflict"],
)
async def test_add_social_group(
    urban_api_host: str,
    soc_group_post_req: SocGroupPost,
    expected_status: int,
    error_message: str | None,
):
    """Test POST /social_groups method."""

    # Arrange
    new_group = soc_group_post_req.model_dump()
    new_group["name"] = "new_name"

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.post("/social_groups", json=new_group)

    # Assert
    assert_response(response, expected_status, SocGroupWithServiceTypes, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, soc_group_id_param",
    [
        (201, None, None),
        (404, "not found", 1e9),
        (409, "already exists", None),
    ],
    ids=["success", "not_found", "conflict"],
)
async def test_add_service_type_to_social_group(
    urban_api_host: str,
    social_group: dict[str, Any],
    service_type: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    soc_group_id_param: int | None,
):
    """Test POST /social_groups/{soc_group_id}/service_types method."""

    # Arrange
    soc_group_id = soc_group_id_param or social_group["soc_group_id"]
    json_data = {"service_type_id": service_type["service_type_id"], "infrastructure_type": "basic"}

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.post(f"/social_groups/{soc_group_id}/service_types", json=json_data)

    # Assert
    assert_response(response, expected_status, SocGroupWithServiceTypes, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, soc_group_id_param",
    [
        (200, None, None),
        (404, "not found", 1e9),
    ],
    ids=["success", "not_found"],
)
async def test_delete_social_group(
    urban_api_host: str,
    soc_group_post_req: SocGroupPost,
    expected_status: int,
    error_message: str | None,
    soc_group_id_param: int | None,
):
    """Test DELETE /social_groups/{soc_group_id} method."""

    # Arrange
    new_group = soc_group_post_req.model_dump()
    new_group["name"] = "soc group for deletion"

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        if soc_group_id_param is None:
            response = await client.post("/social_groups", json=new_group)
            soc_group_id = response.json()["soc_group_id"]
            response = await client.delete(f"/social_groups/{soc_group_id}")
        else:
            response = await client.delete(f"/social_groups/{soc_group_id_param}")

    # Assert
    assert_response(response, expected_status, OkResponse, error_message)


@pytest.mark.asyncio
async def test_get_social_values(urban_api_host: str, social_value: dict[str, Any]):
    """Test GET /social_values method."""

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get("/social_values")

    # Assert
    assert_response(response, 200, SocValue, result_type="list")
    for res in response.json():
        for k, v in social_value.items():
            if k in res:
                assert res[k] == v, f"Mismatch for {k}: {res[k]} != {v}."


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, soc_value_id_param",
    [
        (200, None, None),
        (404, "not found", 1e9),
    ],
    ids=["success", "not_found"],
)
async def test_get_social_value_by_id(
    urban_api_host: str,
    social_value: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    soc_value_id_param: int | None,
):
    """Test GET /social_values/{soc_value_id} method."""

    # Arrange
    soc_value_id = soc_value_id_param or social_value["soc_value_id"]

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get(f"/social_values/{soc_value_id}")
        result = response.json()

    # Assert
    assert_response(response, expected_status, SocValueWithSocGroups, error_message)
    if expected_status == 200:
        for k, v in social_value.items():
            if k in result and k != "soc_groups":
                assert result[k] == v, f"Mismatch for {k}: {result[k]} != {v}."


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message",
    [
        (201, None),
        (409, "already exists"),
    ],
    ids=["success", "conflict"],
)
async def test_add_social_value(
    urban_api_host: str,
    soc_value_post_req: SocValuePost,
    expected_status: int,
    error_message: str | None,
):
    """Test POST /social_values method."""

    # Arrange
    new_value = soc_value_post_req.model_dump()
    new_value["name"] = "new_name"

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.post("/social_values", json=new_value)

    # Assert
    assert_response(response, expected_status, SocValueWithSocGroups, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, soc_group_id_param",
    [
        (201, None, None),
        (404, "not found", 1e9),
        (409, "already exists", None),
    ],
    ids=["success", "not_found", "conflict"],
)
async def test_add_value_to_social_group(
    urban_api_host: str,
    social_value: dict[str, Any],
    social_group: dict[str, Any],
    service_type: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    soc_group_id_param: int | None,
):
    """Test POST /social_groups/{soc_group_id}/values method."""

    # Arrange
    soc_group_id = soc_group_id_param or social_group["soc_group_id"]
    params = {"service_type_id": service_type["service_type_id"], "soc_value_id": social_value["soc_value_id"]}

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.post(f"/social_groups/{soc_group_id}/values", params=params)

    # Assert
    assert_response(response, expected_status, SocValueWithSocGroups, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, soc_value_id_param",
    [
        (200, None, None),
        (404, "not found", 1e9),
    ],
    ids=["success", "not_found"],
)
async def test_delete_social_value(
    urban_api_host: str,
    soc_value_post_req: SocValuePost,
    expected_status: int,
    error_message: str | None,
    soc_value_id_param: int | None,
):
    """Test DELETE /social_values/{soc_value_id} method."""

    # Arrange
    new_value = soc_value_post_req.model_dump()
    new_value["name"] = "soc value for deletion"

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        if soc_value_id_param is None:
            response = await client.post("/social_values", json=new_value)
            soc_value_id = response.json()["soc_value_id"]
            response = await client.delete(f"/social_values/{soc_value_id}")
        else:
            response = await client.delete(f"/social_values/{soc_value_id_param}")

    # Assert
    assert_response(response, expected_status, OkResponse, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, soc_group_id_param",
    [
        (200, None, None),
        (404, "not found", 1e9),
        (422, "please, choose either specific year or last_only", 1e9),
    ],
    ids=["success", "not_found", "unprocessable_entity"],
)
async def test_get_social_group_indicator_values(
    urban_api_host: str,
    social_group_indicator: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    soc_group_id_param: int | None,
):
    """Test GET /social_groups/{soc_group_id}/indicators method."""

    # Arrange
    soc_group_id = soc_group_id_param or social_group_indicator["soc_group"]["id"]
    params = {
        "soc_value_id": social_group_indicator["soc_value"]["id"],
        "territory_id": social_group_indicator["territory"]["id"],
        "year": social_group_indicator["year"],
        "last_only": expected_status == 422,
    }

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get(f"/social_groups/{soc_group_id}/indicators", params=params)
        result = response.json()

    # Assert
    if expected_status == 200:
        assert_response(response, expected_status, SocGroupIndicatorValue, error_message, result_type="list")
        assert len(result) > 0, "At least one indicator value was returned."
    else:
        assert_response(response, expected_status, SocGroupIndicatorValue, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, soc_group_id_param",
    [
        (201, None, None),
        (404, "not found", 1e9),
        (409, "already exists", None),
    ],
    ids=["success", "not_found", "conflict"],
)
async def test_add_social_group_indicator_value(
    urban_api_host: str,
    soc_group_indicator_post_req: SocGroupIndicatorValuePost,
    social_group_indicator: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    soc_group_id_param: int | None,
):
    """Test POST /social_groups/{soc_group_id}/indicators method."""

    # Arrange
    soc_group_id = soc_group_id_param or social_group_indicator["soc_group"]["id"]
    json_data = soc_group_indicator_post_req.model_dump()
    json_data["soc_value_id"] = social_group_indicator["soc_value"]["id"]
    json_data["territory_id"] = social_group_indicator["territory"]["id"]
    json_data["year"] = social_group_indicator["year"]

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.post(f"/social_groups/{soc_group_id}/indicators", json=json_data)

    # Assert
    assert_response(response, expected_status, SocGroupIndicatorValue, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, soc_group_id_param",
    [
        (200, None, None),
        (404, "not found", 1e9),
    ],
    ids=["success", "not_found"],
)
async def test_put_social_group_indicator_value(
    urban_api_host: str,
    soc_group_indicator_post_req: SocGroupIndicatorValuePost,
    social_group_indicator: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    soc_group_id_param: int | None,
):
    """Test PUT /social_groups/{soc_group_id}/indicators method."""

    # Arrange
    soc_group_id = soc_group_id_param or social_group_indicator["soc_group"]["id"]
    json_data = soc_group_indicator_post_req.model_dump()
    json_data["soc_value_id"] = social_group_indicator["soc_value"]["id"]
    json_data["territory_id"] = social_group_indicator["territory"]["id"]
    json_data["year"] = social_group_indicator["year"]

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.put(f"/social_groups/{soc_group_id}/indicators", json=json_data)

    # Assert
    assert_response(response, expected_status, SocGroupIndicatorValue, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, soc_group_id_param",
    [
        (200, None, None),
        (404, "not found", 1e9),
    ],
    ids=["success", "not_found"],
)
async def test_delete_social_group_indicator_values(
    urban_api_host: str,
    social_group_indicator: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    soc_group_id_param: int | None,
):
    """Test DELETE /social_groups/{soc_group_id}/indicators method."""

    # Arrange
    soc_group_id = soc_group_id_param or social_group_indicator["soc_group"]["id"]
    params = {
        "soc_value_id": social_group_indicator["soc_value"]["id"],
        "territory_id": social_group_indicator["territory"]["id"],
        "year": social_group_indicator["year"],
    }

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.delete(f"/social_groups/{soc_group_id}/indicators", params=params)

    # Assert
    assert_response(response, expected_status, OkResponse, error_message)

@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message",
    [
        (200, None),
        (404, "not found"),
    ],
    ids=["success", "not_found"],
)
async def test_get_service_types_by_social_value_id(
        urban_api_host: str,
        expected_status: int,
        error_message: str | None,
        social_value_id: int,
):
    """Test GET /social_values/{social_value}/service_types"""

    # Arrange
    params = {
        "ordering": "desc"
    }
    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get(f"/social_values/{social_value_id}/service_types", params=params)
        result = response.json()

    # Assert
    if expected_status == 200:
        assert_response(response, expected_status, ServiceType, error_message, result_type="list")
        assert len(result) > 0, "At least one service type was returned."
    else:
        assert_response(response, expected_status, ServiceType, error_message)

