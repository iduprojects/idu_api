"""Integration tests for territory-related indicators are defined here."""

from typing import Any

import httpx
import pytest
from pydantic import ValidationError

from idu_api.urban_api.schemas import Normative, NormativeDelete, NormativePost, OkResponse, TerritoryWithNormatives
from idu_api.urban_api.schemas.geometries import GeoJSONResponse
from idu_api.urban_api.schemas.short_models import ShortNormativeInfo
from tests.urban_api.helpers.utils import assert_response

####################################################################################
#                           Default use-case tests                                 #
####################################################################################


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, territory_id_param, version",
    [
        (200, None, None, None),
        (400, "You can use cities_only parameter only with including child territories", None, 1),
        (400, "Please, choose either specific year or last_only", None, 2),
        (404, "not found", 1e9, None),
    ],
    ids=["success", "bad_request_1", "bad_request_2", "not_found"],
)
async def test_get_territory_normatives(
    urban_api_host: str,
    normative: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    territory_id_param: int | None,
    version: int | None,
):
    """Test GET /territory/{territory_id}/normatives method."""

    # Arrange
    territory_id = territory_id_param or normative["territory"]["id"]
    params = {
        "last_only": version == 2,
        "include_child_territories": expected_status != 400 and version == 1,
        "cities_only": expected_status == 400 and version == 1,
    }
    if version == 2:
        params["year"] = normative["year"]

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get(f"/territory/{territory_id}/normatives", params=params)
        result = response.json()

    # Assert
    if response.status_code == 200:
        assert_response(response, expected_status, Normative, error_message, result_type="list")
        assert len(result) > 0, "Response should contain at least one normative."
    else:
        assert_response(response, expected_status, Normative, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, territory_id_param",
    [
        (201, None, None),
        (404, "not found", 1e9),
        (409, "already exists", None),
    ],
    ids=["success", "not_found", "conflict"],
)
async def test_post_territory_normatives(
    urban_api_host: str,
    normative_post_req: NormativePost,
    normative: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    territory_id_param: int | None,
):
    """Test POST /territory/{territory_id}/normatives method."""

    # Arrange
    territory_id = territory_id_param or normative["territory"]["id"]
    new_normative = normative_post_req.model_dump()
    new_normative["service_type_id"] = normative["service_type"]["id"]
    new_normative["year"] = normative["year"] if expected_status == 400 else normative["year"] - 1

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.post(f"/territory/{territory_id}/normatives", json=[new_normative])

    # Assert
    if response.status_code == 201:
        assert_response(response, expected_status, Normative, error_message, result_type="list")
    else:
        assert_response(response, expected_status, Normative, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, territory_id_param",
    [
        (200, None, None),
        (404, "not found", 1e9),
    ],
    ids=["success", "not_found"],
)
async def test_put_territory_normatives(
    urban_api_host: str,
    normative_post_req: NormativePost,
    normative: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    territory_id_param: int | None,
):
    """Test PUT /territory/{territory_id}/normatives method."""

    # Arrange
    territory_id = territory_id_param or normative["territory"]["id"]
    new_normative = normative_post_req.model_dump()
    new_normative["service_type_id"] = normative["service_type"]["id"]
    new_normative["year"] = normative["year"]

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.put(f"/territory/{territory_id}/normatives", json=[new_normative])

    # Assert
    if response.status_code == 200:
        assert_response(response, expected_status, Normative, error_message, result_type="list")
    else:
        assert_response(response, expected_status, Normative, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, territory_id_param",
    [
        (200, None, None),
        (404, "not found", 1e9),
    ],
    ids=["success", "not_found"],
)
async def test_patch_territory_normatives(
    urban_api_host: str,
    normative_post_req: NormativePost,
    normative: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    territory_id_param: int | None,
):
    """Test PATCH /territory/{territory_id}/normatives method."""

    # Arrange
    territory_id = territory_id_param or normative["territory"]["id"]
    new_normative = normative_post_req.model_dump()
    new_normative["service_type_id"] = normative["service_type"]["id"]
    new_normative["year"] = normative["year"]

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.patch(f"/territory/{territory_id}/normatives", json=[new_normative])

    # Assert
    if response.status_code == 200:
        assert_response(response, expected_status, Normative, error_message, result_type="list")
    else:
        assert_response(response, expected_status, Normative, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, territory_id_param",
    [
        (200, None, None),
        (404, "not found", 1e9),
    ],
    ids=["success", "not_found"],
)
async def test_delete_territory_normatives(
    urban_api_host: str,
    normative_delete_req: NormativeDelete,
    normative: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    territory_id_param: int | None,
):
    """Test DELETE /territory/{territory_id}/normatives method."""

    # Arrange
    territory_id = territory_id_param or normative["territory"]["id"]
    new_normative = normative_delete_req.model_dump()
    new_normative["service_type_id"] = normative["service_type"]["id"]
    new_normative["year"] = normative["year"] - 1

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.request("DELETE", f"/territory/{territory_id}/normatives", json=[new_normative])

    # Assert
    assert_response(response, expected_status, OkResponse, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, territory_id_param",
    [
        (200, None, None),
        (400, "Please, choose either specific year or last_only", None),
        (404, "not found", 1e9),
    ],
    ids=["success", "bad_request", "not_found"],
)
async def test_get_normatives_values_by_parent_id(
    urban_api_host: str,
    normative: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    territory_id_param: int | None,
):
    """Test GET /territory/normatives_values method."""

    # Arrange
    params = {"last_only": True}
    if territory_id_param is not None:
        params["parent_id"] = territory_id_param
    if expected_status == 400:
        params["year"] = normative["year"]

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get("/territory/normatives_values", params=params)
        result = response.json()

    # Assert
    assert_response(response, expected_status, GeoJSONResponse, error_message)
    if response.status_code == 200:
        assert len(result["features"]) > 0, "Response should contain at least one territory."
        try:
            TerritoryWithNormatives(**result["features"][0]["properties"])
        except ValidationError as e:
            pytest.fail(f"Pydantic validation error: {str(e)}")
        assert (
            len(result["features"][0]["properties"]["normatives"]) > 0
        ), "Response should contain at least one indicator value."
        try:
            ShortNormativeInfo(**result["features"][0]["properties"]["normatives"][0])
        except ValidationError as e:
            pytest.fail(f"Pydantic validation error: {str(e)}")
