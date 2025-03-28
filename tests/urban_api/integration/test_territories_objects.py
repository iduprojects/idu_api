"""Integration tests for territories are defined here."""

from typing import Any

import httpx
import pytest
from pydantic import ValidationError

from idu_api.urban_api.schemas import Page, Territory, TerritoryPost, TerritoryPut, TerritoryWithoutGeometry
from idu_api.urban_api.schemas.geometries import AllPossibleGeometry, GeoJSONResponse
from tests.urban_api.helpers.utils import assert_response

####################################################################################
#                           Default use-case tests                                 #
####################################################################################


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, territory_id_param",
    [
        (200, None, None),
        (404, "not found", 1e9),
    ],
    ids=["success", "not_found"],
)
async def test_get_territory_by_id(
    urban_api_host: str,
    city: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    territory_id_param: int | None,
):
    """Test GET /territory method."""

    # Arrange
    territory_id = territory_id_param or city["territory_id"]

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get(f"/territory/{territory_id}")
        result = response.json()

    # Assert
    assert_response(response, expected_status, Territory, error_message)
    if response.status_code == 200:
        for k, v in city.items():
            if k in result:
                assert result[k] == v, f"Mismatch for {k}: {result[k]} != {v}."


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, territory_type_id_param, target_city_type_id_param, admin_center_id_param",
    [
        (201, None, None, None, None),
        (404, "not found", 1e9, 1e9, 1e9),
    ],
    ids=["success", "not_found"],
)
async def test_add_territory(
    urban_api_host: str,
    territory_post_req: TerritoryPost,
    country: dict[str, Any],
    city: dict[str, Any],
    territory_type: dict[str, Any],
    target_city_type: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    territory_type_id_param: int | None,
    target_city_type_id_param: int | None,
    admin_center_id_param: int | None,
):
    """Test POST /territory method."""

    # Arrange
    new_territory = territory_post_req.model_dump()
    new_territory["parent_id"] = country["territory_id"]
    new_territory["territory_type_id"] = territory_type_id_param or territory_type["territory_type_id"]
    new_territory["target_city_type_id"] = target_city_type_id_param or target_city_type["target_city_type_id"]
    new_territory["admin_center_id"] = admin_center_id_param or city["territory_id"]

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.post("/territory", json=new_territory)

    # Assert
    assert_response(response, expected_status, Territory, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, territory_id_param",
    [
        (200, None, None),
        (404, "not found", 1e9),
    ],
    ids=["success", "not_found"],
)
async def test_put_territory(
    urban_api_host: str,
    territory_put_req: TerritoryPut,
    country: dict[str, Any],
    region: dict[str, Any],
    city: dict[str, Any],
    territory_type: dict[str, Any],
    target_city_type: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    territory_id_param: int | None,
):
    """Test PUT /territory method."""

    # Arrange
    new_territory = territory_put_req.model_dump()
    new_territory["parent_id"] = country["territory_id"]
    new_territory["territory_type_id"] = territory_type["territory_type_id"]
    new_territory["target_city_type_id"] = target_city_type["target_city_type_id"]
    new_territory["admin_center_id"] = city["territory_id"]
    territory_id = territory_id_param or region["territory_id"]

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.put(f"/territory/{territory_id}", json=new_territory)

    # Assert
    assert_response(response, expected_status, Territory, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, territory_id_param",
    [
        (200, None, None),
        (404, "not found", 1e9),
    ],
    ids=["success", "not_found"],
)
async def test_patch_territory(
    urban_api_host: str,
    territory_put_req: TerritoryPut,
    country: dict[str, Any],
    region: dict[str, Any],
    city: dict[str, Any],
    territory_type: dict[str, Any],
    target_city_type: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    territory_id_param: int | None,
):
    """Test PATCH /territory method."""

    # Arrange
    new_territory = territory_put_req.model_dump()
    new_territory["parent_id"] = country["territory_id"]
    new_territory["territory_type_id"] = territory_type["territory_type_id"]
    new_territory["target_city_type_id"] = target_city_type["target_city_type_id"]
    new_territory["admin_center_id"] = city["territory_id"]
    territory_id = territory_id_param or region["territory_id"]

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.patch(f"/territory/{territory_id}", json=new_territory)

    # Assert
    assert_response(response, expected_status, Territory, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, territory_id_param, version",
    [
        (200, None, None, "v1"),
        (200, None, None, "v2"),
        (400, "You can use cities_only parameter only with including all levels", None, "v1"),
        (404, "not found", 1e9, "v1"),
    ],
    ids=["success_v1", "success_v2", "bad_request", "not_found"],
)
async def test_get_territories_by_parent_id(
    urban_api_host: str,
    city: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    territory_id_param: int | None,
    version: str,
):
    """Test GET /territories method."""

    # Arrange
    params = {
        "territory_type_id": city["territory_type"]["id"],
        "name": city["name"],
        "created_at": city["created_at"][:10],
        "get_all_levels": expected_status != 400,
        "cities_only": expected_status == 400,
        "page_size": 1,
    }
    if territory_id_param is not None:
        params["parent_id"] = territory_id_param

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/{version}") as client:
        response = await client.get("/territories", params=params)
        result = response.json()

    # Assert
    assert_response(response, expected_status, Page, error_message)
    if response.status_code == 200:
        assert len(result["results"]) > 0, "Response should contain at least one item."
        assert (
            len(result["results"]) <= params["page_size"]
        ), f"Response should contain no more than {params['page_size']} items."
        try:
            Territory(**result["results"][0])
        except ValidationError as e:
            pytest.fail(f"Pydantic validation error: {str(e)}")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, territory_id_param",
    [
        (200, None, None),
        (400, "You can use cities_only parameter only with including all levels", None),
        (404, "not found", 1e9),
    ],
    ids=["success", "bad_request", "not_found"],
)
async def test_get_all_territories_by_parent_id(
    urban_api_host: str,
    city: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    territory_id_param: int | None,
):
    """Test GET /all_territories method."""

    # Arrange
    params = {
        "territory_type_id": city["territory_type"]["id"],
        "name": city["name"],
        "created_at": city["created_at"][:10],
        "get_all_levels": expected_status != 400,
        "cities_only": expected_status == 400,
        "page_size": 1,
    }
    if territory_id_param is not None:
        params["parent_id"] = territory_id_param

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get("/all_territories", params=params)
        result = response.json()

    # Assert
    assert_response(response, expected_status, GeoJSONResponse, error_message)
    if response.status_code == 200:
        assert len(result["features"]) > 0, "Response should contain at least one feature."
        try:
            TerritoryWithoutGeometry(**result["features"][0]["properties"])
        except ValidationError as e:
            pytest.fail(f"Pydantic validation error: {str(e)}")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, territory_id_param, version",
    [
        (200, None, None, "v1"),
        (200, None, None, "v2"),
        (400, "You can use cities_only parameter only with including all levels", None, "v1"),
        (404, "not found", 1e9, "v1"),
    ],
    ids=["success_v1", "success_v2", "bad_request", "not_found"],
)
async def test_get_territories_without_geometry_by_parent_id(
    urban_api_host: str,
    city: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    territory_id_param: int | None,
    version: str,
):
    """Test GET /territories_without_geometry method."""

    # Arrange
    params = {
        "territory_type_id": city["territory_type"]["id"],
        "name": city["name"],
        "created_at": city["created_at"][:10],
        "get_all_levels": expected_status != 400,
        "cities_only": expected_status == 400,
        "page_size": 1,
    }
    if territory_id_param is not None:
        params["parent_id"] = territory_id_param

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/{version}") as client:
        response = await client.get("/territories_without_geometry", params=params)
        result = response.json()

    # Assert
    assert_response(response, expected_status, Page, error_message)
    if response.status_code == 200:
        assert len(result["results"]) > 0, "Response should contain at least one item."
        assert (
            len(result["results"]) <= params["page_size"]
        ), f"Response should contain no more than {params['page_size']} items."
        try:
            TerritoryWithoutGeometry(**result["results"][0])
        except ValidationError as e:
            pytest.fail(f"Pydantic validation error: {str(e)}")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, territory_id_param",
    [
        (200, None, None),
        (400, "You can use cities_only parameter only with including all levels", None),
        (404, "not found", 1e9),
    ],
    ids=["success", "bad_request", "not_found"],
)
async def test_get_all_territories_without_geometry_by_parent_id(
    urban_api_host: str,
    city: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    territory_id_param: int | None,
):
    """Test GET /all_territories_without_geometry method."""

    # Arrange
    params = {
        "territory_type_id": city["territory_type"]["id"],
        "name": city["name"],
        "created_at": city["created_at"][:10],
        "get_all_levels": expected_status != 400,
        "cities_only": expected_status == 400,
        "page_size": 1,
    }
    if territory_id_param is not None:
        params["parent_id"] = territory_id_param

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get("/all_territories_without_geometry", params=params)

    # Assert
    if response.status_code == 200:
        assert_response(response, expected_status, TerritoryWithoutGeometry, error_message, result_type="list")
    else:
        assert_response(response, expected_status, TerritoryWithoutGeometry, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, geometry_param",
    [
        (200, None, AllPossibleGeometry(type="Point", coordinates=[30.22, 59.86], geometries=None)),
        (400, None, AllPossibleGeometry(type="Polygon", coordinates=[30.22, 59.86], geometries=None)),
        (404, None, AllPossibleGeometry(type="Point", coordinates=[1, 1], geometries=None)),
    ],
    ids=["success", "bad_request", "not_found"],
)
async def test_get_common_territory(
    urban_api_host: str,
    city: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    geometry_param: AllPossibleGeometry,
):
    """Test POST /common_territory."""

    # Arrange
    territory_id = city["territory_id"]

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.post("/common_territory", json=geometry_param.model_dump())
        result = response.json()

    # Assert
    assert_response(response, expected_status, Territory, error_message)
    if response.status_code == 200:
        assert result["territory_id"] == territory_id, "Expected territory was not found in result."


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, territory_id_param, geometry_param",
    [
        (200, None, None, AllPossibleGeometry(type="Point", coordinates=[30.22, 59.86], geometries=None)),
        (400, None, None, AllPossibleGeometry(type="Polygon", coordinates=[30.22, 59.86], geometries=None)),
        (404, None, 1e9, AllPossibleGeometry(type="Point", coordinates=[30.22, 59.86], geometries=None)),
    ],
    ids=["success", "bad_request", "not_found"],
)
async def test_intersecting_territories(
    urban_api_host: str,
    municipality: dict[str, Any],
    city: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    territory_id_param: int | None,
    geometry_param: AllPossibleGeometry,
):
    """Test POST /territory/{parent_territory_id}/intersecting_territories."""

    # Arrange
    territory_id = territory_id_param or municipality["territory_id"]

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.post(
            f"/territory/{territory_id}/intersecting_territories", json=geometry_param.model_dump()
        )
        result = response.json()

    # Assert
    if response.status_code == 200:
        assert_response(response, expected_status, Territory, error_message, result_type="list")
        assert any(
            item["territory_id"] == city["territory_id"] for item in result
        ), "Expected territory was not found in result."
    else:
        assert_response(response, expected_status, Territory, error_message)


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
async def test_get_territories_by_ids(
    urban_api_host: str,
    city: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    ids_param: int | None,
):
    """Test GET /territories/{territories_ids} method."""

    # Arrange
    territories_ids = ids_param or str(city["territory_id"])

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get(f"/territories/{territories_ids}")
        result = response.json()

    # Assert
    assert_response(response, expected_status, GeoJSONResponse, error_message)
    if response.status_code == 200:
        assert len(result["features"]) > 0, "Response should contain at least one feature."
        try:
            TerritoryWithoutGeometry(**result["features"][0]["properties"])
        except ValidationError as e:
            pytest.fail(f"Pydantic validation error: {str(e)}")
