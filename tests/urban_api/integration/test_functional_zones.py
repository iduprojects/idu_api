"""Integration tests for functional zone objects are defined here."""

from typing import Any

import httpx
import pytest

from idu_api.urban_api.schemas import (
    FunctionalZone,
    FunctionalZonePost,
    FunctionalZoneType,
    FunctionalZoneTypePost,
    OkResponse,
    ProfilesReclamationData,
    ProfilesReclamationDataMatrix,
    ProfilesReclamationDataPost,
    ProfilesReclamationDataPut,
)
from idu_api.urban_api.schemas.geometries import AllPossibleGeometry
from tests.urban_api.helpers.utils import assert_response

####################################################################################
#                           Default use-case tests                                 #
####################################################################################


@pytest.mark.asyncio
async def test_get_functional_zone_types(urban_api_host: str, functional_zone_type: dict[str, Any]):
    """Test GET /functional_zones_types method."""

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get("/functional_zones_types")

    # Assert
    assert_response(response, 200, FunctionalZoneType, result_type="list")
    for res in response.json():
        for k, v in functional_zone_type.items():
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
async def test_add_functional_zone_type(
    urban_api_host: str,
    functional_zone_type_post_req: FunctionalZoneTypePost,
    expected_status: int,
    error_message: str | None,
):
    """Test POST /functional_zones_types method."""

    # Arrange
    new_zone_type = functional_zone_type_post_req.model_dump()
    new_zone_type["name"] = "new name"

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.post("/functional_zones_types", json=new_zone_type)

    # Assert
    assert_response(response, expected_status, FunctionalZoneType, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, labels_param, territory_id_param",
    [
        (200, None, None, None),
        (400, None, "1.2.3", None),
        (404, "not found", None, 1e9),
    ],
    ids=["success", "bad_request", "not_found"],
)
async def test_get_profiles_reclamation_data_matrix(
    urban_api_host: str,
    profiles_reclamation_data: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    labels_param: int | None,
    territory_id_param: str | None,
):
    """Test GET /profiles_reclamation/matrix method."""

    # Arrange
    params = {}
    if labels_param is not None:
        params["labels"] = labels_param
    if territory_id_param is not None:
        params["territory_id"] = territory_id_param

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get("/profiles_reclamation/matrix", params=params)

    # Assert
    assert_response(response, expected_status, ProfilesReclamationDataMatrix, error_message)
    if response.status_code == 200:
        assert (
            profiles_reclamation_data["source_profile_id"] in response.json()["labels"]
        ), "Expected source id was not found in labels."


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, type_id_param, territory_id_param",
    [
        (201, None, None, None),
        (404, "not found", 1e9, 1e9),
        (409, "already exists", None, None),
    ],
    ids=["success", "not_found", "conflict"],
)
async def test_add_profiles_reclamation_data(
    urban_api_host,
    profiles_reclamation_post_req: ProfilesReclamationDataPost,
    functional_zone_type: dict[str, Any],
    region: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    type_id_param: int | None,
    territory_id_param: int | None,
):
    """Test POST /profiles_reclamation method."""

    # Arrange
    new_profiles_reclamation = profiles_reclamation_post_req.model_dump()
    for k in ("source_profile_id", "target_profile_id"):
        new_profiles_reclamation[k] = type_id_param or functional_zone_type["functional_zone_type_id"]
    new_profiles_reclamation["territory_id"] = territory_id_param or region["territory_id"]

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.post("/profiles_reclamation", json=new_profiles_reclamation)

    # Assert
    assert_response(response, expected_status, ProfilesReclamationData, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, type_id_param, territory_id_param",
    [
        (200, None, None, None),
        (404, "not found", 1e9, 1e9),
    ],
    ids=["success", "not_found"],
)
async def test_put_profiles_reclamation_data(
    urban_api_host,
    profiles_reclamation_put_req: ProfilesReclamationDataPut,
    functional_zone_type: dict[str, Any],
    region: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    type_id_param: int | None,
    territory_id_param: int | None,
):
    """Test PUT /profiles_reclamation method."""

    # Arrange
    new_profiles_reclamation = profiles_reclamation_put_req.model_dump()
    for k in ("source_profile_id", "target_profile_id"):
        new_profiles_reclamation[k] = type_id_param or functional_zone_type["functional_zone_type_id"]
    new_profiles_reclamation["territory_id"] = territory_id_param or region["territory_id"]

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.put("/profiles_reclamation", json=new_profiles_reclamation)

    # Assert
    assert_response(response, expected_status, ProfilesReclamationData, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, type_id_param, territory_id_param",
    [
        (200, None, None, None),
        (404, "not found", 1e9, 1e9),
    ],
    ids=["success", "not_found"],
)
async def test_delete_profiles_reclamation_data(
    urban_api_host,
    functional_zone_type: dict[str, Any],
    region: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    type_id_param: int | None,
    territory_id_param: int | None,
):
    """Test DELETE /profiles_reclamation method."""

    # Arrange
    json_data = {
        "source_profile_id": type_id_param or functional_zone_type["functional_zone_type_id"],
        "target_profile_id": type_id_param or functional_zone_type["functional_zone_type_id"],
        "territory_id": territory_id_param or region["territory_id"],
    }

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.request("DELETE", "/profiles_reclamation", json=json_data)

    # Assert
    assert_response(response, expected_status, OkResponse, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, type_id_param, territory_id_param",
    [
        (201, None, None, None),
        (404, "not found", 1e9, 1e9),
    ],
    ids=["success", "not_found"],
)
async def test_add_functional_zone(
    urban_api_host,
    functional_zone_post_req: FunctionalZonePost,
    functional_zone_type: dict[str, Any],
    city: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    type_id_param: int | None,
    territory_id_param: int | None,
):
    """Test POST /functional_zones method."""

    # Arrange
    new_functional_zone = functional_zone_post_req.model_dump()
    new_functional_zone["functional_zone_type_id"] = type_id_param or functional_zone_type["functional_zone_type_id"]
    new_functional_zone["territory_id"] = territory_id_param or city["territory_id"]

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.post("/functional_zones", json=new_functional_zone)

    # Assert
    assert_response(response, expected_status, FunctionalZone, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, functional_zone_id_param",
    [
        (200, None, None),
        (404, "not found", 1e9),
    ],
    ids=["success", "not_found"],
)
async def test_put_functional_zone(
    urban_api_host,
    functional_zone: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    functional_zone_id_param: int | None,
):
    """Test PUT /functional_zones method."""

    # Arrange
    new_functional_zone = {k: v for k, v in functional_zone.items() if k not in ("territory", "functional_zone_type")}
    new_functional_zone["functional_zone_type_id"] = functional_zone["functional_zone_type"]["id"]
    new_functional_zone["territory_id"] = functional_zone["territory"]["id"]
    functional_zone_id = functional_zone_id_param or new_functional_zone.pop("functional_zone_id")

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.put(f"/functional_zones/{functional_zone_id}", json=new_functional_zone)

    # Assert
    assert_response(response, expected_status, FunctionalZone, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, functional_zone_id_param",
    [
        (200, None, None),
        (404, "not found", 1e9),
    ],
    ids=["success", "not_found"],
)
async def test_patch_functional_zone(
    urban_api_host,
    functional_zone: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    functional_zone_id_param: int | None,
):
    """Test PATCH /functional_zones method."""

    # Arrange
    new_functional_zone = {k: v for k, v in functional_zone.items() if k not in ("territory", "functional_zone_type")}
    new_functional_zone["functional_zone_type_id"] = functional_zone["functional_zone_type"]["id"]
    new_functional_zone["territory_id"] = functional_zone["territory"]["id"]
    functional_zone_id = functional_zone_id_param or new_functional_zone.pop("functional_zone_id")

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.patch(f"/functional_zones/{functional_zone_id}", json=new_functional_zone)

    # Assert
    assert_response(response, expected_status, FunctionalZone, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, functional_zone_id_param",
    [
        (200, None, None),
        (404, "not found", 1e9),
    ],
    ids=["success", "not_found"],
)
async def test_delete_functional_zone(
    urban_api_host: str,
    functional_zone_post_req: FunctionalZonePost,
    functional_zone_type: dict[str, Any],
    city: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    functional_zone_id_param: int | None,
):
    """Test DELETE /functional_zones method."""

    # Arrange
    new_functional_zone = functional_zone_post_req.model_dump()
    new_functional_zone["functional_zone_type_id"] = functional_zone_type["functional_zone_type_id"]
    new_functional_zone["territory_id"] = city["territory_id"]

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        if functional_zone_id_param is None:
            response = await client.post("/functional_zones", json=new_functional_zone)
            response = await client.delete(f"/functional_zones/{response.json()['functional_zone_id']}")
        else:
            response = await client.delete(f"/functional_zones/{functional_zone_id_param}")

    # Assert
    assert_response(response, expected_status, OkResponse, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, geometry_param",
    [
        (200, None, AllPossibleGeometry(type="Point", coordinates=[30.22, 59.86], geometries=None)),
        (400, None, AllPossibleGeometry(type="Polygon", coordinates=[30.22, 59.86], geometries=None)),
    ],
    ids=["success", "bad_request"],
)
async def test_get_functional_zones_intersects_geometry(
    urban_api_host: str,
    functional_zone: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    geometry_param: AllPossibleGeometry,
):
    """Test POST /functional_zones/around."""

    # Arrange
    functional_zone_id = functional_zone["functional_zone_id"]
    params = {"year": functional_zone["year"], "source": functional_zone["source"]}

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.post("/functional_zones/around", params=params, json=geometry_param.model_dump())

    # Assert
    if response.status_code == 200:
        assert_response(response, expected_status, FunctionalZone, error_message, result_type="list")
        assert any(
            functional_zone_id == item["functional_zone_id"] for item in response.json()
        ), "Expected physical object was not found in result."
    else:
        assert_response(response, expected_status, FunctionalZone, error_message)
