"""Integration tests for project-related geometries are defined here."""

from typing import Any

import httpx
import pytest
from pydantic import ValidationError

from idu_api.urban_api.schemas import ScenarioGeometryAttributes, ScenarioAllObjects, GeometryAttributes, AllObjects, \
    ObjectGeometryPut, ScenarioObjectGeometry, OkResponse, PhysicalObjectWithGeometryPost
from idu_api.urban_api.schemas.geometries import GeoJSONResponse
from tests.urban_api.helpers.utils import assert_response

####################################################################################
#                           Default use-case tests                                 #
####################################################################################


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, scenario_id_param",
    [
        (200, None, None),
        (403, "denied", None),
        (404, "not found", 1e9),
    ],
    ids=["success", "forbidden", "not_found"],
)
async def test_get_geometries_by_scenario_id(
    urban_api_host: str,
    scenario: dict[str, Any],
    scenario_geometry: dict[str, Any],
    valid_token: str,
    superuser_token: str,
    expected_status: int,
    error_message: str | None,
    scenario_id_param: int | None,
):
    """Test GET /scenarios/{scenario_id}/geometries method."""

    # Arrange
    scenario_id = scenario_id_param or scenario["scenario_id"]
    headers = {"Authorization": f"Bearer {valid_token if expected_status == 403 else superuser_token}"}

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get(f"/scenarios/{scenario_id}/geometries", headers=headers)
        result = response.json()

    # Assert
    assert_response(response, expected_status, GeoJSONResponse, error_message)
    if response.status_code == 200:
        assert len(result["features"]) > 0, "Response should contain at least one feature."
        try:
            ScenarioGeometryAttributes(**result["features"][0]["properties"])
        except ValidationError as e:
            pytest.fail(f"Pydantic validation error: {str(e)}")
        assert any(
            scenario_geometry["object_geometry_id"] == item["properties"]["object_geometry_id"]
            for item in result["features"]
        ), "Response should contain created geometry."


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, scenario_id_param",
    [
        (200, None, None),
        (403, "denied", None),
        (404, "not found", 1e9),
    ],
    ids=["success", "forbidden", "not_found"],
)
async def test_get_geometries_with_all_objects_by_scenario_id(
    urban_api_host: str,
    scenario: dict[str, Any],
    scenario_geometry: dict[str, Any],
    valid_token: str,
    superuser_token: str,
    expected_status: int,
    error_message: str | None,
    scenario_id_param: int | None,
):
    """Test GET /scenarios/{scenario_id}/geometries_with_all_objects method."""

    # Arrange
    scenario_id = scenario_id_param or scenario["scenario_id"]
    headers = {"Authorization": f"Bearer {valid_token if expected_status == 403 else superuser_token}"}

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get(f"/scenarios/{scenario_id}/geometries_with_all_objects", headers=headers)
        result = response.json()

    # Assert
    assert_response(response, expected_status, GeoJSONResponse, error_message)
    if response.status_code == 200:
        assert len(result["features"]) > 0, "Response should contain at least one feature."
        try:
            ScenarioAllObjects(**result["features"][0]["properties"])
        except ValidationError as e:
            pytest.fail(f"Pydantic validation error: {str(e)}")
        assert any(
            scenario_geometry["object_geometry_id"] == item["properties"]["object_geometry_id"]
            for item in result["features"]
        ), "Response should contain created geometry."


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, project_id_param",
    [
        (200, None, None),
        (403, "denied", None),
        (404, "not found", 1e9),
    ],
    ids=["success", "forbidden", "not_found"],
)
async def test_get_context_geometries(
    urban_api_host: str,
    project: dict[str, Any],
    object_geometry: dict[str, Any],
    valid_token: str,
    superuser_token: str,
    expected_status: int,
    error_message: str | None,
    project_id_param: int | None,
):
    """Test GET /projects/{project_id}/context/geometries method."""

    # Arrange
    project_id = project_id_param or project["project_id"]
    headers = {"Authorization": f"Bearer {valid_token if expected_status == 403 else superuser_token}"}

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get(f"/projects/{project_id}/context/geometries", headers=headers)
        result = response.json()

    # Assert
    assert_response(response, expected_status, GeoJSONResponse, error_message)
    if response.status_code == 200:
        assert len(result["features"]) > 0, "Response should contain at least one feature."
        try:
            GeometryAttributes(**result["features"][0]["properties"])
        except ValidationError as e:
            pytest.fail(f"Pydantic validation error: {str(e)}")
        assert any(
            object_geometry["object_geometry_id"] == item["properties"]["object_geometry_id"]
            for item in result["features"]
        ), "Response should contain created geometry."


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, project_id_param",
    [
        (200, None, None),
        (403, "denied", None),
        (404, "not found", 1e9),
    ],
    ids=["success", "forbidden", "not_found"],
)
async def test_get_context_geometries_with_all_objects(
    urban_api_host: str,
    project: dict[str, Any],
    object_geometry: dict[str, Any],
    valid_token: str,
    superuser_token: str,
    expected_status: int,
    error_message: str | None,
    project_id_param: int | None,
):
    """Test GET /projects/{project_id}/context/geometries_with_all_objects method."""

    # Arrange
    project_id = project_id_param or project["project_id"]
    headers = {"Authorization": f"Bearer {valid_token if expected_status == 403 else superuser_token}"}

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get(f"/projects/{project_id}/context/geometries_with_all_objects", headers=headers)
        result = response.json()

    # Assert
    assert_response(response, expected_status, GeoJSONResponse, error_message)
    if response.status_code == 200:
        assert len(result["features"]) > 0, "Response should contain at least one feature."
        try:
            AllObjects(**result["features"][0]["properties"])
        except ValidationError as e:
            pytest.fail(f"Pydantic validation error: {str(e)}")
        assert any(
            object_geometry["object_geometry_id"] == item["properties"]["object_geometry_id"]
            for item in result["features"]
        ), "Response should contain created geometry."


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, scenario_id_param, is_scenario_param",
    [
        (200, None, None, True),
        (200, None, None, False),
        (403, "denied", None, True),
        (404, "not found", 1e9, True),
        (409, "already exists", None, False),
    ],
    ids=["success_1", "success_2", "forbidden", "not_found", "conflict"],
)
async def test_put_scenario_geometry(
    urban_api_host: str,
    object_geometries_put_req: ObjectGeometryPut,
    scenario: dict[str, Any],
    scenario_geometry: dict[str, Any],
    object_geometry: dict[str, Any],
    valid_token: str,
    superuser_token: str,
    expected_status: int,
    error_message: str | None,
    scenario_id_param: int | None,
    is_scenario_param: bool,
):
    """Test PUT /scenarios/{scenario_id}/geometries method."""

    # Arrange
    scenario_id = scenario_id_param or scenario["scenario_id"]
    object_geometry_id = scenario_geometry["object_geometry_id"] if is_scenario_param else object_geometry["object_geometry_id"]
    new_object_geometry = object_geometries_put_req.model_dump()
    new_object_geometry["territory_id"] = scenario_geometry["territory"]["id"]
    headers = {"Authorization": f"Bearer {valid_token if expected_status == 403 else superuser_token}"}
    params = {"is_scenario_object": is_scenario_param}

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.put(
            f"/scenarios/{scenario_id}/geometries/{object_geometry_id}",
            json=new_object_geometry,
            headers=headers,
            params=params,
        )

    # Assert
    assert_response(response, expected_status, ScenarioObjectGeometry, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, scenario_id_param, is_scenario_param",
    [
        (200, None, None, True),
        (200, None, None, False),
        (403, "denied", None, True),
        (404, "not found", 1e9, True),
        (409, "already exists", None, False),
    ],
    ids=["success_1", "success_2", "forbidden", "not_found", "conflict"],
)
async def test_patch_scenario_geometry(
    urban_api_host: str,
    object_geometries_put_req: ObjectGeometryPut,
    scenario: dict[str, Any],
    scenario_geometry: dict[str, Any],
    object_geometry: dict[str, Any],
    project: dict[str, Any],
    functional_zone_type: dict[str, Any],
    valid_token: str,
    superuser_token: str,
    expected_status: int,
    error_message: str | None,
    scenario_id_param: int | None,
    is_scenario_param: bool,
):
    """Test PATCH /scenarios/{scenario_id}/geometries method."""

    # Arrange
    scenario_id = scenario_id_param or scenario["scenario_id"]
    if expected_status != 409 and not is_scenario_param:
        base_scenario_id = project["base_scenario"]["id"]
        headers = {"Authorization": f"Bearer {superuser_token}"}
        new_scenario = {
            "project_id": project["project_id"],
            "name": "Test Scenario Name",
            "functional_zone_type_id": functional_zone_type["functional_zone_type_id"],
        }
        async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
            response = await client.post(f"/scenarios/{base_scenario_id}", json=new_scenario, headers=headers)
            scenario_id = response.json()["scenario_id"]
    object_geometry_id = scenario_geometry["object_geometry_id"] if is_scenario_param else object_geometry["object_geometry_id"]
    new_object_geometry = object_geometries_put_req.model_dump()
    new_object_geometry["territory_id"] = scenario_geometry["territory"]["id"]
    headers = {"Authorization": f"Bearer {valid_token if expected_status == 403 else superuser_token}"}
    params = {"is_scenario_object": is_scenario_param}

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.patch(
            f"/scenarios/{scenario_id}/geometries/{object_geometry_id}",
            json=new_object_geometry,
            headers=headers,
            params=params,
        )

    # Assert
    assert_response(response, expected_status, ScenarioObjectGeometry, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, scenario_id_param, is_scenario_param",
    [
        (200, None, None, True),
        (200, None, None, False),
        (403, "denied", None, True),
        (404, "not found", 1e9, True),
    ],
    ids=["success_1", "success_2", "forbidden", "not_found"],
)
async def test_delete_object_geometry(
    urban_api_host: str,
    physical_object_with_geometry_post_req: PhysicalObjectWithGeometryPost,
    scenario: dict[str, Any],
    scenario_physical_object: dict[str, Any],
    scenario_geometry: dict[str, Any],
    object_geometry: dict[str, Any],
    project: dict[str, Any],
    functional_zone_type: dict[str, Any],
    valid_token: str,
    superuser_token: str,
    expected_status: int,
    error_message: str | None,
    scenario_id_param: int | None,
    is_scenario_param: bool,
):
    """Test DELETE /scenarios/{scenario_id}/geometries method."""

    # Arrange
    scenario_id = scenario_id_param or scenario["scenario_id"]
    if not is_scenario_param:
        base_scenario_id = project["base_scenario"]["id"]
        headers = {"Authorization": f"Bearer {superuser_token}"}
        new_scenario = {
            "project_id": project["project_id"],
            "name": "Test Scenario Name",
            "functional_zone_type_id": functional_zone_type["functional_zone_type_id"],
        }
        async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
            response = await client.post(f"/scenarios/{base_scenario_id}", json=new_scenario, headers=headers)
            scenario_id = response.json()["scenario_id"]
    new_object = physical_object_with_geometry_post_req.model_dump()
    new_object["physical_object_type_id"] = scenario_physical_object["physical_object_type"]["physical_object_type_id"]
    new_object["territory_id"] = scenario_geometry["territory"]["id"]
    headers = {"Authorization": f"Bearer {valid_token if expected_status == 403 else superuser_token}"}
    params = {"is_scenario_object": is_scenario_param}

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        if expected_status == 200 and is_scenario_param:
            response = await client.post(
                f"scenarios/{scenario_id}/physical_objects",
                json=new_object, headers=headers,
            )
            object_geometry_id = response.json()["object_geometry"]["object_geometry_id"]
            response = await client.delete(
                f"/scenarios/{scenario_id}/geometries/{object_geometry_id}",
                headers=headers, params=params,
            )
        elif not is_scenario_param:
            object_geometry_id = object_geometry["object_geometry_id"]
            response = await client.delete(
                f"/scenarios/{scenario_id}/geometries/{object_geometry_id}",
                headers=headers, params=params,
            )
        else:
            response = await client.delete(
                f"/scenarios/{scenario_id}/geometries/1",
                headers=headers, params=params,
            )

    # Assert
    assert_response(response, expected_status, OkResponse, error_message)
