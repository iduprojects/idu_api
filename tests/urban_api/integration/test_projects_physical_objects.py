"""Integration tests for project-related physical objects are defined here."""

from typing import Any

import httpx
import pytest
from pydantic import ValidationError

from idu_api.urban_api.schemas import (
    BuildingPost,
    OkResponse,
    PhysicalObject,
    PhysicalObjectPut,
    PhysicalObjectWithGeometryPost,
    ScenarioBuildingPatch,
    ScenarioBuildingPost,
    ScenarioBuildingPut,
    ScenarioPhysicalObject,
    ScenarioPhysicalObjectWithGeometryAttributes,
    ScenarioUrbanObject,
)
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
        (400, "please, choose either physical_object_type_id or physical_object_function_id", None),
        (403, "denied", None),
        (404, "not found", 1e9),
    ],
    ids=["success", "bad_request", "forbidden", "not_found"],
)
async def test_get_physical_objects_by_scenario_id(
    urban_api_host: str,
    scenario: dict[str, Any],
    scenario_physical_object: dict[str, Any],
    valid_token: str,
    superuser_token: str,
    expected_status: int,
    error_message: str | None,
    scenario_id_param: int | None,
):
    """Test GET /scenarios/{scenario_id}/physical_objects method."""

    # Arrange
    scenario_id = scenario_id_param or scenario["scenario_id"]
    headers = {"Authorization": f"Bearer {valid_token if expected_status == 403 else superuser_token}"}
    params = {"physical_object_type_id": scenario_physical_object["physical_object_type"]["physical_object_type_id"]}
    if expected_status == 400:
        physical_object_function = scenario_physical_object["physical_object_type"]["physical_object_function"]
        params["physical_object_function_id"] = physical_object_function["id"]

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get(f"/scenarios/{scenario_id}/physical_objects", headers=headers, params=params)
        result = response.json()

    # Assert
    if expected_status == 200:
        assert_response(response, expected_status, ScenarioPhysicalObject, error_message, result_type="list")
        assert any(
            scenario_physical_object["physical_object_id"] == item["physical_object_id"] for item in result
        ), "Response should contain created physical_object."
    else:
        assert_response(response, expected_status, ScenarioPhysicalObject, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, scenario_id_param, is_regional_param",
    [
        (200, None, None, False),
        (200, None, None, True),
        (400, "please, choose either physical_object_type_id or physical_object_function_id", None, False),
        (403, "denied", None, False),
        (404, "not found", 1e9, False),
    ],
    ids=["success_common", "success_regional", "bad_request", "forbidden", "not_found"],
)
async def test_get_physical_objects_with_geometry_by_scenario_id(
    urban_api_host: str,
    scenario: dict[str, Any],
    regional_scenario: dict[str, Any],
    scenario_physical_object: dict[str, Any],
    valid_token: str,
    superuser_token: str,
    expected_status: int,
    error_message: str | None,
    scenario_id_param: int | None,
    is_regional_param: bool,
):
    """Test GET /scenarios/{scenario_id}/physical_objects_with_geometry method."""

    # Arrange
    scenario_id = scenario_id_param or (
        regional_scenario["scenario_id"] if is_regional_param else scenario["scenario_id"]
    )
    headers = {"Authorization": f"Bearer {valid_token if expected_status == 403 else superuser_token}"}
    params = {"physical_object_type_id": scenario_physical_object["physical_object_type"]["physical_object_type_id"]}
    if expected_status == 400 and not is_regional_param:
        physical_object_function = scenario_physical_object["physical_object_type"]["physical_object_function"]
        params["physical_object_function_id"] = physical_object_function["id"]

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get(
            f"/scenarios/{scenario_id}/physical_objects_with_geometry", headers=headers, params=params
        )
        result = response.json()

    # Assert
    assert_response(response, expected_status, GeoJSONResponse, error_message)
    if response.status_code == 200:
        assert len(result["features"]) > 0, "Response should contain at least one feature."
        try:
            ScenarioPhysicalObjectWithGeometryAttributes(**result["features"][0]["properties"])
        except ValidationError as e:
            pytest.fail(f"Pydantic validation error: {str(e)}")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, project_id_param, is_regional_param",
    [
        (200, None, None, False),
        (400, "please, choose either physical_object_type_id or physical_object_function_id", None, False),
        (400, "this method cannot be accessed in a regional project", None, True),
        (403, "denied", None, False),
        (404, "not found", 1e9, False),
    ],
    ids=["success", "bad_request", "regional_project", "forbidden", "not_found"],
)
async def test_get_context_physical_objects(
    urban_api_host: str,
    project: dict[str, Any],
    regional_project: dict[str, Any],
    physical_object: dict[str, Any],
    valid_token: str,
    superuser_token: str,
    expected_status: int,
    error_message: str | None,
    project_id_param: int | None,
    is_regional_param: bool,
):
    """Test GET /projects/{project_id}/context/physical_objects method."""

    # Arrange
    project_id = project_id_param or (regional_project["project_id"] if is_regional_param else project["project_id"])
    headers = {"Authorization": f"Bearer {valid_token if expected_status == 403 else superuser_token}"}
    params = {"physical_object_type_id": physical_object["physical_object_type"]["physical_object_type_id"]}
    if expected_status == 400 and not is_regional_param:
        physical_object_function = physical_object["physical_object_type"]["physical_object_function"]
        params["physical_object_function_id"] = physical_object_function["id"]

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get(f"/projects/{project_id}/context/physical_objects", headers=headers, params=params)
        result = response.json()

    # Assert
    if expected_status == 200:
        assert_response(response, expected_status, PhysicalObject, error_message, result_type="list")
        assert any(
            physical_object["physical_object_id"] == item["physical_object_id"] for item in result
        ), "Response should contain created physical_object."
    else:
        assert_response(response, expected_status, ScenarioPhysicalObject, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, project_id_param, is_regional_param",
    [
        (200, None, None, False),
        (400, "please, choose either physical_object_type_id or physical_object_function_id", None, False),
        (400, "this method cannot be accessed in a regional project", None, True),
        (403, "denied", None, False),
        (404, "not found", 1e9, False),
    ],
    ids=["success", "bad_request", "regional_project", "forbidden", "not_found"],
)
async def test_get_context_physical_objects_with_geometry(
    urban_api_host: str,
    project: dict[str, Any],
    regional_project: dict[str, Any],
    physical_object: dict[str, Any],
    valid_token: str,
    superuser_token: str,
    expected_status: int,
    error_message: str | None,
    project_id_param: int | None,
    is_regional_param: bool,
):
    """Test GET /projects/{project_id}/context/physical_objects_with_geometry method."""

    # Arrange
    project_id = project_id_param or (regional_project["project_id"] if is_regional_param else project["project_id"])
    headers = {"Authorization": f"Bearer {valid_token if expected_status == 403 else superuser_token}"}
    params = {"physical_object_type_id": physical_object["physical_object_type"]["physical_object_type_id"]}
    if expected_status == 400 and not is_regional_param:
        physical_object_function = physical_object["physical_object_type"]["physical_object_function"]
        params["physical_object_function_id"] = physical_object_function["id"]

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get(
            f"/projects/{project_id}/context/physical_objects_with_geometry", headers=headers, params=params
        )
        result = response.json()

    # Assert
    assert_response(response, expected_status, GeoJSONResponse, error_message)
    if response.status_code == 200:
        assert len(result["features"]) > 0, "Response should contain at least one feature."
        try:
            PhysicalObject(**result["features"][0]["properties"])
        except ValidationError as e:
            pytest.fail(f"Pydantic validation error: {str(e)}")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, scenario_id_param",
    [
        (201, None, None),
        (403, "denied", None),
        (404, "not found", 1e9),
    ],
    ids=["success", "forbidden", "not_found"],
)
async def test_add_physical_object_with_geometry(
    urban_api_host: str,
    physical_object_with_geometry_post_req: PhysicalObjectWithGeometryPost,
    scenario: dict[str, Any],
    scenario_physical_object: dict[str, Any],
    city: dict[str, Any],
    valid_token: str,
    superuser_token: str,
    expected_status: int,
    error_message: str | None,
    scenario_id_param: int | None,
):
    """Test POST /scenarios/{scenario_id}/physical_objects method."""

    # Arrange
    scenario_id = scenario_id_param or scenario["scenario_id"]
    new_object = physical_object_with_geometry_post_req.model_dump()
    new_object["physical_object_type_id"] = scenario_physical_object["physical_object_type"]["physical_object_type_id"]
    new_object["territory_id"] = city["territory_id"]
    headers = {"Authorization": f"Bearer {valid_token if expected_status == 403 else superuser_token}"}

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.post(
            f"/scenarios/{scenario_id}/physical_objects",
            json=new_object,
            headers=headers,
        )

    # Assert
    assert_response(response, expected_status, ScenarioUrbanObject, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, scenario_id_param",
    [
        (201, None, None),
        (400, "You can only upload physical objects with given physical object function", None),
        (403, "denied", None),
        (404, "not found", 1e9),
    ],
    ids=["success", "bad_request", "forbidden", "not_found"],
)
async def test_update_physical_objects_by_function_id(
    urban_api_host: str,
    physical_object_with_geometry_post_req: PhysicalObjectWithGeometryPost,
    project: dict[str, Any],
    functional_zone_type: dict[str, Any],
    scenario_physical_object: dict[str, Any],
    city: dict[str, Any],
    valid_token: str,
    superuser_token: str,
    expected_status: int,
    error_message: str | None,
    scenario_id_param: int | None,
):
    """Test POST /scenarios/{scenario_id}/all_physical_objects method."""

    # Arrange
    if scenario_id_param is None:
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
    else:
        scenario_id = scenario_id_param
    new_object = physical_object_with_geometry_post_req.model_dump()
    new_object["physical_object_type_id"] = scenario_physical_object["physical_object_type"]["physical_object_type_id"]
    new_object["territory_id"] = city["territory_id"]
    headers = {"Authorization": f"Bearer {valid_token if expected_status == 403 else superuser_token}"}
    function_id = scenario_physical_object["physical_object_type"]["physical_object_function"]["id"]
    params = {"physical_object_function_id": function_id if expected_status != 400 else 1e9}

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.post(
            f"/scenarios/{scenario_id}/all_physical_objects",
            json=[new_object],
            headers=headers,
            params=params,
        )

    # Assert
    if expected_status == 201:
        assert_response(response, expected_status, ScenarioUrbanObject, error_message, result_type="list")
    else:
        assert_response(response, expected_status, ScenarioUrbanObject, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, scenario_id_param, is_scenario_param",
    [
        (200, None, None, True),
        (200, None, None, False),
        (403, "denied", None, True),
        (404, "not found", 1e9, True),
        (409, "has already been edited or deleted for the scenario", None, False),
    ],
    ids=["success_1", "success_2", "forbidden", "not_found", "conflict"],
)
async def test_put_scenario_physical_object(
    urban_api_host: str,
    physical_object_put_req: PhysicalObjectPut,
    scenario: dict[str, Any],
    scenario_physical_object: dict[str, Any],
    physical_object: dict[str, Any],
    valid_token: str,
    superuser_token: str,
    expected_status: int,
    error_message: str | None,
    scenario_id_param: int | None,
    is_scenario_param: bool,
):
    """Test PUT /scenarios/{scenario_id}/physical_objects method."""

    # Arrange
    scenario_id = scenario_id_param or scenario["scenario_id"]
    physical_object_id = (
        scenario_physical_object["physical_object_id"] if is_scenario_param else physical_object["physical_object_id"]
    )
    new_object = physical_object_put_req.model_dump()
    new_object["physical_object_type_id"] = scenario_physical_object["physical_object_type"]["physical_object_type_id"]
    headers = {"Authorization": f"Bearer {valid_token if expected_status == 403 else superuser_token}"}
    params = {"is_scenario_object": is_scenario_param}

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.put(
            f"/scenarios/{scenario_id}/physical_objects/{physical_object_id}",
            json=new_object,
            headers=headers,
            params=params,
        )

    # Assert
    assert_response(response, expected_status, ScenarioPhysicalObject, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, scenario_id_param, is_scenario_param",
    [
        (200, None, None, True),
        (200, None, None, False),
        (403, "denied", None, True),
        (404, "not found", 1e9, True),
        (409, "has already been edited or deleted for the scenario", None, False),
    ],
    ids=["success_1", "success_2", "forbidden", "not_found", "conflict"],
)
async def test_patch_scenario_physical_object(
    urban_api_host: str,
    physical_object_put_req: PhysicalObjectPut,
    scenario: dict[str, Any],
    scenario_physical_object: dict[str, Any],
    physical_object: dict[str, Any],
    project: dict[str, Any],
    functional_zone_type: dict[str, Any],
    valid_token: str,
    superuser_token: str,
    expected_status: int,
    error_message: str | None,
    scenario_id_param: int | None,
    is_scenario_param: bool,
):
    """Test PATCH /scenarios/{scenario_id}/physical_objects method."""

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
    physical_object_id = (
        scenario_physical_object["physical_object_id"] if is_scenario_param else physical_object["physical_object_id"]
    )
    new_object = physical_object_put_req.model_dump()
    new_object["physical_object_type_id"] = scenario_physical_object["physical_object_type"]["physical_object_type_id"]
    headers = {"Authorization": f"Bearer {valid_token if expected_status == 403 else superuser_token}"}
    params = {"is_scenario_object": is_scenario_param}

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.patch(
            f"/scenarios/{scenario_id}/physical_objects/{physical_object_id}",
            json=new_object,
            headers=headers,
            params=params,
        )

    # Assert
    assert_response(response, expected_status, ScenarioPhysicalObject, error_message)


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
async def test_delete_physical_object(
    urban_api_host: str,
    physical_object_with_geometry_post_req: PhysicalObjectWithGeometryPost,
    scenario: dict[str, Any],
    scenario_physical_object: dict[str, Any],
    scenario_geometry: dict[str, Any],
    physical_object: dict[str, Any],
    project: dict[str, Any],
    functional_zone_type: dict[str, Any],
    valid_token: str,
    superuser_token: str,
    expected_status: int,
    error_message: str | None,
    scenario_id_param: int | None,
    is_scenario_param: bool,
):
    """Test DELETE /scenarios/{scenario_id}/physical_objects method."""

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
                json=new_object,
                headers=headers,
            )
            physical_object_id = response.json()["physical_object"]["physical_object_id"]
            response = await client.delete(
                f"/scenarios/{scenario_id}/physical_objects/{physical_object_id}",
                headers=headers,
                params=params,
            )
        elif not is_scenario_param:
            physical_object_id = physical_object["physical_object_id"]
            response = await client.delete(
                f"/scenarios/{scenario_id}/physical_objects/{physical_object_id}",
                headers=headers,
                params=params,
            )
        else:
            response = await client.delete(
                f"/scenarios/{scenario_id}/physical_objects/1",
                headers=headers,
                params=params,
            )

    # Assert
    assert_response(response, expected_status, OkResponse, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, scenario_id_param, is_scenario_param",
    [
        (201, None, None, True),
        (201, None, None, False),
        (403, "denied", None, True),
        (404, "not found", 1e9, True),
        (409, "already exists", None, True),
    ],
    ids=["success_1", "success_2", "forbidden", "not_found", "conflict"],
)
async def test_add_scenario_building(
    urban_api_host: str,
    scenario_building_post_req: ScenarioBuildingPost,
    physical_object_with_geometry_post_req: PhysicalObjectWithGeometryPost,
    scenario: dict[str, Any],
    scenario_physical_object: dict[str, Any],
    physical_object_type: dict[str, Any],
    city: dict[str, Any],
    valid_token: str,
    superuser_token: str,
    expected_status: int,
    error_message: str | None,
    scenario_id_param: int | None,
    is_scenario_param: bool,
):
    """Test POST /scenarios/{scenario_id}/buildings method."""

    # Arrange
    scenario_id = scenario_id_param or scenario["scenario_id"]
    physical_object_id = scenario_physical_object["physical_object_id"]
    if not is_scenario_param:
        new_object = physical_object_with_geometry_post_req.model_dump()
        new_object["physical_object_type_id"] = physical_object_type["physical_object_type_id"]
        new_object["territory_id"] = city["territory_id"]
        async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
            response = await client.post("/physical_objects", json=new_object)
            physical_object_id = response.json()["physical_object"]["physical_object_id"]
    new_building = scenario_building_post_req.model_dump()
    new_building["physical_object_id"] = physical_object_id
    new_building["is_scenario_object"] = is_scenario_param
    headers = {"Authorization": f"Bearer {valid_token if expected_status == 403 else superuser_token}"}

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.post(f"/scenarios/{scenario_id}/buildings", json=new_building, headers=headers)

    # Assert
    assert_response(response, expected_status, ScenarioPhysicalObject, error_message)


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
async def test_put_scenario_building(
    urban_api_host: str,
    scenario_building_put_req: ScenarioBuildingPut,
    physical_object_with_geometry_post_req: PhysicalObjectWithGeometryPost,
    scenario: dict[str, Any],
    scenario_physical_object: dict[str, Any],
    physical_object_type: dict[str, Any],
    city: dict[str, Any],
    valid_token: str,
    superuser_token: str,
    expected_status: int,
    error_message: str | None,
    scenario_id_param: int | None,
    is_scenario_param: bool,
):
    """Test PUT /scenarios/{scenario_id}/buildings method."""

    # Arrange
    scenario_id = scenario_id_param or scenario["scenario_id"]
    physical_object_id = scenario_physical_object["physical_object_id"]
    if not is_scenario_param:
        new_object = physical_object_with_geometry_post_req.model_dump()
        new_object["physical_object_type_id"] = physical_object_type["physical_object_type_id"]
        new_object["territory_id"] = city["territory_id"]
        async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
            response = await client.post("/physical_objects", json=new_object)
            physical_object_id = response.json()["physical_object"]["physical_object_id"]
    new_building = scenario_building_put_req.model_dump()
    new_building["physical_object_id"] = physical_object_id
    new_building["is_scenario_object"] = is_scenario_param
    headers = {"Authorization": f"Bearer {valid_token if expected_status == 403 else superuser_token}"}

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.put(f"/scenarios/{scenario_id}/buildings", json=new_building, headers=headers)

    # Assert
    assert_response(response, expected_status, ScenarioPhysicalObject, error_message)


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
async def test_patch_scenario_building(
    urban_api_host: str,
    building_post_req: BuildingPost,
    scenario_building_put_req: ScenarioBuildingPut,
    scenario_building_patch_req: ScenarioBuildingPatch,
    physical_object_with_geometry_post_req: PhysicalObjectWithGeometryPost,
    scenario: dict[str, Any],
    scenario_physical_object: dict[str, Any],
    physical_object_type: dict[str, Any],
    city: dict[str, Any],
    valid_token: str,
    superuser_token: str,
    expected_status: int,
    error_message: str | None,
    scenario_id_param: int | None,
    is_scenario_param: bool,
):
    """Test PATCH /scenarios/{scenario_id}/buildings method."""

    # Arrange
    scenario_id = scenario_id_param or scenario["scenario_id"]
    if not is_scenario_param:
        new_object = physical_object_with_geometry_post_req.model_dump()
        new_object["physical_object_type_id"] = physical_object_type["physical_object_type_id"]
        new_object["territory_id"] = city["territory_id"]
        async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
            response = await client.post("/physical_objects", json=new_object)
            physical_object_id = response.json()["physical_object"]["physical_object_id"]
            new_building = building_post_req.model_dump()
            new_building["physical_object_id"] = physical_object_id
            response = await client.post("/buildings", json=new_building)
            building_id = response.json()["building"]["id"]
    else:
        new_building = scenario_building_put_req.model_dump()
        new_building["physical_object_id"] = scenario_physical_object["physical_object_id"]
        new_building["is_scenario_object"] = is_scenario_param
        headers = {"Authorization": f"Bearer {superuser_token}"}
        async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
            response = await client.put(
                f"/scenarios/{scenario['scenario_id']}/buildings", json=new_building, headers=headers
            )
            building_id = response.json()["building"]["id"]
    new_building = scenario_building_patch_req.model_dump()
    params = {"is_scenario_object": is_scenario_param}
    headers = {"Authorization": f"Bearer {valid_token if expected_status == 403 else superuser_token}"}

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.patch(
            f"/scenarios/{scenario_id}/buildings/{building_id}",
            json=new_building,
            params=params,
            headers=headers,
        )

    # Assert
    assert_response(response, expected_status, ScenarioPhysicalObject, error_message)


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
async def test_delete_scenario_building(
    urban_api_host: str,
    building_post_req: BuildingPost,
    scenario_building_put_req: ScenarioBuildingPut,
    physical_object_with_geometry_post_req: PhysicalObjectWithGeometryPost,
    scenario: dict[str, Any],
    scenario_physical_object: dict[str, Any],
    physical_object_type: dict[str, Any],
    city: dict[str, Any],
    valid_token: str,
    superuser_token: str,
    expected_status: int,
    error_message: str | None,
    scenario_id_param: int | None,
    is_scenario_param: bool,
):
    """Test DELETE /scenarios/{scenario_id}/buildings method."""

    # Arrange
    scenario_id = scenario_id_param or scenario["scenario_id"]
    if not is_scenario_param:
        new_object = physical_object_with_geometry_post_req.model_dump()
        new_object["physical_object_type_id"] = physical_object_type["physical_object_type_id"]
        new_object["territory_id"] = city["territory_id"]
        async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
            response = await client.post("/physical_objects", json=new_object)
            physical_object_id = response.json()["physical_object"]["physical_object_id"]
            new_building = building_post_req.model_dump()
            new_building["physical_object_id"] = physical_object_id
            response = await client.post("/buildings", json=new_building)
            building_id = response.json()["building"]["id"]
    else:
        new_building = scenario_building_put_req.model_dump()
        new_building["physical_object_id"] = scenario_physical_object["physical_object_id"]
        new_building["is_scenario_object"] = is_scenario_param
        headers = {"Authorization": f"Bearer {superuser_token}"}
        async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
            response = await client.put(
                f"/scenarios/{scenario['scenario_id']}/buildings", json=new_building, headers=headers
            )
            building_id = response.json()["building"]["id"]
    params = {"is_scenario_object": is_scenario_param}
    headers = {"Authorization": f"Bearer {valid_token if expected_status == 403 else superuser_token}"}

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.delete(
            f"/scenarios/{scenario_id}/buildings/{building_id}",
            params=params,
            headers=headers,
        )

    # Assert
    assert_response(response, expected_status, OkResponse, error_message)
