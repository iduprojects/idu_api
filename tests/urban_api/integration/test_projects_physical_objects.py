"""Integration tests for project-related physical objects are defined here."""

from typing import Any

import httpx
import pytest
from pydantic import ValidationError

from idu_api.urban_api.schemas import ScenarioPhysicalObject, PhysicalObject, PhysicalObjectPut, \
    PhysicalObjectWithGeometryPost, OkResponse, ScenarioUrbanObject
from tests.urban_api.helpers.utils import assert_response

####################################################################################
#                           Default use-case tests                                 #
####################################################################################


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, scenario_id_param",
    [
        (200, None, None),
        (400, "Please, choose either physical_object_type_id or physical_object_function_id", None),
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
    "expected_status, error_message, project_id_param",
    [
        (200, None, None),
        (400, "Please, choose either physical_object_type_id or physical_object_function_id", None),
        (403, "denied", None),
        (404, "not found", 1e9),
    ],
    ids=["success", "bad_request", "forbidden", "not_found"],
)
async def test_get_context_physical_objects(
    urban_api_host: str,
    project: dict[str, Any],
    physical_object: dict[str, Any],
    valid_token: str,
    superuser_token: str,
    expected_status: int,
    error_message: str | None,
    project_id_param: int | None,
):
    """Test GET /projects/{project_id}/context/physical_objects method."""

    # Arrange
    project_id = project_id_param or project["project_id"]
    headers = {"Authorization": f"Bearer {valid_token if expected_status == 403 else superuser_token}"}
    params = {"physical_object_type_id": physical_object["physical_object_type"]["physical_object_type_id"]}
    if expected_status == 400:
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
            json=new_object, headers=headers,
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
            json=[new_object], headers=headers, params=params,
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
        (409, "already exists", None, False),
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
    physical_object_id = scenario_physical_object["physical_object_id"] if is_scenario_param else physical_object["physical_object_id"]
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
        (409, "already exists", None, False),
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
    physical_object_id = scenario_physical_object["physical_object_id"] if is_scenario_param else physical_object["physical_object_id"]
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
                json=new_object, headers=headers,
            )
            physical_object_id = response.json()["physical_object"]["physical_object_id"]
            response = await client.delete(
                f"/scenarios/{scenario_id}/physical_objects/{physical_object_id}",
                headers=headers, params=params,
            )
        elif not is_scenario_param:
            physical_object_id = physical_object["physical_object_id"]
            response = await client.delete(
                f"/scenarios/{scenario_id}/physical_objects/{physical_object_id}",
                headers=headers, params=params,
            )
        else:
            response = await client.delete(
                f"/scenarios/{scenario_id}/physical_objects/1",
                headers=headers, params=params,
            )

    # Assert
    assert_response(response, expected_status, OkResponse, error_message)
