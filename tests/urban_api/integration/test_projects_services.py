"""Integration tests for project-related services are defined here."""

from typing import Any

import httpx
import pytest
from pydantic import ValidationError

from idu_api.urban_api.schemas import (
    OkResponse,
    ScenarioService,
    ScenarioServicePost,
    ScenarioServiceWithGeometryAttributes,
    ScenarioUrbanObject,
    Service,
    ServicePut,
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
        (400, "please, choose either service_type_id or urban_function_id", None),
        (403, "denied", None),
        (404, "not found", 1e9),
    ],
    ids=["success", "bad_request", "forbidden", "not_found"],
)
async def test_get_services_by_scenario_id(
    urban_api_host: str,
    scenario: dict[str, Any],
    scenario_service: dict[str, Any],
    valid_token: str,
    superuser_token: str,
    expected_status: int,
    error_message: str | None,
    scenario_id_param: int | None,
):
    """Test GET /scenarios/{scenario_id}/services method."""

    # Arrange
    scenario_id = scenario_id_param or scenario["scenario_id"]
    headers = {"Authorization": f"Bearer {valid_token if expected_status == 403 else superuser_token}"}
    params = {"service_type_id": scenario_service["service_type"]["service_type_id"]}
    if expected_status == 400:
        params["urban_function_id"] = scenario_service["service_type"]["urban_function"]["id"]

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get(f"/scenarios/{scenario_id}/services", headers=headers, params=params)
        result = response.json()

    # Assert
    if expected_status == 200:
        assert_response(response, expected_status, ScenarioService, error_message, result_type="list")
        assert any(
            scenario_service["service_id"] == item["service_id"] for item in result
        ), "Response should contain created service."
    else:
        assert_response(response, expected_status, ScenarioService, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, scenario_id_param, is_regional_param",
    [
        (200, None, None, False),
        (200, None, None, True),
        (400, "please, choose either service_type_id or urban_function_id", None, False),
        (403, "denied", None, False),
        (404, "not found", 1e9, False),
    ],
    ids=["success_common", "success_regional", "bad_request", "forbidden", "not_found"],
)
async def test_get_services_with_geometry_by_scenario_id(
    urban_api_host: str,
    scenario: dict[str, Any],
    regional_scenario: dict[str, Any],
    scenario_service: dict[str, Any],
    valid_token: str,
    superuser_token: str,
    expected_status: int,
    error_message: str | None,
    scenario_id_param: int | None,
    is_regional_param: bool,
):
    """Test GET /scenarios/{scenario_id}/services method."""

    # Arrange
    scenario_id = scenario_id_param or (
        regional_scenario["scenario_id"] if is_regional_param else scenario["scenario_id"]
    )
    headers = {"Authorization": f"Bearer {valid_token if expected_status == 403 else superuser_token}"}
    params = {"service_type_id": scenario_service["service_type"]["service_type_id"]}
    if expected_status == 400 and not is_regional_param:
        params["urban_function_id"] = scenario_service["service_type"]["urban_function"]["id"]

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get(f"/scenarios/{scenario_id}/services_with_geometry", headers=headers, params=params)
        result = response.json()

    # Assert
    assert_response(response, expected_status, GeoJSONResponse, error_message)
    if response.status_code == 200:
        assert len(result["features"]) > 0, "Response should contain at least one feature."
        try:
            ScenarioServiceWithGeometryAttributes(**result["features"][0]["properties"])
        except ValidationError as e:
            pytest.fail(f"Pydantic validation error: {str(e)}")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, scenario_id_param, is_regional_param",
    [
        (200, None, None, False),
        (400, "please, choose either service_type_id or urban_function_id", None, False),
        (400, "this method cannot be accessed in a regional scenario", None, True),
        (403, "denied", None, False),
        (404, "not found", 1e9, False),
    ],
    ids=["success", "bad_request", "regional_scenario", "forbidden", "not_found"],
)
async def test_get_context_services(
    urban_api_host: str,
    scenario: dict[str, Any],
    regional_scenario: dict[str, Any],
    service: dict[str, Any],
    valid_token: str,
    superuser_token: str,
    expected_status: int,
    error_message: str | None,
    scenario_id_param: int | None,
    is_regional_param: bool,
):
    """Test GET /scenarios/{scenario_id}/context/services method."""

    # Arrange
    scenario_id = scenario_id_param or (
        regional_scenario["scenario_id"] if is_regional_param else scenario["scenario_id"]
    )
    headers = {"Authorization": f"Bearer {valid_token if expected_status == 403 else superuser_token}"}
    params = {"service_type_id": service["service_type"]["service_type_id"]}
    if expected_status == 400 and not is_regional_param:
        params["urban_function_id"] = service["service_type"]["urban_function"]["id"]

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get(f"/scenarios/{scenario_id}/context/services", headers=headers, params=params)
        result = response.json()

    # Assert
    if expected_status == 200:
        assert_response(response, expected_status, Service, error_message, result_type="list")
        assert any(
            service["service_id"] == item["service_id"] for item in result
        ), "Response should contain created service."
    else:
        assert_response(response, expected_status, Service, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, scenario_id_param, is_regional_param",
    [
        (200, None, None, False),
        (400, "please, choose either service_type_id or urban_function_id", None, False),
        (400, "this method cannot be accessed in a regional scenario", None, True),
        (403, "denied", None, False),
        (404, "not found", 1e9, False),
    ],
    ids=["success", "bad_request", "regional_scenario", "forbidden", "not_found"],
)
async def test_get_context_services_with_geometry(
    urban_api_host: str,
    scenario: dict[str, Any],
    regional_scenario: dict[str, Any],
    service: dict[str, Any],
    valid_token: str,
    superuser_token: str,
    expected_status: int,
    error_message: str | None,
    scenario_id_param: int | None,
    is_regional_param: bool,
):
    """Test GET /scenarios/{scenario_id}/context/services_with_geometry method."""

    # Arrange
    scenario_id = scenario_id_param or (
        regional_scenario["scenario_id"] if is_regional_param else scenario["scenario_id"]
    )
    headers = {"Authorization": f"Bearer {valid_token if expected_status == 403 else superuser_token}"}
    params = {"service_type_id": service["service_type"]["service_type_id"]}
    if expected_status == 400 and not is_regional_param:
        params["urban_function_id"] = service["service_type"]["urban_function"]["id"]

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get(
            f"/scenarios/{scenario_id}/context/services_with_geometry", headers=headers, params=params
        )
        result = response.json()

    # Assert
    assert_response(response, expected_status, GeoJSONResponse, error_message)
    if response.status_code == 200:
        assert len(result["features"]) > 0, "Response should contain at least one feature."
        try:
            Service(**result["features"][0]["properties"])
        except ValidationError as e:
            pytest.fail(f"Pydantic validation error: {str(e)}")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, scenario_id_param, is_scenario_param",
    [
        (201, None, None, True),
        (201, None, None, False),
        (403, "denied", None, True),
        (404, "not found", 1e9, True),
    ],
    ids=["success_1", "success_2", "forbidden", "not_found"],
)
async def test_add_service(
    urban_api_host: str,
    scenario_service_post_req: ScenarioServicePost,
    scenario: dict[str, Any],
    scenario_service: dict[str, Any],
    scenario_geometry: dict[str, Any],
    scenario_physical_object: dict[str, Any],
    object_geometry: dict[str, Any],
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
    """Test POST /scenarios/{scenario_id}/services method."""

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
    new_service = scenario_service_post_req.model_dump()
    new_service["object_geometry_id"] = (
        scenario_geometry["object_geometry_id"] if is_scenario_param else object_geometry["object_geometry_id"]
    )
    new_service["is_scenario_geometry"] = is_scenario_param
    new_service["physical_object_id"] = (
        scenario_physical_object["physical_object_id"] if is_scenario_param else physical_object["physical_object_id"]
    )
    new_service["is_scenario_physical_object"] = is_scenario_param
    new_service["service_type_id"] = scenario_service["service_type"]["service_type_id"]
    new_service["territory_type_id"] = scenario_service["territory_type"]["territory_type_id"]
    headers = {"Authorization": f"Bearer {valid_token if expected_status == 403 else superuser_token}"}

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.post(
            f"/scenarios/{scenario_id}/services",
            json=new_service,
            headers=headers,
        )

    # Assert
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
async def test_put_scenario_service(
    urban_api_host: str,
    service_put_req: ServicePut,
    scenario: dict[str, Any],
    scenario_service: dict[str, Any],
    service: dict[str, Any],
    valid_token: str,
    superuser_token: str,
    expected_status: int,
    error_message: str | None,
    scenario_id_param: int | None,
    is_scenario_param: bool,
):
    """Test PUT /scenarios/{scenario_id}/services method."""

    # Arrange
    scenario_id = scenario_id_param or scenario["scenario_id"]
    service_id = scenario_service["service_id"] if is_scenario_param else service["service_id"]
    new_service = service_put_req.model_dump()
    new_service["service_type_id"] = scenario_service["service_type"]["service_type_id"]
    new_service["territory_type_id"] = scenario_service["territory_type"]["territory_type_id"]
    headers = {"Authorization": f"Bearer {valid_token if expected_status == 403 else superuser_token}"}
    params = {"is_scenario_object": is_scenario_param}

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.put(
            f"/scenarios/{scenario_id}/services/{service_id}",
            json=new_service,
            headers=headers,
            params=params,
        )

    # Assert
    assert_response(response, expected_status, ScenarioService, error_message)


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
async def test_patch_scenario_service(
    urban_api_host: str,
    service_put_req: ServicePut,
    scenario: dict[str, Any],
    scenario_service: dict[str, Any],
    service: dict[str, Any],
    project: dict[str, Any],
    functional_zone_type: dict[str, Any],
    valid_token: str,
    superuser_token: str,
    expected_status: int,
    error_message: str | None,
    scenario_id_param: int | None,
    is_scenario_param: bool,
):
    """Test PATCH /scenarios/{scenario_id}/services method."""

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
    service_id = scenario_service["service_id"] if is_scenario_param else service["service_id"]
    new_service = service_put_req.model_dump()
    new_service["service_type_id"] = scenario_service["service_type"]["service_type_id"]
    new_service["territory_type_id"] = scenario_service["territory_type"]["territory_type_id"]
    headers = {"Authorization": f"Bearer {valid_token if expected_status == 403 else superuser_token}"}
    params = {"is_scenario_object": is_scenario_param}

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.patch(
            f"/scenarios/{scenario_id}/services/{service_id}",
            json=new_service,
            headers=headers,
            params=params,
        )

    # Assert
    assert_response(response, expected_status, ScenarioService, error_message)


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
async def test_delete_service(
    urban_api_host: str,
    scenario_service_post_req: ScenarioServicePost,
    scenario: dict[str, Any],
    scenario_service: dict[str, Any],
    scenario_physical_object: dict[str, Any],
    scenario_geometry: dict[str, Any],
    service: dict[str, Any],
    project: dict[str, Any],
    functional_zone_type: dict[str, Any],
    valid_token: str,
    superuser_token: str,
    expected_status: int,
    error_message: str | None,
    scenario_id_param: int | None,
    is_scenario_param: bool,
):
    """Test DELETE /scenarios/{scenario_id}/services method."""

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
    new_service = scenario_service_post_req.model_dump()
    new_service["object_geometry_id"] = scenario_geometry["object_geometry_id"]
    new_service["is_scenario_geometry"] = True
    new_service["physical_object_id"] = scenario_physical_object["physical_object_id"]
    new_service["is_scenario_physical_object"] = True
    new_service["service_type_id"] = scenario_service["service_type"]["service_type_id"]
    new_service["territory_type_id"] = scenario_service["territory_type"]["territory_type_id"]
    headers = {"Authorization": f"Bearer {valid_token if expected_status == 403 else superuser_token}"}
    params = {"is_scenario_object": is_scenario_param}

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        if expected_status == 200 and is_scenario_param:
            response = await client.post(
                f"scenarios/{scenario_id}/services",
                json=new_service,
                headers=headers,
            )
            service_id = response.json()["service"]["service_id"]
            response = await client.delete(
                f"/scenarios/{scenario_id}/services/{service_id}",
                headers=headers,
                params=params,
            )
        elif not is_scenario_param:
            service_id = service["service_id"]
            response = await client.delete(
                f"/scenarios/{scenario_id}/services/{service_id}",
                headers=headers,
                params=params,
            )
        else:
            response = await client.delete(
                f"/scenarios/{scenario_id}/services/1",
                headers=headers,
                params=params,
            )

    # Assert
    assert_response(response, expected_status, OkResponse, error_message)
