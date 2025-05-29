"""Integration tests for indicators objects are defined here."""

import asyncio
from typing import Any

import httpx
import pytest
from otteroad import KafkaConsumerService
from otteroad.models import IndicatorValuesUpdated

from idu_api.urban_api.schemas import (
    Indicator,
    IndicatorPost,
    IndicatorPut,
    IndicatorsGroup,
    IndicatorValue,
    IndicatorValuePost,
    IndicatorValuePut,
    MeasurementUnit,
    MeasurementUnitPost,
    OkResponse,
)
from tests.urban_api.helpers.broker import mock_handler
from tests.urban_api.helpers.utils import assert_response

####################################################################################
#                           Default use-case tests                                 #
####################################################################################


@pytest.mark.asyncio
async def test_get_measurement_units(urban_api_host: str, measurement_unit: dict[str, Any]):
    """Test GET /measurement_units method."""

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get("/measurement_units")

    # Assert
    assert_response(response, 200, MeasurementUnit, result_type="list")
    for res in response.json():
        for k, v in measurement_unit.items():
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
async def test_add_measurement_units(
    urban_api_host: str,
    measurement_unit_post_req: MeasurementUnitPost,
    expected_status: int,
    error_message: str | None,
):
    """Test POST /measurement_units method."""

    # Arrange
    new_unit = measurement_unit_post_req.model_dump()
    new_unit["name"] = "new_name"

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.post("/measurement_units", json=new_unit)

    # Assert
    assert_response(response, expected_status, MeasurementUnit, error_message)


@pytest.mark.asyncio
async def test_get_indicators_groups(urban_api_host: str, indicators_group: dict[str, Any]):
    """Test GET /indicators_groups method."""

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get("/indicators_groups")

    # Assert
    assert_response(response, 200, IndicatorsGroup, result_type="list")
    for res in response.json():
        for k, v in indicators_group.items():
            if k in res:
                assert res[k] == v, f"Mismatch for {k}: {res[k]} != {v}."


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, indicators_ids_param",
    [
        (201, None, None),
        (404, "not found", [1e9]),
        (409, "already exists", None),
    ],
    ids=["success", "not_found", "conflict"],
)
async def test_add_indicators_groups(
    urban_api_host: str,
    indicator: dict[str, any],
    expected_status: int,
    error_message: str | None,
    indicators_ids_param: list[int] | None,
):
    """Test POST /indicators_groups method."""

    # Arrange
    new_group = {
        "name": "new group name",
        "indicators_ids": indicators_ids_param or [indicator["indicator_id"]],
    }

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.post("/indicators_groups", json=new_group)

    # Assert
    assert_response(response, expected_status, IndicatorsGroup, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, indicators_ids",
    [
        (200, None, None),
        (404, "not found", [1e9]),
    ],
    ids=["success", "not_found"],
)
async def test_update_indicators_group(
    urban_api_host: str,
    indicator: dict[str, any],
    expected_status: int,
    error_message: str | None,
    indicators_ids: list[int],
):
    """Test PUT /indicators_groups method."""

    # Arrange
    new_group = {
        "name": "updated indicators group",
        "indicators_ids": [indicator["indicator_id"]] if indicators_ids is None else indicators_ids,
    }

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.put("/indicators_groups", json=new_group)

    # Assert
    assert_response(response, expected_status, IndicatorsGroup, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, group_id",
    [
        (200, None, None),
        (404, "not found", 1e9),
    ],
    ids=["success", "not_found"],
)
async def test_get_indicators_by_group_id(
    urban_api_host,
    indicators_group,
    indicator: dict[str, any],
    expected_status: int,
    error_message: str | None,
    group_id: int | None,
):
    """Test GET /indicators_groups/{indicators_group_id} method."""

    # Arrange
    indicators_group_id = group_id if group_id is not None else indicators_group["indicators_group_id"]

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get(f"/indicators_groups/{indicators_group_id}")

    # Assert
    if response.status_code == 200:
        assert_response(response, expected_status, Indicator, result_type="list")
        for res in response.json():
            for k, v in indicator.items():
                if k in res:
                    assert res[k] == v, f"Mismatch for {k}: {res[k]} != {v}."
    else:
        assert_response(response, expected_status, Indicator, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, parent_id_param, parent_name_param, type_id_param",
    [
        (200, None, None, None, None),
        (400, "Please, choose either parent_id or parent_name", 1, "test", None),
        (404, "not found", 1e9, None, None),
    ],
    ids=["success", "bad_request", "not_found"],
)
async def test_get_indicators_by_parent(
    urban_api_host: str,
    indicator: dict[str, Any],
    region: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    parent_id_param: int | None,
    parent_name_param: str | None,
    type_id_param: int | None,
):
    """Test GET /indicators_by_parent method."""

    # Arrange
    params = {
        "get_all_subtree": True,
        "name": "test",
        "territory_id": region["territory_id"],
    }
    if parent_id_param is not None:
        params["parent_id"] = parent_id_param
    if parent_name_param is not None:
        params["parent_name"] = parent_name_param
    if type_id_param is not None:
        params["physical_object_type_id"] = type_id_param
        params["service_type_id"] = type_id_param

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get("/indicators_by_parent", params=params)

    # Assert
    if response.status_code == 200:
        assert_response(response, expected_status, Indicator, result_type="list")
        for res in response.json():
            for k, v in indicator.items():
                if k in res:
                    assert res[k] == v, f"Mismatch for {k}: {res[k]} != {v}."
    else:
        assert_response(response, expected_status, IndicatorsGroup, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, indicator_id_param",
    [
        (200, None, None),
        (404, "not found", 1e9),
    ],
    ids=["success", "not_found"],
)
async def test_get_indicator_by_id(
    urban_api_host: str,
    indicator: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    indicator_id_param: int | None,
):
    """Test GET /indicators method."""

    # Arrange
    indicator_id = indicator["indicator_id"] if indicator_id_param is None else indicator_id_param

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get(f"/indicators/{indicator_id}")

    # Assert
    assert_response(response, expected_status, Indicator, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, measurement_unit_id_param, parent_id_param",
    [
        (201, None, None, None),
        (404, "not found", 1e9, 1e9),
        (409, "already exists", None, None),
    ],
    ids=["success", "not_found", "conflict"],
)
async def test_add_indicator(
    urban_api_host: str,
    indicators_post_req: IndicatorPost,
    measurement_unit: dict[str, Any],
    service_type: dict[str, Any],
    physical_object_type: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    measurement_unit_id_param: int | None,
    parent_id_param: int | None,
):
    """Test POST /indicators method."""

    # Arrange
    new_indicator = indicators_post_req.model_dump()
    new_indicator["name_full"] = "new name"
    new_indicator["measurement_unit_id"] = measurement_unit_id_param or measurement_unit["measurement_unit_id"]
    new_indicator["parent_id"] = parent_id_param
    new_indicator["service_type_id"] = service_type["service_type_id"]
    new_indicator["physical_object_type_id"] = physical_object_type["physical_object_type_id"]

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.post("/indicators", json=new_indicator)

    # Assert
    assert_response(response, expected_status, Indicator, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, measurement_unit_id_param, parent_id_param",
    [
        (200, None, None, None),
        (404, "not found", 1e9, 1e9),
    ],
    ids=["success", "not_found"],
)
async def test_put_indicator(
    urban_api_host: str,
    indicators_put_req: IndicatorPut,
    measurement_unit: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    measurement_unit_id_param: int | None,
    parent_id_param: int | None,
):
    """Test PUT /indicators method."""

    # Arrange
    new_indicator = indicators_put_req.model_dump()
    new_indicator["name_full"] = "new name"
    new_indicator["measurement_unit_id"] = measurement_unit_id_param or measurement_unit["measurement_unit_id"]
    new_indicator["parent_id"] = parent_id_param
    new_indicator["service_type_id"] = None
    new_indicator["physical_object_type_id"] = None

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.put("/indicators", json=new_indicator)

    # Assert
    assert_response(response, expected_status, Indicator, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, name_full_param, indicator_id_param",
    [
        (200, None, "updated indicator", None),
        (404, "not found", "updated indicator", 1e9),
        (409, "already exists", "new name", None),
    ],
    ids=["success", "not_found", "conflict"],
)
async def test_patch_indicator(
    urban_api_host: str,
    indicator: dict[str, any],
    expected_status: int,
    error_message: str | None,
    name_full_param: str,
    indicator_id_param: int | None,
):
    """Test PATCH /indicators method."""

    # Arrange
    new_indicator = {k: v for k, v in indicator.items() if k != "indicator_id"}
    new_indicator["name_full"] = name_full_param
    indicator_id = indicator_id_param or indicator["indicator_id"]

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.patch(f"/indicators/{indicator_id}", json=new_indicator)

    # Assert
    assert_response(response, expected_status, Indicator, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, indicator_id_param",
    [
        (200, None, None),
        (404, "not found", 1e9),
    ],
    ids=["success", "not_found"],
)
async def test_delete_indicator(
    urban_api_host: str,
    indicators_post_req: IndicatorPost,
    expected_status: int,
    error_message: str | None,
    indicator_id_param: int | None,
):
    """Test DELETE /indicators/{indicator_id} method."""

    # Arrange
    new_indicator = indicators_post_req.model_dump()
    new_indicator["name_full"] = "indicator for deletion"
    new_indicator["measurement_unit_id"] = None
    new_indicator["parent_id"] = None
    new_indicator["service_type_id"] = None
    new_indicator["physical_object_type_id"] = None

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        if indicator_id_param is None:
            response = await client.post("/indicators", json=new_indicator)
            response = await client.delete(f"/indicators/{response.json()['indicator_id']}")
        else:
            response = await client.delete(f"/indicators/{indicator_id_param}")

    # Assert
    assert_response(response, expected_status, OkResponse, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, indicator_value_id_param",
    [
        (200, None, None),
        (404, "not found", 1e9),
    ],
    ids=["success", "not_found"],
)
async def test_get_indicator_value_by_id(
    urban_api_host: str,
    indicator_value: dict[str, any],
    expected_status: int,
    error_message: str | None,
    indicator_value_id_param: int | None,
):
    """Test GET /indicator_value method."""

    # Arrange
    indicator_value_id = indicator_value_id_param or indicator_value["indicator_value_id"]

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get(f"/indicator_value/{indicator_value_id}")
        result = response.json()

    # Assert
    assert_response(response, expected_status, IndicatorValue, error_message)
    if response.status_code == 200:
        for k, v in indicator_value.items():
            assert result[k] == v, f"Mismatch for {k}: {result[k]} != {v}."


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, indicator_id_param, territory_id_param",
    [
        (201, None, None, None),
        (404, "not found", 1e9, 1e9),
        (409, "already exists", None, None),
    ],
    ids=["success", "not_found", "conflict"],
)
async def test_add_indicator_value(
    urban_api_host: str,
    indicator_value_post_req: IndicatorValuePost,
    indicator: dict[str, Any],
    country: dict[str, Any],
    kafka_consumer: KafkaConsumerService,
    expected_status: int,
    error_message: str | None,
    indicator_id_param: int | None,
    territory_id_param: int | None,
):
    """Test POST /indicator_value method."""

    # Arrange
    new_handler = mock_handler(IndicatorValuesUpdated)
    kafka_consumer.register_handler(new_handler)
    new_indicator_value = indicator_value_post_req.model_dump()
    new_indicator_value["indicator_id"] = indicator_id_param or indicator["indicator_id"]
    new_indicator_value["territory_id"] = territory_id_param or country["territory_id"]
    new_indicator_value["date_value"] = str(new_indicator_value["date_value"])

    # Act
    if expected_status == 201:
        await asyncio.sleep(5)
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.post("/indicator_value", json=new_indicator_value)
    if expected_status == 201:
        await asyncio.sleep(5)

    # Assert
    assert_response(response, expected_status, IndicatorValue, error_message)
    if expected_status == 201:
        assert len(new_handler.received_events) == 1, "No one event was received"
        assert isinstance(
            new_handler.received_events[0], IndicatorValuesUpdated
        ), "Received event is not IndicatorValuesUpdated"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, indicator_id_param, territory_id_param",
    [
        (200, None, None, None),
        (404, "not found", 1e9, 1e9),
    ],
    ids=["success", "not_found"],
)
async def test_put_indicator_value(
    urban_api_host: str,
    indicator_value_put_req: IndicatorValuePut,
    indicator: dict[str, Any],
    country: dict[str, Any],
    kafka_consumer: KafkaConsumerService,
    expected_status: int,
    error_message: str | None,
    indicator_id_param: int | None,
    territory_id_param: int | None,
):
    """Test PUT /indicator_value method."""

    # Arrange
    new_handler = mock_handler(IndicatorValuesUpdated)
    kafka_consumer.register_handler(new_handler)
    new_indicator_value = indicator_value_put_req.model_dump()
    new_indicator_value["indicator_id"] = indicator_id_param or indicator["indicator_id"]
    new_indicator_value["territory_id"] = territory_id_param or country["territory_id"]
    new_indicator_value["date_value"] = str(new_indicator_value["date_value"])

    # Act
    if expected_status == 201:
        await asyncio.sleep(5)
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.put("/indicator_value", json=new_indicator_value)
    if expected_status == 201:
        await asyncio.sleep(5)

    # Assert
    assert_response(response, expected_status, IndicatorValue, error_message)
    if expected_status == 201:
        assert len(new_handler.received_events) == 1, "No one event was received"
        assert isinstance(
            new_handler.received_events[0], IndicatorValuesUpdated
        ), "Received event is not IndicatorValuesUpdated"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, indicator_value_id_param",
    [
        (200, None, None),
        (404, "not found", 1e9),
    ],
    ids=["success", "not_found"],
)
async def test_delete_indicator_value(
    urban_api_host: str,
    indicator_value_post_req: IndicatorValuePost,
    indicator: dict[str, Any],
    district: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    indicator_value_id_param: int | None,
):
    """Test DELETE /indicator_value method."""

    # Arrange
    new_indicator_value = indicator_value_post_req.model_dump()
    new_indicator_value["indicator_id"] = indicator["indicator_id"]
    new_indicator_value["territory_id"] = district["territory_id"]
    new_indicator_value["date_value"] = str(new_indicator_value["date_value"])

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        if indicator_value_id_param is None:
            response = await client.post("/indicator_value", json=new_indicator_value)
            indicator_value_id = response.json()["indicator_value_id"]
            response = await client.delete(f"/indicator_value/{indicator_value_id}")
        else:
            response = await client.delete(f"/indicator_value/{indicator_value_id_param}")

    # Assert
    assert_response(response, expected_status, OkResponse, error_message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_status, error_message, indicator_id_param",
    [
        (200, None, None),
        (404, "not found", 1e9),
    ],
    ids=["success", "not_found"],
)
async def test_get_indicator_values_by_id(
    urban_api_host: str,
    indicator: dict[str, Any],
    expected_status: int,
    error_message: str | None,
    indicator_id_param: int | None,
):
    """Test GET /indicator/{indicator_id}/values method."""

    # Arrange
    indicator_id = indicator_id_param or indicator["indicator_id"]

    # Act
    async with httpx.AsyncClient(base_url=f"{urban_api_host}/api/v1") as client:
        response = await client.get(f"/indicator/{indicator_id}/values")

    # Assert
    if response.status_code == 200:
        assert_response(response, expected_status, IndicatorValue, result_type="list")
        assert len(response.json()) > 0, "At least one indicator value was expected in result."
    else:
        assert_response(response, expected_status, IndicatorValue, error_message)
