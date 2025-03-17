"""All fixtures for functional zones tests are defined here."""

from datetime import date
from typing import Any

import httpx
import pytest

from idu_api.urban_api.schemas import (
    IndicatorPost,
    IndicatorPut,
    IndicatorsGroupPost,
    IndicatorsPatch,
    IndicatorValuePost,
    IndicatorValuePut,
    MeasurementUnitPost,
    ScenarioIndicatorValuePatch,
    ScenarioIndicatorValuePost,
    ScenarioIndicatorValuePut,
)

__all__ = [
    "indicator",
    "indicator_value",
    "indicators_group",
    "indicators_group_post_req",
    "indicators_patch_req",
    "indicators_post_req",
    "indicators_put_req",
    "indicator_value_post_req",
    "indicator_value_put_req",
    "measurement_unit",
    "measurement_unit_post_req",
    "scenario_indicator_value",
    "scenario_indicator_value_patch_req",
    "scenario_indicator_value_post_req",
    "scenario_indicator_value_put_req",
]


####################################################################################
#                        Integration tests helpers                                 #
####################################################################################


@pytest.fixture(scope="session")
def measurement_unit(urban_api_host) -> dict[str, Any]:
    """Returns created measurement unit."""
    measurement_unit_post_req = MeasurementUnitPost(name="Test Measurement Unit")

    with httpx.Client(base_url=f"{urban_api_host}/api/v1") as client:
        response = client.post("/measurement_units", json=measurement_unit_post_req.model_dump())

    assert response.status_code == 201, f"Invalid status code was returned: {response.status_code}."
    return response.json()


@pytest.fixture(scope="session")
def indicator(urban_api_host, measurement_unit) -> dict[str, Any]:
    """Returns created indicator."""
    indicators_post_req = IndicatorPost(
        name_full="Test Indicator Full Name",
        name_short="Test Indicator Short Name",
        measurement_unit_id=measurement_unit["measurement_unit_id"],
        parent_id=None,
    )

    with httpx.Client(base_url=f"{urban_api_host}/api/v1") as client:
        response = client.post("/indicators", json=indicators_post_req.model_dump())

    assert response.status_code == 201, f"Invalid status code was returned: {response.status_code}."
    return response.json()


@pytest.fixture(scope="session")
def indicators_group(urban_api_host, indicator) -> dict[str, Any]:
    """Returns created indicators group."""
    indicators_group_post_req = IndicatorsGroupPost(
        name="Test Indicators Group", indicators_ids=[indicator["indicator_id"]]
    )

    with httpx.Client(base_url=f"{urban_api_host}/api/v1") as client:
        response = client.post("/indicators_groups", json=indicators_group_post_req.model_dump())

    assert response.status_code == 201, f"Invalid status code was returned: {response.status_code}."
    return response.json()


@pytest.fixture(scope="session")
def indicator_value(urban_api_host, indicator, region) -> dict[str, Any]:
    """Returns created indicator value."""
    indicator_value_post_req = {
        "indicator_id": indicator["indicator_id"],
        "territory_id": region["territory_id"],
        "date_type": "day",
        "date_value": str(date.today()),
        "value": 100.5,
        "value_type": "real",
        "information_source": "Test Source",
    }

    with httpx.Client(base_url=f"{urban_api_host}/api/v1") as client:
        response = client.post("/indicator_value", json=indicator_value_post_req)

    assert response.status_code == 201, f"Invalid status code was returned: {response.status_code}."
    return response.json()


@pytest.fixture(scope="session")
def scenario_indicator_value(urban_api_host, scenario, indicator, city, hexagon, superuser_token) -> dict[str, Any]:
    """Returns created indicator value."""
    scenario_id = scenario["scenario_id"]
    scenario_indicator_value_post_req = {
        "scenario_id": scenario_id,
        "indicator_id": indicator["indicator_id"],
        "territory_id": city["territory_id"],
        "hexagon_id": hexagon["hexagon_id"],
        "value": 100.5,
        "comment": "Test Comment",
        "information_source": "Test Source",
    }
    headers = {"Authorization": f"Bearer {superuser_token}"}

    with httpx.Client(base_url=f"{urban_api_host}/api/v1") as client:
        response = client.post(
            f"scenarios/{scenario_id}/indicators_values",
            json=scenario_indicator_value_post_req, headers=headers,
        )

    assert response.status_code == 201, f"Invalid status code was returned: {response.status_code}."
    return response.json()


####################################################################################
#                                 Models                                           #
####################################################################################


@pytest.fixture
def measurement_unit_post_req() -> MeasurementUnitPost:
    """POST request template for measurement unit data."""

    return MeasurementUnitPost(name="Test Measurement Unit")


@pytest.fixture
def indicators_group_post_req() -> IndicatorsGroupPost:
    """POST request template for indicators group data."""

    return IndicatorsGroupPost(name="Test Indicators Group", indicators_ids=[1])


@pytest.fixture
def indicators_post_req() -> IndicatorPost:
    """POST request template for indicator data."""

    return IndicatorPost(
        name_full="Test Indicator Full Name",
        name_short="Test Indicator Short Name",
        measurement_unit_id=1,
        parent_id=2,
    )


@pytest.fixture
def indicators_put_req() -> IndicatorPut:
    """PUT request template for indicator data."""

    return IndicatorPut(
        name_full="Updated Test Indicator Full Name",
        name_short="Updated Test Indicator Short Namе",
        measurement_unit_id=1,
        parent_id=2,
    )


@pytest.fixture
def indicators_patch_req() -> IndicatorsPatch:
    """PATCH request template for indicator data."""

    return IndicatorsPatch(
        name_full="New Patched Indicator Full Name",
        parent_id=1,
        measurement_unit_id=1,
    )


@pytest.fixture
def indicator_value_post_req() -> IndicatorValuePost:
    """POST request template for indicator value data."""

    return IndicatorValuePost(
        indicator_id=1,
        territory_id=1,
        date_type="day",
        date_value=date.today(),
        value=100.5,
        value_type="real",
        information_source="Test Source",
    )


@pytest.fixture
def indicator_value_put_req() -> IndicatorValuePut:
    """PUT request template for indicator value data."""

    return IndicatorValuePut(
        indicator_id=1,
        territory_id=1,
        date_type="day",
        date_value=date.today(),
        value=100.5,
        value_type="real",
        information_source="Test Source",
    )


@pytest.fixture
def scenario_indicator_value_post_req() -> IndicatorValuePost:
    """POST request template for scenario indicator value data."""

    return ScenarioIndicatorValuePost(
        indicator_id=1,
        scenario_id=1,
        territory_id=1,
        hexagon_id=1,
        value=100.5,
        comment="Test Comment",
        information_source="Test Source",
        properties={},
    )


@pytest.fixture
def scenario_indicator_value_put_req() -> IndicatorValuePut:
    """PUT request template for scenario indicator value data."""

    return ScenarioIndicatorValuePut(
        indicator_id=1,
        scenario_id=1,
        territory_id=1,
        hexagon_id=1,
        value=100.5,
        comment="Test Comment",
        information_source="Test Source",
        properties={},
    )


@pytest.fixture
def scenario_indicator_value_patch_req() -> IndicatorValuePut:
    """PATCH request template for scenario indicator value data."""

    return ScenarioIndicatorValuePatch(
        value=100.5,
    )
