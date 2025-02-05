"""All fixtures for functional zones tests are defined here."""

from datetime import date

import pytest

from idu_api.urban_api.schemas import (
    IndicatorsGroupPost,
    IndicatorsPatch,
    IndicatorsPost,
    IndicatorsPut,
    IndicatorValuePost,
    IndicatorValuePut,
    MeasurementUnitPost,
    ScenarioIndicatorValuePatch,
    ScenarioIndicatorValuePost,
    ScenarioIndicatorValuePut,
)

__all__ = [
    "measurement_unit_post_req",
    "indicators_group_post_req",
    "indicators_patch_req",
    "indicators_post_req",
    "indicators_put_req",
    "indicator_value_post_req",
    "indicator_value_put_req",
    "scenario_indicator_value_patch_req",
    "scenario_indicator_value_post_req",
    "scenario_indicator_value_put_req",
]


@pytest.fixture
def measurement_unit_post_req() -> MeasurementUnitPost:
    """POST request template for measurement unit data."""

    return MeasurementUnitPost(
        name="Test Measurement Unit",
    )


@pytest.fixture
def indicators_group_post_req() -> IndicatorsGroupPost:
    """POST request template for indicators group data."""

    return IndicatorsGroupPost(name="Test Indicators Group", indicators_ids=[1])


@pytest.fixture
def indicators_post_req() -> IndicatorsPost:
    """POST request template for indicator data."""

    return IndicatorsPost(
        name_full="Test Indicator Full Name",
        name_short="Test Indicator Short Name",
        measurement_unit_id=1,
        parent_id=2,
    )


@pytest.fixture
def indicators_put_req() -> IndicatorsPut:
    """PUT request template for indicator data."""

    return IndicatorsPut(
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
        date_type="year",
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
        date_type="year",
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
