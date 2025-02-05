"""All fixtures for services objects tests are defined here."""

import pytest

from idu_api.urban_api.schemas import (
    ScenarioServicePost,
    ServicePatch,
    ServicePost,
    ServicePut,
)

__all__ = [
    "service_patch_req",
    "service_post_req",
    "service_put_req",
    "scenario_service_post_req",
]


@pytest.fixture
def service_post_req() -> ServicePost:
    """POST request template for services data."""

    return ServicePost(
        physical_object_id=1,
        object_geometry_id=1,
        service_type_id=1,
        territory_type_id=1,
        name="Test Service",
        capacity_real=100,
        properties={},
    )


@pytest.fixture
def scenario_service_post_req() -> ScenarioServicePost:
    """POST request template for scenario services data."""

    return ScenarioServicePost(
        physical_object_id=1,
        is_scenario_physical_object=True,
        object_geometry_id=1,
        is_scenario_geometry=True,
        service_type_id=1,
        territory_type_id=1,
        name="Test Service",
        capacity_real=100,
        properties={},
    )


@pytest.fixture
def service_put_req() -> ServicePut:
    """PUT request template for services data."""

    return ServicePut(
        service_type_id=1,
        territory_type_id=1,
        name="Updated Service",
        capacity_real=100,
        properties={"updated_key": "updated_value"},
    )


@pytest.fixture
def service_patch_req() -> ServicePatch:
    """PATCH request template for services data."""

    return ServicePatch(
        service_type_id=1,
        territory_type_id=1,
        name="Patched Service",
        capacity_real=100,
        properties={"patched_key": "patched_value"},
    )
