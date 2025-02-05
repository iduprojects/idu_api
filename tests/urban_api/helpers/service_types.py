"""All fixtures for service types tests are defined here."""

import pytest

from idu_api.urban_api.schemas import (
    ServiceType,
    ServiceTypePatch,
    ServiceTypePost,
    ServiceTypePut,
    ServiceTypesHierarchy,
    UrbanFunction,
    UrbanFunctionPatch,
    UrbanFunctionPost,
    UrbanFunctionPut,
)
from idu_api.urban_api.schemas.short_models import UrbanFunctionBasic

__all__ = [
    "service_type_patch_req",
    "service_type_post_req",
    "service_type_put_req",
    "urban_function_req",
    "urban_function_patch_req",
    "urban_function_post_req",
    "urban_function_put_req",
]


@pytest.fixture
def service_type_req() -> ServiceType:
    """Request template for service types data."""

    return ServiceType(
        service_type_id=1,
        name="Test Type",
        urban_function=UrbanFunctionBasic(
            id=1,
            name="Test Function",
        ),
    )


@pytest.fixture
def service_type_post_req() -> ServiceTypePost:
    """POST request template for service types data."""

    return ServiceTypePost(
        name="Test Type",
        urban_function_id=1,
        capacity_modeled=100,
        code="1",
        infrastructure_type="basic",
        properties={},
    )


@pytest.fixture
def service_type_put_req() -> ServiceTypePatch:
    """PATCH request template for service types data."""

    return ServiceTypePut(
        name="Updated Type",
        urban_function_id=1,
        capacity_modeled=100,
        code="1",
        infrastructure_type="basic",
        properties={},
    )


@pytest.fixture
def service_type_patch_req() -> ServiceTypePatch:
    """PATCH request template for service types data."""

    return ServiceTypePatch(
        name="Updated Type",
        urban_function_id=1,
    )


@pytest.fixture
def urban_function_req() -> UrbanFunction:
    """Request template for urban functions data."""

    return UrbanFunction(
        urban_function_id=1,
        parent_urban_function=UrbanFunctionBasic(
            id=1,
            name="Parent Function",
        ),
        name="Test Function",
        level=1,
        list_label="1.1.1",
        code="1",
    )


@pytest.fixture
def urban_function_post_req() -> UrbanFunctionPost:
    """POST request template for urban functions data."""

    return UrbanFunctionPost(
        name="Test Function",
        parent_id=1,
        code="1",
    )


@pytest.fixture
def urban_function_put_req() -> UrbanFunctionPut:
    """PUT request template for urban functions data."""

    return UrbanFunctionPut(
        name="Updated Test Function",
        parent_id=1,
        code="1",
    )


@pytest.fixture
def urban_function_patch_req() -> UrbanFunctionPatch:
    """PATCH request template for urban functions data."""

    return UrbanFunctionPatch(
        name="Patched Test Function",
        parent_id=1,
    )


@pytest.fixture
def services_types_hierarchy_req() -> ServiceTypesHierarchy:
    """Request template for service types hierarchy data."""

    return ServiceTypesHierarchy(
        urban_function_id=1,
        parent_id=1,
        name="Test Function",
        level=1,
        list_label="1.1.1",
        code="1",
        children=[
            ServiceType(
                service_type_id=1,
                name="Test Type",
                urban_function=UrbanFunctionBasic(
                    id=1,
                    name="Test Function",
                ),
            ),
        ],
    )
