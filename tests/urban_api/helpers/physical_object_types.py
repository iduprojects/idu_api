"""All fixtures for physical object types tests are defined here."""

import pytest

from idu_api.urban_api.schemas import (
    PhysicalObjectFunction,
    PhysicalObjectFunctionPatch,
    PhysicalObjectFunctionPost,
    PhysicalObjectFunctionPut,
    PhysicalObjectsTypesHierarchy,
    PhysicalObjectType,
    PhysicalObjectTypePatch,
    PhysicalObjectTypePost,
)
from idu_api.urban_api.schemas.short_models import PhysicalObjectFunctionBasic

__all__ = [
    "physical_object_type_req",
    "physical_object_type_patch_req",
    "physical_object_type_post_req",
    "physical_objects_types_hierarchy_req",
    "physical_object_function_req",
    "physical_object_function_patch_req",
    "physical_object_function_post_req",
    "physical_object_function_put_req",
]


@pytest.fixture
def physical_object_type_req() -> PhysicalObjectType:
    """Request template for physical object type data."""

    return PhysicalObjectType(
        physical_object_type_id=1,
        name="Test Type",
        physical_object_function=PhysicalObjectFunctionBasic(
            id=1,
            name="Test Function",
        ),
    )


@pytest.fixture
def physical_object_type_post_req() -> PhysicalObjectTypePost:
    """POST request template for physical object type data."""

    return PhysicalObjectTypePost(
        name="Test Type",
        physical_object_function_id=1,
    )


@pytest.fixture
def physical_object_type_patch_req() -> PhysicalObjectTypePatch:
    """PATCH request template for physical object type data."""

    return PhysicalObjectTypePatch(
        name="Updated Type",
        physical_object_function_id=1,
    )


@pytest.fixture
def physical_object_function_req() -> PhysicalObjectFunction:
    """Request template for physical object function data."""

    return PhysicalObjectFunction(
        physical_object_function_id=1,
        parent_physical_object_function=PhysicalObjectFunctionBasic(
            id=1,
            name="Parent Function",
        ),
        name="Test Function",
        level=1,
        list_label="1.1.1",
        code="1",
    )


@pytest.fixture
def physical_object_function_post_req() -> PhysicalObjectFunctionPost:
    """POST request template for physical object function data."""

    return PhysicalObjectFunctionPost(
        name="Test Function",
        parent_id=1,
        code="1",
    )


@pytest.fixture
def physical_object_function_put_req() -> PhysicalObjectFunctionPut:
    """PUT request template for physical object function data."""

    return PhysicalObjectFunctionPut(
        name="Updated Test Function",
        parent_id=1,
        code="1",
    )


@pytest.fixture
def physical_object_function_patch_req() -> PhysicalObjectFunctionPatch:
    """PATCH request template for physical object function data."""

    return PhysicalObjectFunctionPatch(
        name="Patched Test Function",
        parent_id=1,
    )


@pytest.fixture
def physical_objects_types_hierarchy_req() -> PhysicalObjectsTypesHierarchy:
    """Request template for physical objects types hierarchy data."""

    return PhysicalObjectsTypesHierarchy(
        physical_object_function_id=1,
        parent_id=1,
        name="Test Function",
        level=1,
        list_label="1.1.1",
        code="1",
        children=[
            PhysicalObjectType(
                physical_object_type_id=1,
                name="Test Type",
                physical_object_function=PhysicalObjectFunctionBasic(
                    id=1,
                    name="Test Function",
                ),
            ),
        ],
    )
