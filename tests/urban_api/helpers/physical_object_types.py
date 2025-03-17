"""All fixtures for physical object types tests are defined here."""

from typing import Any

import httpx
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
    "physical_object_function",
    "physical_object_type",
    "physical_object_function_req",
    "physical_object_function_patch_req",
    "physical_object_function_post_req",
    "physical_object_function_put_req",
    "physical_object_type_req",
    "physical_object_type_patch_req",
    "physical_object_type_post_req",
    "physical_objects_types_hierarchy_req",
]

####################################################################################
#                        Integration tests helpers                                 #
####################################################################################


@pytest.fixture(scope="session")
def physical_object_function(urban_api_host) -> dict[str, Any]:
    """Returns created physical object function."""
    physical_object_function_post_req = PhysicalObjectFunctionPost(
        name="Test Function",
        parent_id=None,
        code="1",
    )

    with httpx.Client(base_url=f"{urban_api_host}/api/v1") as client:
        response = client.post("/physical_object_functions", json=physical_object_function_post_req.model_dump())

    assert response.status_code == 201, f"Invalid status code was returned: {response.status_code}."
    return response.json()


@pytest.fixture(scope="session")
def physical_object_type(urban_api_host, physical_object_function) -> dict[str, Any]:
    """Returns created physical object type."""
    physical_object_type_post_req = PhysicalObjectTypePost(
        name="Test Type",
        physical_object_function_id=physical_object_function["physical_object_function_id"],
    )

    with httpx.Client(base_url=f"{urban_api_host}/api/v1") as client:
        response = client.post("/physical_object_types", json=physical_object_type_post_req.model_dump())

    assert response.status_code == 201, f"Invalid status code was returned: {response.status_code}."
    return response.json()


####################################################################################
#                                 Models                                           #
####################################################################################


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
