"""All fixtures for buffers tests are defined here."""

from typing import Any

import httpx
import pytest

from idu_api.urban_api.schemas import (
    Buffer,
    BufferPut,
    BufferType,
    BufferTypePost,
    DefaultBufferValue,
    DefaultBufferValuePost,
    DefaultBufferValuePut,
    ScenarioBuffer,
    ScenarioBufferDelete,
    ScenarioBufferPut,
)
from idu_api.urban_api.schemas.geometries import Geometry
from idu_api.urban_api.schemas.short_models import (
    BufferTypeBasic,
    ObjectGeometryBasic,
    PhysicalObjectBasic,
    PhysicalObjectTypeBasic,
    ServiceBasic,
    ServiceTypeBasic,
    ShortTerritory,
    ShortUrbanObject,
)

__all__ = [
    "buffer",
    "buffer_req",
    "buffer_put_req",
    "buffer_type",
    "buffer_type_req",
    "buffer_type_post_req",
    "default_buffer_value",
    "default_buffer_value_req",
    "default_buffer_value_post_req",
    "default_buffer_value_put_req",
    "scenario_buffer",
    "scenario_buffer_delete_req",
    "scenario_buffer_req",
    "scenario_buffer_put_req",
]


####################################################################################
#                        Integration tests helpers                                 #
####################################################################################


@pytest.fixture(scope="session")
def buffer_type(urban_api_host) -> dict[str, Any]:
    """Returns created buffer type."""
    buffer_type_post_req = BufferTypePost(name="Test buffer type name")

    with httpx.Client(base_url=f"{urban_api_host}/api/v1") as client:
        response = client.post("/buffer_types", json=buffer_type_post_req.model_dump())

    assert response.status_code == 201, f"Invalid status code was returned: {response.status_code}."
    return response.json()


@pytest.fixture(scope="session")
def default_buffer_value(urban_api_host, buffer_type, physical_object_type) -> dict[str, Any]:
    """Returns created default buffer value."""
    default_buffer_value_post_req = DefaultBufferValuePost(
        buffer_type_id=buffer_type["buffer_type_id"],
        physical_object_type_id=physical_object_type["physical_object_type_id"],
        service_type_id=None,
        buffer_value=100,
    )

    with httpx.Client(base_url=f"{urban_api_host}/api/v1") as client:
        response = client.post("/buffer_types/defaults", json=default_buffer_value_post_req.model_dump())

    assert response.status_code == 201, f"Invalid status code was returned: {response.status_code}."
    return response.json()


@pytest.fixture(scope="session")
def buffer(urban_api_host, buffer_type, urban_object) -> dict[str, Any]:
    """Returns created buffer."""
    buffer_post_req = BufferPut(
        buffer_type_id=buffer_type["buffer_type_id"],
        urban_object_id=urban_object["urban_object_id"],
        geometry=Geometry(
            type="Polygon",
            coordinates=[[[30.22, 59.86], [30.22, 59.85], [30.25, 59.85], [30.25, 59.86], [30.22, 59.86]]],
        ),
    )

    with httpx.Client(base_url=f"{urban_api_host}/api/v1") as client:
        response = client.put("/buffers", json=buffer_post_req.model_dump())

    assert response.status_code == 200, f"Invalid status code was returned: {response.status_code}."
    return response.json()


@pytest.fixture(scope="session")
def scenario_buffer(urban_api_host, buffer_type, scenario, scenario_urban_object, superuser_token) -> dict[str, Any]:
    """Returns created scenario buffer."""
    scenario_buffer_post_req = ScenarioBufferPut(
        buffer_type_id=buffer_type["buffer_type_id"],
        physical_object_id=scenario_urban_object["physical_object"]["physical_object_id"],
        is_scenario_physical_object=scenario_urban_object["physical_object"]["is_scenario_object"],
        object_geometry_id=scenario_urban_object["object_geometry"]["object_geometry_id"],
        is_scenario_geometry=scenario_urban_object["object_geometry"]["is_scenario_object"],
        service_id=scenario_urban_object["service"]["service_id"],
        is_scenario_service=scenario_urban_object["service"]["is_scenario_object"],
        geometry=Geometry(
            type="Polygon",
            coordinates=[[[30.22, 59.86], [30.22, 59.85], [30.25, 59.85], [30.25, 59.86], [30.22, 59.86]]],
        ),
    )
    scenario_id = scenario["scenario_id"]
    headers = {"Authorization": f"Bearer {superuser_token}"}

    with httpx.Client(base_url=f"{urban_api_host}/api/v1") as client:
        response = client.put(
            f"/scenarios/{scenario_id}/buffers",
            json=scenario_buffer_post_req.model_dump(),
            headers=headers,
        )

    assert response.status_code == 200, f"Invalid status code was returned: {response.status_code}."
    return response.json()


####################################################################################
#                                 Models                                           #
####################################################################################


@pytest.fixture
def buffer_type_req() -> BufferType:
    """GET request template for buffer type data."""

    return BufferType(
        buffer_type_id=1,
        name="Test buffer type name",
    )


@pytest.fixture
def buffer_type_post_req() -> BufferTypePost:
    """POST request template for buffer type data."""

    return BufferTypePost(name="Test buffer type name")


@pytest.fixture
def buffer_req() -> Buffer:
    """GET request template for buffer data."""

    return Buffer(
        buffer_type=BufferTypeBasic(
            id=1,
            name="Test buffer type name",
        ),
        urban_object=ShortUrbanObject(
            id=1,
            physical_object=PhysicalObjectBasic(
                id=1, name="test name", type=PhysicalObjectTypeBasic(id=1, name="test name")
            ),
            object_geometry=ObjectGeometryBasic(id=1, territory=ShortTerritory(id=1, name="test name")),
            service=ServiceBasic(id=1, name="test name", type=ServiceTypeBasic(id=1, name="test name")),
        ),
        geometry=Geometry(
            type="Polygon",
            coordinates=[[[30.22, 59.86], [30.22, 59.85], [30.25, 59.85], [30.25, 59.86], [30.22, 59.86]]],
        ),
        is_custom=True,
    )


@pytest.fixture
def buffer_put_req() -> BufferPut:
    """PUT request template for buffer data."""

    return BufferPut(
        buffer_type_id=1,
        urban_object_id=1,
        geometry=Geometry(
            type="Polygon",
            coordinates=[[[30.22, 59.86], [30.22, 59.85], [30.25, 59.85], [30.25, 59.86], [30.22, 59.86]]],
        ),
    )


@pytest.fixture
def default_buffer_value_req() -> DefaultBufferValue:
    """GET request template for default buffer value."""

    return DefaultBufferValue(
        buffer_type=BufferTypeBasic(id=1, name="test name"),
        physical_object_type=PhysicalObjectTypeBasic(id=1, name="test name"),
        service_type=None,
        buffer_value=100,
    )


@pytest.fixture
def default_buffer_value_post_req() -> DefaultBufferValuePost:
    """POST request template for default buffer value."""

    return DefaultBufferValuePost(
        buffer_type_id=1,
        physical_object_type_id=1,
        service_type_id=None,
        buffer_value=100,
    )


@pytest.fixture
def default_buffer_value_put_req() -> DefaultBufferValuePut:
    """PUT request template for default buffer value."""

    return DefaultBufferValuePut(
        buffer_type_id=1,
        physical_object_type_id=1,
        service_type_id=None,
        buffer_value=100,
    )


@pytest.fixture
def scenario_buffer_req() -> ScenarioBuffer:
    """GET request template for scenario buffer data."""

    return ScenarioBuffer(
        buffer_type=BufferTypeBasic(
            id=1,
            name="Test buffer type name",
        ),
        urban_object=ShortUrbanObject(
            id=1,
            physical_object=PhysicalObjectBasic(
                id=1, name="test name", type=PhysicalObjectTypeBasic(id=1, name="test name")
            ),
            object_geometry=ObjectGeometryBasic(id=1, territory=ShortTerritory(id=1, name="test name")),
            service=ServiceBasic(id=1, name="test name", type=ServiceTypeBasic(id=1, name="test name")),
        ),
        geometry=Geometry(
            type="Polygon",
            coordinates=[[[30.22, 59.86], [30.22, 59.85], [30.25, 59.85], [30.25, 59.86], [30.22, 59.86]]],
        ),
        is_custom=True,
        is_scenario_object=True,
        is_locked=False,
    )


@pytest.fixture
def scenario_buffer_put_req() -> ScenarioBufferPut:
    """PUT request template for scenario buffer data."""

    return ScenarioBufferPut(
        buffer_type_id=1,
        physical_object_id=1,
        is_scenario_physical_object=True,
        object_geometry_id=1,
        is_scenario_geometry=True,
        service_id=1,
        is_scenario_service=True,
        geometry=Geometry(
            type="Polygon",
            coordinates=[[[30.22, 59.86], [30.22, 59.85], [30.25, 59.85], [30.25, 59.86], [30.22, 59.86]]],
        ),
    )


@pytest.fixture
def scenario_buffer_delete_req() -> ScenarioBufferDelete:
    """DELETE request template for scenario buffer data."""

    return ScenarioBufferDelete(
        buffer_type_id=1,
        physical_object_id=1,
        is_scenario_physical_object=True,
        object_geometry_id=1,
        is_scenario_geometry=True,
        service_id=1,
        is_scenario_service=True,
    )
