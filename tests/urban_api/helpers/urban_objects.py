"""All fixtures for urban objects tests are defined here."""

from typing import Any

import httpx
import pytest

from idu_api.urban_api.schemas import PhysicalObjectWithGeometryPost, ScenarioServicePost, ServicePost, UrbanObjectPatch
from idu_api.urban_api.schemas.geometries import Geometry

__all__ = [
    "scenario_urban_object",
    "urban_object",
    "urban_object_patch_req",
]


####################################################################################
#                                 Models                                           #
####################################################################################


@pytest.fixture(scope="session")
def urban_object(urban_api_host, city, physical_object_type, service_type, territory_type) -> dict[str, Any]:
    """Returns created urban object."""
    physical_object_with_geometry_post_req = PhysicalObjectWithGeometryPost(
        territory_id=city["territory_id"],
        geometry=Geometry(
            type="Polygon",
            coordinates=[
                [
                    [30.22, 59.86],
                    [30.22, 59.85],
                    [30.25, 59.85],
                    [30.25, 59.86],
                    [30.22, 59.86],
                ]
            ],
        ),
        address="Test Address",
        osm_id="12345",
        physical_object_type_id=physical_object_type["physical_object_type_id"],
        name="Test Object",
        properties={"key": "value"},
    )

    with httpx.Client(base_url=f"{urban_api_host}/api/v1") as client:
        response = client.post("/physical_objects", json=physical_object_with_geometry_post_req.model_dump())
        physical_object_with_geometry = response.json()
        assert response.status_code == 201, f"Invalid status code was returned: {response.status_code}."

    service_post_req = ServicePost(
        physical_object_id=physical_object_with_geometry["physical_object"]["physical_object_id"],
        object_geometry_id=physical_object_with_geometry["object_geometry"]["object_geometry_id"],
        service_type_id=service_type["service_type_id"],
        territory_type_id=territory_type["territory_type_id"],
        name="Test Service",
        capacity=100,
        is_capacity_real=True,
    )

    with httpx.Client(base_url=f"{urban_api_host}/api/v1") as client:
        response = client.post("/services", json=service_post_req.model_dump())
        assert response.status_code == 201, f"Invalid status code was returned: {response.status_code}."
        response = client.get(f"/urban_objects/{physical_object_with_geometry['urban_object_id']}")
        assert response.status_code == 200, f"Invalid status code was returned: {response.status_code}."

    return response.json()


@pytest.fixture(scope="session")
def scenario_urban_object(
    urban_api_host,
    scenario,
    city,
    physical_object_type,
    service_type,
    territory_type,
    superuser_token,
) -> dict[str, Any]:
    """Returns created scenario urban object."""
    scenario_id = scenario["scenario_id"]
    headers = {"Authorization": f"Bearer {superuser_token}"}

    physical_object_with_geometry_post_req = PhysicalObjectWithGeometryPost(
        territory_id=city["territory_id"],
        geometry=Geometry(
            type="Polygon",
            coordinates=[
                [
                    [30.22, 59.86],
                    [30.22, 59.85],
                    [30.25, 59.85],
                    [30.25, 59.86],
                    [30.22, 59.86],
                ]
            ],
        ),
        address="Test Address",
        osm_id="12345",
        physical_object_type_id=physical_object_type["physical_object_type_id"],
        name="Test Object",
        properties={"key": "value"},
    )

    with httpx.Client(base_url=f"{urban_api_host}/api/v1") as client:
        response = client.post(
            f"/scenarios/{scenario_id}/physical_objects",
            json=physical_object_with_geometry_post_req.model_dump(),
            headers=headers,
        )
        physical_object_with_geometry = response.json()
        assert response.status_code == 201, f"Invalid status code was returned: {response.status_code}."

    service_post_req = ScenarioServicePost(
        physical_object_id=physical_object_with_geometry["physical_object"]["physical_object_id"],
        is_scenario_physical_object=True,
        object_geometry_id=physical_object_with_geometry["object_geometry"]["object_geometry_id"],
        is_scenario_geometry=True,
        service_type_id=service_type["service_type_id"],
        territory_type_id=territory_type["territory_type_id"],
        name="Test Service",
        capacity=100,
        is_capacity_real=True,
    )

    with httpx.Client(base_url=f"{urban_api_host}/api/v1") as client:
        response = client.post(
            f"/scenarios/{scenario_id}/services",
            json=service_post_req.model_dump(),
            headers=headers,
        )
        assert response.status_code == 201, f"Invalid status code was returned: {response.status_code}."

    return response.json()


####################################################################################
#                                 Models                                           #
####################################################################################


@pytest.fixture
def urban_object_patch_req() -> UrbanObjectPatch:
    """PATCH request template for hexagons data."""

    return UrbanObjectPatch(
        physical_object_id=1,
        object_geometry_id=1,
        service_id=1,
    )
