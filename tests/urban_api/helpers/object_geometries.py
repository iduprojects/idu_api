"""All fixtures for object geometries tests are defined here."""

from typing import Any

import pytest

from idu_api.urban_api.schemas import ObjectGeometryPatch, ObjectGeometryPost, ObjectGeometryPut
from idu_api.urban_api.schemas.geometries import Geometry

__all__ = [
    "object_geometry",
    "object_geometries_patch_req",
    "object_geometries_post_req",
    "object_geometries_put_req",
    "scenario_geometry",
]


####################################################################################
#                        Integration tests helpers                                 #
####################################################################################


@pytest.fixture(scope="session")
def object_geometry(urban_object) -> dict[str, Any]:
    """Returns created object geometry."""
    return urban_object["object_geometry"]


@pytest.fixture(scope="session")
def scenario_geometry(scenario_urban_object) -> dict[str, Any]:
    """Returns created object geometry."""
    return scenario_urban_object["object_geometry"]


####################################################################################
#                                 Models                                           #
####################################################################################


@pytest.fixture
def object_geometries_post_req() -> ObjectGeometryPost:
    """POST request template for object geometry data."""

    return ObjectGeometryPost(
        territory_id=1,
        geometry=Geometry(
            type="Polygon",
            coordinates=[[[30.22, 59.86], [30.22, 59.85], [30.25, 59.85], [30.25, 59.86], [30.22, 59.86]]],
        ),
        centre_point=None,
        address="Test Address",
        osm_id="1",
    )


@pytest.fixture
def object_geometries_put_req() -> ObjectGeometryPut:
    """PUT request template for object geometry data."""

    return ObjectGeometryPut(
        territory_id=1,
        geometry=Geometry(
            type="Polygon",
            coordinates=[[[30.22, 59.86], [30.22, 59.85], [30.25, 59.85], [30.25, 59.86], [30.22, 59.86]]],
        ),
        centre_point=Geometry(type="Point", coordinates=[40.7128, -74.0060]),
        address="Updated Test Address",
        osm_id="1",
    )


@pytest.fixture
def object_geometries_patch_req() -> ObjectGeometryPatch:
    """PATCH request template for object geometry data."""

    return ObjectGeometryPatch(
        address="Patched Test Address",
        territory_id=1,
    )
