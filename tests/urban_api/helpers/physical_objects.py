"""All fixtures for physical objects tests are defined here."""

from datetime import datetime, timezone

import pytest

from idu_api.urban_api.schemas import (
    PhysicalObject,
    PhysicalObjectPatch,
    PhysicalObjectPost,
    PhysicalObjectPut,
    PhysicalObjectType,
    PhysicalObjectWithGeometry,
    PhysicalObjectWithGeometryPost,
)
from idu_api.urban_api.schemas.geometries import Geometry
from idu_api.urban_api.schemas.short_models import PhysicalObjectFunctionBasic, ShortTerritory

__all__ = [
    "physical_object_req",
    "physical_object_patch_req",
    "physical_object_post_req",
    "physical_object_put_req",
    "physical_object_with_geometry_req",
    "physical_object_with_geometry_post_req",
]


@pytest.fixture
def physical_object_req() -> PhysicalObject:
    """Request template for PhysicalObject data."""

    return PhysicalObject(
        physical_object_id=1,
        physical_object_type=PhysicalObjectType(
            physical_object_type_id=1,
            name="Test Type",
            physical_object_function=PhysicalObjectFunctionBasic(
                id=1,
                name="Test Function",
            ),
        ),
        name="Test Object",
        properties={"key": "value"},
        living_building=None,
        territories=[ShortTerritory(id=1, name="Test Territory")],
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
    )


@pytest.fixture
def physical_object_with_geometry_req() -> PhysicalObjectWithGeometry:
    """Request template for PhysicalObjectWithGeometry data."""

    return PhysicalObjectWithGeometry(
        physical_object_id=1,
        physical_object_type=PhysicalObjectType(
            physical_object_type_id=1,
            name="Test Type",
            physical_object_function=PhysicalObjectFunctionBasic(
                id=1,
                name="Test Function",
            ),
        ),
        territory=ShortTerritory(id=1, name="Test Territory"),
        name="Test Object",
        properties={"key": "value"},
        living_building=None,
        object_geometry_id=1,
        address="Test Address",
        osm_id="12345",
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
        centre_point=Geometry(
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
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
    )


@pytest.fixture
def physical_object_post_req() -> PhysicalObjectPost:
    """POST request template for PhysicalObject data."""

    return PhysicalObjectPost(
        physical_object_type_id=1,
        name="Test Object",
        properties={"key": "value"},
    )


@pytest.fixture
def physical_object_with_geometry_post_req() -> PhysicalObjectWithGeometryPost:
    """POST request template for PhysicalObjectWithGeometry data."""

    return PhysicalObjectWithGeometryPost(
        territory_id=1,
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
        physical_object_type_id=1,
        name="Test Object",
        properties={"key": "value"},
    )


@pytest.fixture
def physical_object_put_req() -> PhysicalObjectPut:
    """PUT request template for PhysicalObject data."""

    return PhysicalObjectPut(
        physical_object_type_id=1,
        name="Updated Object",
        properties={"updated_key": "updated_value"},
    )


@pytest.fixture
def physical_object_patch_req() -> PhysicalObjectPatch:
    """PATCH request template for PhysicalObject data."""

    return PhysicalObjectPatch(
        physical_object_type_id=1,
        name="Patched Object",
        properties={"patched_key": "patched_value"},
    )
