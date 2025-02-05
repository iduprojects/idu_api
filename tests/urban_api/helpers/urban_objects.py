"""All fixtures for urban objects tests are defined here."""

import pytest

from idu_api.urban_api.schemas import UrbanObjectPatch

__all__ = ["urban_object_patch_req"]


@pytest.fixture
def urban_object_patch_req() -> UrbanObjectPatch:
    """PATCH request template for hexagons data."""

    return UrbanObjectPatch(
        physical_object_id=1,
        object_geometry_id=1,
        service_id=1,
    )
