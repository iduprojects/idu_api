"""All fixtures for living buildings tests are defined here."""

import pytest

from idu_api.urban_api.schemas import BuildingPatch, BuildingPost, BuildingPut

__all__ = [
    "building_patch_req",
    "building_post_req",
    "building_put_req",
]


@pytest.fixture
def building_post_req() -> BuildingPost:
    """POST request template for buildings data."""

    return BuildingPost(
        physical_object_id=1,
        properties={"key": "value"},
    )


@pytest.fixture
def building_put_req() -> BuildingPut:
    """PUT request template for buildings data."""

    return BuildingPut(
        physical_object_id=1,
        floors=1,
        building_area_official=1.0,
        building_area_modeled=1.0,
        project_type="example",
        floor_type="example",
        wall_material="example",
        built_year=1,
        exploitation_start_year=1,
        properties={"updated_key": "updated_value"},
    )


@pytest.fixture
def building_patch_req() -> BuildingPatch:
    """PATCH request template for buildings data."""

    return BuildingPatch(
        physical_object_id=1,
        properties={"patched_key": "patched_value"},
    )
