"""All fixtures for living buildings tests are defined here."""

import pytest

from idu_api.urban_api.schemas import LivingBuildingPatch, LivingBuildingPost, LivingBuildingPut

__all__ = [
    "living_building_patch_req",
    "living_building_post_req",
    "living_building_put_req",
]


@pytest.fixture
def living_building_post_req() -> LivingBuildingPost:
    """POST request template for living buildings data."""

    return LivingBuildingPost(
        physical_object_id=1,
        living_area=1,
        properties={"key": "value"},
    )


@pytest.fixture
def living_building_put_req() -> LivingBuildingPut:
    """PUT request template for living buildings data."""

    return LivingBuildingPut(
        physical_object_id=1,
        living_area=1,
        properties={"updated_key": "updated_value"},
    )


@pytest.fixture
def living_building_patch_req() -> LivingBuildingPatch:
    """PATCH request template for living buildings data."""

    return LivingBuildingPatch(
        physical_object_id=1,
        properties={"patched_key": "patched_value"},
    )
