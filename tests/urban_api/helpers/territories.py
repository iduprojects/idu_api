"""All fixtures for territories tests are defined here."""

import pytest

from idu_api.urban_api.schemas import TargetCityTypePost, TerritoryPatch, TerritoryPost, TerritoryPut, TerritoryTypePost
from idu_api.urban_api.schemas.geometries import Geometry

__all__ = [
    "target_city_type_post_req",
    "territory_type_post_req",
    "territory_patch_req",
    "territory_post_req",
    "territory_put_req",
]


@pytest.fixture
def territory_type_post_req() -> TerritoryPost:
    """POST request template for territories types data."""

    return TerritoryTypePost(name="Test Territory Type Name")


@pytest.fixture
def target_city_type_post_req() -> TerritoryPost:
    """POST request template for target city types data."""

    return TargetCityTypePost(name="Test Target City Type Name", description="Test Description")


@pytest.fixture
def territory_post_req() -> TerritoryPost:
    """POST request template for territories data."""

    return TerritoryPost(
        name="Test Territory Name",
        geometry=Geometry(
            type="Polygon",
            coordinates=[[[30.22, 59.86], [30.22, 59.85], [30.25, 59.85], [30.25, 59.86], [30.22, 59.86]]],
        ),
        territory_type_id=1,
        parent_id=1,
        properties={},
        admin_center_id=1,
        target_city_type_id=1,
        okato_code="1",
        oktmo_code="1",
        is_city=False,
    )


@pytest.fixture
def territory_put_req() -> TerritoryPut:
    """PUT request template for territories data."""

    return TerritoryPut(
        name="Updated Test Territory Name",
        geometry=Geometry(
            type="Polygon",
            coordinates=[[[30.22, 59.86], [30.22, 59.85], [30.25, 59.85], [30.25, 59.86], [30.22, 59.86]]],
        ),
        centre_point=Geometry(
            type="Point",
            coordinates=[30.22, 59.86],
        ),
        territory_type_id=1,
        parent_id=1,
        properties={},
        admin_center_id=1,
        target_city_type_id=1,
        okato_code="1",
        oktmo_code="1",
        is_city=False,
    )


@pytest.fixture
def territory_patch_req() -> TerritoryPatch:
    """PATCH request template for territories data."""

    return TerritoryPatch(
        name="New Patched Territory Name",
        parent_id=1,
        territory_type_id=1,
        admin_center_id=1,
        target_city_type_id=1,
    )
