"""All fixtures for normatives tests are defined here."""

from datetime import date

import pytest

from idu_api.urban_api.schemas import NormativeDelete, NormativePatch, NormativePost

__all__ = [
    "normative_delete_req",
    "normative_patch_req",
    "normative_post_req",
]


@pytest.fixture
def normative_post_req() -> NormativePost:
    """POST request template for normatives data."""

    return NormativePost(
        service_type_id=1,
        urban_function_id=None,
        is_regulated=True,
        radius_availability_meters=500,
        time_availability_minutes=None,
        services_per_1000_normative=5,
        services_capacity_per_1000_normative=None,
        year=date.today().year,
        source="Test Source",
    )


@pytest.fixture
def normative_patch_req() -> NormativePatch:
    """PATCH request template for normatives data."""

    return NormativePatch(
        service_type_id=1,
        urban_function_id=None,
        year=date.today().year,
        radius_availability_meters=100,
        services_per_1000_normative=1,
        source="Patched Source",
    )


@pytest.fixture
def normative_delete_req() -> NormativePatch:
    """DELETE request template for normatives data."""

    return NormativeDelete(
        service_type_id=1,
        urban_function_id=None,
        year=date.today().year,
    )
