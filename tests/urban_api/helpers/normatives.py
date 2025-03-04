"""All fixtures for normatives tests are defined here."""

from datetime import date
from typing import Any

import httpx
import pytest

from idu_api.urban_api.schemas import NormativeDelete, NormativePatch, NormativePost

__all__ = [
    "normative",
    "normative_delete_req",
    "normative_patch_req",
    "normative_post_req",
]


####################################################################################
#                        Integration tests helpers                                 #
####################################################################################


@pytest.fixture(scope="session")
def normative(urban_api_host, service_type, country) -> dict[str, Any]:
    """Returns created normative."""
    normative_post_req = NormativePost(
        service_type_id=service_type["service_type_id"],
        urban_function_id=None,
        is_regulated=True,
        radius_availability_meters=500,
        time_availability_minutes=None,
        services_per_1000_normative=5,
        services_capacity_per_1000_normative=None,
        year=date.today().year,
        source="Test Source",
    )

    with httpx.Client(base_url=f"{urban_api_host}/api/v1") as client:
        response = client.post(
            f"territory/{country['territory_id']}/normatives", json=[normative_post_req.model_dump()]
        )

    assert response.status_code == 201, f"Invalid status code was returned: {response.status_code}."
    return response.json()[0]


####################################################################################
#                                 Models                                           #
####################################################################################


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
