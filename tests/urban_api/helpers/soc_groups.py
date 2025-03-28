"""All fixtures for social groups and values tests are defined here."""

from datetime import date
from typing import Any

import httpx
import pytest

from idu_api.urban_api.schemas import (
    SocGroupIndicatorValuePost,
    SocGroupIndicatorValuePut,
    SocGroupPost,
    SocGroupServiceTypePost,
    SocValuePost,
)

__all__ = [
    "soc_group_indicator_post_req",
    "soc_group_indicator_put_req",
    "soc_group_post_req",
    "soc_group_service_type_post_req",
    "soc_value_post_req",
    "social_group",
    "social_group_indicator",
    "social_value",
]

####################################################################################
#                        Integration tests helpers                                 #
####################################################################################


@pytest.fixture(scope="session")
def social_group(urban_api_host) -> dict[str, Any]:
    """Returns created social group."""
    soc_group_post_req = SocGroupPost(name="Test Social Group")

    with httpx.Client(base_url=f"{urban_api_host}/api/v1") as client:
        response = client.post("/social_groups", json=soc_group_post_req.model_dump())

    assert response.status_code == 201, f"Invalid status code was returned: {response.status_code}."
    return response.json()


@pytest.fixture(scope="session")
def social_value(urban_api_host) -> dict[str, Any]:
    """Returns created social value."""
    soc_value_post_req = SocValuePost(name="Test Social Value")

    with httpx.Client(base_url=f"{urban_api_host}/api/v1") as client:
        response = client.post("/social_values", json=soc_value_post_req.model_dump())

    assert response.status_code == 201, f"Invalid status code was returned: {response.status_code}."
    return response.json()


@pytest.fixture(scope="session")
def social_group_indicator(urban_api_host, social_group, social_value, region) -> dict[str, Any]:
    """Returns created social group's indicator value."""
    soc_group_indicator_post_req = SocGroupIndicatorValuePost(
        soc_value_id=social_value["soc_value_id"],
        territory_id=region["territory_id"],
        year=date.today().year,
        value_type="target",
        value=0.5,
    )

    with httpx.Client(base_url=f"{urban_api_host}/api/v1") as client:
        response = client.post(
            f"/social_groups/{social_group['soc_group_id']}/indicators",
            json=soc_group_indicator_post_req.model_dump(),
        )

    assert response.status_code == 201, f"Invalid status code was returned: {response.status_code}.\n{response.json()}"
    return response.json()


####################################################################################
#                                 Models                                           #
####################################################################################


@pytest.fixture
def soc_group_post_req() -> SocGroupPost:
    """POST request template for social group data."""

    return SocGroupPost(name="Test Social Group")


@pytest.fixture
def soc_value_post_req() -> SocValuePost:
    """POST request template for social value data."""

    return SocGroupPost(name="Test Social Value")


@pytest.fixture
def soc_group_indicator_post_req() -> SocGroupIndicatorValuePost:
    """POST request template for social group's indicator value data."""

    return SocGroupIndicatorValuePost(
        soc_value_id=1,
        territory_id=1,
        year=date.today().year,
        value_type="target",
        value=0.5,
    )


@pytest.fixture
def soc_group_indicator_put_req() -> SocGroupIndicatorValuePut:
    """PUT request template for social group's indicator value data."""

    return SocGroupIndicatorValuePut(
        soc_value_id=1,
        territory_id=1,
        year=date.today().year,
        value_type="target",
        value=0.5,
    )


@pytest.fixture
def soc_group_service_type_post_req() -> SocGroupIndicatorValuePut:
    """POST request template for social group's service type data."""

    return SocGroupServiceTypePost(
        service_type_id=1,
        infrastructure_type="basic",
    )
