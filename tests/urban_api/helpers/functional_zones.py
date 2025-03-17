"""All fixtures for functional zones tests are defined here."""

from datetime import datetime, timezone
from typing import Any

import httpx
import pytest

from idu_api.urban_api.schemas import (
    FunctionalZone,
    FunctionalZonePatch,
    FunctionalZonePost,
    FunctionalZonePut,
    FunctionalZoneType,
    FunctionalZoneTypePost,
    ProfilesReclamationData,
    ProfilesReclamationDataDelete,
    ProfilesReclamationDataPost,
    ProfilesReclamationDataPut,
    ScenarioFunctionalZone,
    ScenarioFunctionalZonePatch,
    ScenarioFunctionalZonePost,
    ScenarioFunctionalZonePut,
)
from idu_api.urban_api.schemas.geometries import Geometry
from idu_api.urban_api.schemas.short_models import FunctionalZoneTypeBasic, ShortScenario, ShortTerritory

__all__ = [
    "functional_zone",
    "functional_zone_type",
    "functional_zone_type_req",
    "functional_zone_type_post_req",
    "functional_zone_req",
    "functional_zone_patch_req",
    "functional_zone_post_req",
    "functional_zone_put_req",
    "scenario_functional_zone",
    "scenario_functional_zone_patch_req",
    "scenario_functional_zone_post_req",
    "scenario_functional_zone_put_req",
    "profiles_reclamation_data",
    "profiles_reclamation_req",
    "profiles_reclamation_post_req",
    "profiles_reclamation_put_req",
]


####################################################################################
#                        Integration tests helpers                                 #
####################################################################################


@pytest.fixture(scope="session")
def functional_zone_type(urban_api_host) -> dict[str, Any]:
    """Returns created functional zone type."""
    functional_zone_type_post_req = FunctionalZoneTypePost(
        name="Test functional zone type name",
        zone_nickname="Test functional zone type nickname",
        description="Test functional zone type description",
    )

    with httpx.Client(base_url=f"{urban_api_host}/api/v1") as client:
        response = client.post("/functional_zones_types", json=functional_zone_type_post_req.model_dump())

    assert response.status_code == 201, f"Invalid status code was returned: {response.status_code}."
    return response.json()


@pytest.fixture(scope="session")
def profiles_reclamation_data(urban_api_host, functional_zone_type) -> dict[str, Any]:
    """Returns created profiles reclamation data."""
    profiles_reclamation_post_req = ProfilesReclamationDataPost(
        source_profile_id=functional_zone_type["functional_zone_type_id"],
        target_profile_id=functional_zone_type["functional_zone_type_id"],
        territory_id=None,
        technical_price=0,
        technical_time=0,
        biological_price=0,
        biological_time=0,
    )

    with httpx.Client(base_url=f"{urban_api_host}/api/v1") as client:
        response = client.put("/profiles_reclamation", json=profiles_reclamation_post_req.model_dump())

    assert response.status_code == 200, f"Invalid status code was returned: {response.status_code}."
    return response.json()


@pytest.fixture(scope="session")
def functional_zone(urban_api_host, functional_zone_type, city) -> dict[str, Any]:
    """Returns created functional zone."""
    functional_zone_post_req = FunctionalZonePost(
        name="Test Functional Zone Name",
        territory_id=city["territory_id"],
        functional_zone_type_id=functional_zone_type["functional_zone_type_id"],
        year=datetime.today().year,
        source="Test Source",
        geometry=Geometry(
            type="Polygon",
            coordinates=[[[30.22, 59.86], [30.22, 59.85], [30.25, 59.85], [30.25, 59.86], [30.22, 59.86]]],
        ),
    )

    with httpx.Client(base_url=f"{urban_api_host}/api/v1") as client:
        response = client.post("/functional_zones", json=functional_zone_post_req.model_dump())

    assert response.status_code == 201, f"Invalid status code was returned: {response.status_code}."
    return response.json()


@pytest.fixture(scope="session")
def scenario_functional_zone(urban_api_host, functional_zone_type, scenario, superuser_token) -> dict[str, Any]:
    """Returns created scenario functional zone."""
    scenario_functional_zone_post_req = ScenarioFunctionalZonePost(
        name="Test Functional Zone Name",
        functional_zone_type_id=functional_zone_type["functional_zone_type_id"],
        year=datetime.today().year,
        source="Test Source",
        geometry=Geometry(
            type="Polygon",
            coordinates=[[[30.22, 59.86], [30.22, 59.85], [30.25, 59.85], [30.25, 59.86], [30.22, 59.86]]],
        ),
    )
    scenario_id = scenario["scenario_id"]
    headers = {"Authorization": f"Bearer {superuser_token}"}

    with httpx.Client(base_url=f"{urban_api_host}/api/v1") as client:
        response = client.post(
            f"/scenarios/{scenario_id}/functional_zones",
            json=[scenario_functional_zone_post_req.model_dump()],
            headers=headers,
        )

    assert response.status_code == 201, f"Invalid status code was returned: {response.status_code}."
    return response.json()[0]


####################################################################################
#                                 Models                                           #
####################################################################################


@pytest.fixture
def functional_zone_type_req() -> FunctionalZone:
    """GET request template for functional zone type data."""

    return FunctionalZoneType(
        functional_zone_type_id=1,
        name="Test functional zone type name",
        zone_nickname="Test functional zone type nickname",
        description="Test functional zone type description",
    )


@pytest.fixture
def functional_zone_type_post_req() -> FunctionalZonePost:
    """POST request template for functional zone type data."""

    return FunctionalZoneTypePost(
        name="Test functional zone type name",
        zone_nickname="Test functional zone type nickname",
        description="Test functional zone type description",
    )


@pytest.fixture
def functional_zone_req() -> FunctionalZone:
    """GET request template for functional zone data."""

    return FunctionalZone(
        functional_zone_id=1,
        territory=ShortTerritory(id=1, name="Test Territory"),
        functional_zone_type=FunctionalZoneTypeBasic(
            id=1,
            name="Test functional zone type name",
            nickname="Test functional zone type nickname",
        ),
        name="Test Functional Zone",
        year=datetime.today().year,
        source="Test Source",
        geometry=Geometry(
            type="Polygon",
            coordinates=[[[30.22, 59.86], [30.22, 59.85], [30.25, 59.85], [30.25, 59.86], [30.22, 59.86]]],
        ),
        properties={},
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
    )


@pytest.fixture
def functional_zone_post_req() -> FunctionalZonePost:
    """POST request template for functional zone data."""

    return FunctionalZonePost(
        name="Test Functional Zone Name",
        territory_id=1,
        functional_zone_type_id=1,
        year=datetime.today().year,
        source="Test Source",
        geometry=Geometry(
            type="Polygon",
            coordinates=[[[30.22, 59.86], [30.22, 59.85], [30.25, 59.85], [30.25, 59.86], [30.22, 59.86]]],
        ),
        properties={},
    )


@pytest.fixture
def functional_zone_put_req() -> FunctionalZonePut:
    """PUT request template for functional zone data."""

    return FunctionalZonePost(
        name="Updated Test Functional Zone Name",
        territory_id=1,
        functional_zone_type_id=1,
        year=datetime.today().year,
        source="Test Source",
        geometry=Geometry(
            type="Polygon",
            coordinates=[[[30.22, 59.86], [30.22, 59.85], [30.25, 59.85], [30.25, 59.86], [30.22, 59.86]]],
        ),
        properties={},
    )


@pytest.fixture
def functional_zone_patch_req() -> FunctionalZonePatch:
    """PATCH request template for functional zone data."""

    return FunctionalZonePatch(
        name="New Patched Functional Zone Name",
        territory_id=1,
        functional_zone_type_id=1,
    )


@pytest.fixture
def profiles_reclamation_req() -> ProfilesReclamationData:
    """GET request template for profiles reclamation data."""

    return ProfilesReclamationData(
        profile_reclamation_id=1,
        source_profile_id=1,
        target_profile_id=1,
        territory=(
            ShortTerritory(
                id=1,
                name="Test Territory",
            )
        ),
        technical_price=0,
        technical_time=0,
        biological_price=0,
        biological_time=0,
    )


@pytest.fixture
def profiles_reclamation_post_req() -> ProfilesReclamationDataPost:
    """POST request template for profiles reclamation data."""

    return ProfilesReclamationDataPost(
        source_profile_id=1,
        target_profile_id=1,
        territory_id=1,
        technical_price=0,
        technical_time=0,
        biological_price=0,
        biological_time=0,
    )


@pytest.fixture
def profiles_reclamation_put_req() -> ProfilesReclamationDataPut:
    """PUT request template for profiles reclamation data."""

    return ProfilesReclamationDataPut(
        source_profile_id=1,
        target_profile_id=1,
        territory_id=1,
        technical_price=0,
        technical_time=0,
        biological_price=0,
        biological_time=0,
    )


@pytest.fixture
def scenario_functional_zone_req() -> FunctionalZone:
    """GET request template for scenario functional zone data."""

    return ScenarioFunctionalZone(
        functional_zone_id=1,
        scenario=ShortScenario(id=1, name="Test Scenario"),
        functional_zone_type=FunctionalZoneTypeBasic(
            id=1,
            name="Test functional zone type name",
            nickname="Test functional zone type nickname",
        ),
        name="Test Functional Zone",
        year=datetime.today().year,
        source="Test Source",
        geometry=Geometry(
            type="Polygon",
            coordinates=[[[30.22, 59.86], [30.22, 59.85], [30.25, 59.85], [30.25, 59.86], [30.22, 59.86]]],
        ),
        properties={},
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
    )


@pytest.fixture
def scenario_functional_zone_post_req() -> FunctionalZonePost:
    """POST request template for scenario functional zone data."""

    return ScenarioFunctionalZonePost(
        name="Test Functional Zone Name",
        functional_zone_type_id=1,
        year=datetime.today().year,
        source="Test Source",
        geometry=Geometry(
            type="Polygon",
            coordinates=[[[30.22, 59.86], [30.22, 59.85], [30.25, 59.85], [30.25, 59.86], [30.22, 59.86]]],
        ),
        properties={},
    )


@pytest.fixture
def scenario_functional_zone_put_req() -> FunctionalZonePut:
    """POST request template for scenario functional zone data."""

    return ScenarioFunctionalZonePut(
        name="Updated Test Functional Zone Name",
        functional_zone_type_id=1,
        year=datetime.today().year,
        source="Test Source",
        geometry=Geometry(
            type="Polygon",
            coordinates=[[[30.22, 59.86], [30.22, 59.85], [30.25, 59.85], [30.25, 59.86], [30.22, 59.86]]],
        ),
        properties={},
    )


@pytest.fixture
def scenario_functional_zone_patch_req() -> FunctionalZonePatch:
    """POST request template for scenario functional zone data."""

    return ScenarioFunctionalZonePatch(
        name="New Patched Functional Zone Name",
        functional_zone_type_id=1,
    )
