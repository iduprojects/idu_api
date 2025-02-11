"""All fixtures for territories tests are defined here."""

import httpx
import pytest

from idu_api.urban_api.schemas import TargetCityTypePost, TerritoryPatch, TerritoryPost, TerritoryPut, \
    TerritoryTypePost, Territory, TerritoryType
from idu_api.urban_api.schemas.geometries import Geometry

__all__ = [
    "city",
    "city_type",
    "country",
    "country_type",
    "mo",
    "mo_type",
    "region",
    "region_type",
    "target_city_type_post_req",
    "territory_type_post_req",
    "territory_patch_req",
    "territory_post_req",
    "territory_put_req",
]


def get_or_create_type(urban_api_host: str, name: str) -> TerritoryType:
    # Arrange
    territory_type = {"name": name}

    # Act
    with httpx.Client(base_url=f"{urban_api_host}/api/v1") as client:
        try:
            response = client.post("/territory_types", json=territory_type)
            response.raise_for_status()
        except httpx.HTTPError:
            response = client.get("/territory_types")
            territory_types = response.json()
            for t_type in territory_types:
                if t_type.get("name") == name:
                    return TerritoryType(**t_type)

    # Assert
    assert response.status_code == 201, f"Error on creating territory type '{name}'"

    return TerritoryType(**response.json())


@pytest.fixture
def country_type(urban_api_host) -> TerritoryType:
    """Territory type for countries."""

    return get_or_create_type(urban_api_host, "country")


@pytest.fixture
def region_type(urban_api_host) -> TerritoryType:
    """Territory type for regions."""

    return get_or_create_type(urban_api_host, "region")


@pytest.fixture
def mo_type(urban_api_host) -> TerritoryType:
    """Territory type for regions."""

    return get_or_create_type(urban_api_host, "mo")


@pytest.fixture
def city_type(urban_api_host) -> TerritoryType:
    """Territory type for regions."""

    return get_or_create_type(urban_api_host, "city")


@pytest.fixture
def country(urban_api_host, country_type) -> Territory:
    """Country territory."""

    # Arrange
    territory = {
        "name": "Российская Федерация",
        "parent_id": None,
        "geometry": {
            "type": "Polygon",
            "coordinates": [[[30.22, 59.86], [30.22, 59.85], [30.25, 59.85], [30.25, 59.86], [30.22, 59.86]]],
        },
        "territory_type_id": country_type.territory_type_id,
        "admin_center_id": None,
        "target_city_type_id": None,
        "oktmo_code": None,
        "okato_code": None,
        "is_city": False,
    }

    # Act
    with httpx.Client(base_url=f"{urban_api_host}/api/v1") as client:
        response = client.post("/territory", json=territory)

    # Assert
    assert response.status_code == 201, "Error on creating territory with type 'country'"

    return Territory(**response.json())


@pytest.fixture
def region(urban_api_host, country, region_type) -> Territory:
    """Region territory."""

    # Arrange
    territory = {
        "name": "Ленинградская область",
        "parent_id": country.territory_id,
        "geometry": {
            "type": "Polygon",
            "coordinates": [[[30.22, 59.86], [30.22, 59.85], [30.25, 59.85], [30.25, 59.86], [30.22, 59.86]]],
        },
        "territory_type_id": region_type.territory_type_id,
        "admin_center_id": None,
        "target_city_type_id": None,
        "oktmo_code": None,
        "okato_code": None,
        "is_city": False,
    }

    # Act
    with httpx.Client(base_url=f"{urban_api_host}/api/v1") as client:
        response = client.post("/territory", json=territory)

    # Assert
    assert response.status_code == 201, "Error on creating territory with type 'region'"

    return Territory(**response.json())


@pytest.fixture
def mo(urban_api_host, region, mo_type) -> Territory:
    """MO territory."""

    # Arrange
    territory = {
        "name": "Гатчинский муниципальный район",
        "parent_id": region.territory_id,
        "geometry": {
            "type": "Polygon",
            "coordinates": [[[30.22, 59.86], [30.22, 59.85], [30.25, 59.85], [30.25, 59.86], [30.22, 59.86]]],
        },
        "territory_type_id": mo_type.territory_type_id,
        "admin_center_id": None,
        "target_city_type_id": None,
        "oktmo_code": None,
        "okato_code": None,
        "is_city": False,
    }

    # Act
    with httpx.Client(base_url=f"{urban_api_host}/api/v1") as client:
        response = client.post("/territory", json=territory)

    # Assert
    assert response.status_code == 201, "Error on creating territory with type 'region'"

    return Territory(**response.json())


@pytest.fixture
def city(urban_api_host, mo, city_type) -> Territory:
    """City territory."""

    # Arrange
    territory = {
        "name": "Гатчина",
        "parent_id": mo.territory_id,
        "geometry": {
            "type": "Polygon",
            "coordinates": [[[30.22, 59.86], [30.22, 59.85], [30.25, 59.85], [30.25, 59.86], [30.22, 59.86]]],
        },
        "territory_type_id": city_type.territory_type_id,
        "admin_center_id": None,
        "target_city_type_id": None,
        "oktmo_code": None,
        "okato_code": None,
        "is_city": True,
    }

    # Act
    with httpx.Client(base_url=f"{urban_api_host}/api/v1") as client:
        response = client.post("/territory", json=territory)

    # Assert
    assert response.status_code == 201, "Error on creating territory with type 'city'"

    return Territory(**response.json())


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
