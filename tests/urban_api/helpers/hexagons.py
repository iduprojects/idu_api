"""All fixtures for hexagons tests are defined here."""

from typing import Any

import httpx
import pytest

from idu_api.urban_api.schemas import HexagonPost
from idu_api.urban_api.schemas.geometries import Geometry

__all__ = ["hexagon", "hexagon_post_req"]

####################################################################################
#                        Integration tests helpers                                 #
####################################################################################


@pytest.fixture(scope="session")
def hexagon(urban_api_host, region) -> dict[str, Any]:
    """Returns created hexagon."""
    hexagon_post_req = HexagonPost(
        geometry=Geometry(
            type="Polygon",
            coordinates=[[[30.22, 59.86], [30.22, 59.85], [30.25, 59.85], [30.25, 59.86], [30.22, 59.86]]],
        ),
        properties={},
    )

    with httpx.Client(base_url=f"{urban_api_host}/api/v1") as client:
        response = client.post(f"territory/{region['territory_id']}/hexagons", json=[hexagon_post_req.model_dump()])

    assert response.status_code == 201, f"Invalid status code was returned: {response.status_code}."
    return response.json()[0]


####################################################################################
#                                 Models                                           #
####################################################################################


@pytest.fixture
def hexagon_post_req() -> HexagonPost:
    """POST request template for hexagons data."""

    return HexagonPost(
        geometry=Geometry(
            type="Polygon",
            coordinates=[[[30.22, 59.86], [30.22, 59.85], [30.25, 59.85], [30.25, 59.86], [30.22, 59.86]]],
        ),
        properties={},
    )
