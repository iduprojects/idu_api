"""All fixtures for hexagons tests are defined here."""

import pytest

from idu_api.urban_api.schemas import HexagonPost
from idu_api.urban_api.schemas.geometries import Geometry

__all__ = ["hexagon_post_req"]


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
