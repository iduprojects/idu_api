"""All fixtures for tests that use geometries are defined here."""

import pytest
from shapely.geometry import LineString, MultiLineString, MultiPoint, MultiPolygon, Point, Polygon

from idu_api.urban_api.schemas.geometries import AllPossibleGeometry

Geom = Point | Polygon | MultiPolygon | LineString | MultiLineString | MultiPoint

__all__ = ["shapely_geometry"]


@pytest.fixture
def shapely_geometry() -> Geom:
    """Shapely geometry example object."""

    geometry = AllPossibleGeometry(
        type="Polygon",
        coordinates=[
            [
                [30.22, 59.86],
                [30.22, 59.85],
                [30.25, 59.85],
                [30.25, 59.86],
                [30.22, 59.86],
            ]
        ],
    )

    return geometry.as_shapely_geometry()
