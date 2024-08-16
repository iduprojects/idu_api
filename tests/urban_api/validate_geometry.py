import pytest
from pydantic import ValidationError

from idu_api.urban_api.schemas.geometries import Geometry, GeometryValidationModel


class TestModel(GeometryValidationModel):
    pass


def test_valid_geometry():
    valid_geometry = Geometry(
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

    model = TestModel(
        territory_id=1,
        geometry=valid_geometry
    )

    assert model.geometry.type == "Polygon"
    assert model.geometry.as_shapely_geometry() is not None


def test_invalid_geometry_type():
    with pytest.raises(ValidationError, match="Input should be 'Point', 'Polygon', 'MultiPolygon' or 'LineString'"):
        Geometry(
            type="InvalidType",
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


def test_centre_point_validation():
    valid_geometry = Geometry(
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

    centre_point = Geometry(
        type="Point",
        coordinates=[30.24, 59.85]
    )

    model = TestModel(
        territory_id=1,
        geometry=valid_geometry,
        centre_point=centre_point
    )

    assert model.centre_point.type == "Point"
    assert model.centre_point.as_shapely_geometry() is not None


def test_invalid_centre_point():
    valid_geometry = Geometry(
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

    invalid_centre_point = Geometry(
        type="Polygon",
        coordinates=[
            [
                [30.24, 59.85],
                [30.24, 59.84],
                [30.26, 59.84],
                [30.26, 59.85],
                [30.24, 59.85],
            ]
        ]
    )

    with pytest.raises(ValueError, match="Only Point geometry is accepted for centre_point"):
        TestModel(
            territory_id=1,
            geometry=valid_geometry,
            centre_point=invalid_centre_point
        )


def test_automatic_centre_point_generation():
    valid_geometry = Geometry(
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

    model = TestModel(
        territory_id=1,
        geometry=valid_geometry,
    )

    assert model.centre_point is not None
    assert model.centre_point.type == "Point"
    assert model.centre_point.as_shapely_geometry() is not None

