"""
Geometry models is defined here.
"""
from pydantic import BaseModel, Field
from typing import Literal, Any
from shapely import geometry as geom


class Geometry(BaseModel):
    """
    Geometry representation for GeoJSON model.
    """

    type: Literal["Point", "Polygon", "MultiPolygon", "LineString"] = Field(default="Polygon")
    coordinates: list[Any] = Field(
        description="list[int] for Point,\n" "list[list[list[int]]] for Polygon",
        default=[[[30.22, 59.86], [30.22, 59.85], [30.25, 59.85], [30.25, 59.86], [30.22, 59.86]]],
    )

    def as_shapely_geometry(self) -> geom.Point | geom.Polygon | geom.MultiPolygon | geom.LineString:
        """
        Return Shapely geometry object from the parsed geometry.
        """
        match self.type:
            case "Point":
                return geom.Point(self.coordinates)
            case "Polygon":
                return geom.Polygon(self.coordinates[0])
            case "MultiPolygon":
                return geom.MultiPolygon(self.coordinates)
            case "LineString":
                return geom.LineString(self.coordinates)
