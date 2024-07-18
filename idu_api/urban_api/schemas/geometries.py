"""
Geojson response models is defined here.
"""

from typing import Any, Dict, Iterable, Literal, Optional

import shapely.geometry as geom
from geojson_pydantic import Feature, FeatureCollection
from pydantic import BaseModel, Field


class Geometry(BaseModel):
    """
    Geometry representation for GeoJSON model.
    """

    type: Literal["Point", "Polygon", "MultiPolygon", "LineString"] = Field(default="Polygon")
    coordinates: list[Any] = Field(
        description="list[int] for Point,\n" "list[list[list[int]]] for Polygon",
        default=[
            [
                [30.22, 59.86],
                [30.22, 59.85],
                [30.25, 59.85],
                [30.25, 59.86],
                [30.22, 59.86],
            ]
        ],
    )
    _shapely_geom: geom.Point | geom.Polygon | geom.MultiPolygon | geom.LineString | None = None

    def as_shapely_geometry(
        self,
    ) -> geom.Point | geom.Polygon | geom.MultiPolygon | geom.LineString:
        """
        Return Shapely geometry object from the parsed geometry.
        """
        if self._shapely_geom is None:
            match self.type:
                case "Point":
                    self._shapely_geom = geom.Point(self.coordinates)
                case "Polygon":
                    self._shapely_geom = geom.Polygon(self.coordinates[0])  # pylint: disable=unsubscriptable-object
                case "MultiPolygon":
                    self._shapely_geom = geom.MultiPolygon(self.coordinates)
                case "LineString":
                    self._shapely_geom = geom.LineString(self.coordinates)
        return self._shapely_geom

    @classmethod
    def from_shapely_geometry(
        cls, geometry: geom.Point | geom.Polygon | geom.MultiPolygon | geom.LineString | None
    ) -> Optional["Geometry"]:
        """
        Construct Geometry model from shapely geometry.
        """
        if geometry is None:
            return None
        match type(geometry):
            case geom.Point:
                return cls(type="Point", coordinates=geometry.coords[0])
            case geom.Polygon:
                return cls(type="Polygon", coordinates=[list(geometry.exterior.coords)])
            case geom.MultiPolygon:
                return cls(
                    type="MultiPolygon", coordinates=[[list(polygon.exterior.coords)] for polygon in geometry.geoms]
                )
            case geom.LineString:
                return cls(type="LineString", coordinates=geometry.coords)


class GeoJSONResponse(FeatureCollection):
    type: Literal["FeatureCollection"] = "FeatureCollection"

    @classmethod
    async def from_list(
        cls,
        features: Iterable[Dict[str, Any]],
        centers_only: bool = False,
    ) -> "GeoJSONResponse":
        """
        Construct GeoJSON model from list of dictionaries,
        with one field in each containing GeoJSON geometries.
        """

        feature_collection = []
        for feature in features:
            properties = dict(feature)
            if not centers_only:
                geometry = properties.pop("geometry", None)
                del properties["centre_point"]
            else:
                geometry = properties.pop("centre_point", None)
                del properties["geometry"]

            feature_collection.append(Feature(type="Feature", geometry=geometry, properties=properties))

        return cls(features=feature_collection)
