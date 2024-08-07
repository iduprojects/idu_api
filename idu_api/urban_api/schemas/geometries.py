"""Geojson response models are defined here."""

import json
from typing import Any, Dict, Iterable, Literal, Optional

import shapely
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
            self._shapely_geom = shapely.from_geojson(json.dumps({"type": self.type, "coordinates": self.coordinates}))
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
        return cls(**geom.mapping(geometry))


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
