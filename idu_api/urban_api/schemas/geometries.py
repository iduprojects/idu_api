"""Geojson response models are defined here."""

import json
from typing import Any, Iterable, Literal, Optional, Type, TypeVar

import shapely
import shapely.geometry as geom
from geojson_pydantic import Feature, FeatureCollection
from loguru import logger
from pydantic import BaseModel, Field, field_validator, model_validator


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


T = TypeVar("T", bound="GeometryValidationModel")


class GeometryValidationModel(BaseModel):
    """
    Base model with geometry validation methods.
    """

    geometry: Geometry | None = None
    centre_point: Geometry | None = None

    @field_validator("geometry")
    @classmethod
    def validate_geometry(cls, geometry: "Geometry") -> "Geometry":
        """Validate that given geometry is valid by creating a Shapely object."""
        if geometry:
            try:
                geometry.as_shapely_geometry()
            except (AttributeError, ValueError, TypeError) as exc:
                logger.debug("Exception on passing geometry: {!r}", exc)
                raise ValueError("Invalid geometry passed") from exc
        return geometry

    @field_validator("centre_point")
    @classmethod
    def validate_centre_point(cls, centre_point: Geometry | None) -> Geometry | None:
        """Validate that given centre_point is a valid Point geometry."""
        if centre_point:
            if centre_point.type != "Point":
                raise ValueError("Only Point geometry is accepted for centre_point")
            try:
                centre_point.as_shapely_geometry()
            except (AttributeError, ValueError, TypeError) as exc:
                logger.debug("Exception on passing geometry: {!r}", exc)
                raise ValueError("Invalid centre_point passed") from exc
        return centre_point

    @model_validator(mode="after")
    @classmethod
    def validate_centre_point_from_geometry(cls: Type[T], model: T) -> T:
        """Use the geometry's centroid for centre_point if it is missing."""
        if model.centre_point is None and model.geometry:
            model.centre_point = Geometry.from_shapely_geometry(model.geometry.as_shapely_geometry().centroid)
        return model


class GeoJSONResponse(FeatureCollection):
    type: Literal["FeatureCollection"] = "FeatureCollection"

    @classmethod
    async def from_list(
        cls,
        features: Iterable[dict[str, Any]],
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
