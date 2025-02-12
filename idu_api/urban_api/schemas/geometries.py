"""Geojson response models are defined here."""

import json
from collections.abc import Iterable
from typing import Any, Literal, Self, TypeVar

import shapely
import shapely.geometry as geom
import structlog
from geojson_pydantic import Feature, FeatureCollection
from pydantic import BaseModel, Field, field_validator, model_validator

_BaseGeomTypes = geom.Point | geom.Polygon | geom.MultiPolygon | geom.LineString | geom.MultiLineString

_logger: structlog.stdlib.BoundLogger = structlog.get_logger("geometry_schemas")


class AllPossibleGeometry(BaseModel):
    """Geometry representation for GeoJSON model (all valid geometry types )."""

    type: Literal[
        "Point", "Polygon", "MultiPolygon", "LineString", "MultiPoint", "MultiLineString", "GeometryCollection"
    ] = Field(examples=["Polygon"])
    coordinates: list[Any] = Field(
        description="list[float] for Point,\n" "list[list[list[float]]] for Polygon",
        examples=[
            [
                [
                    [30.22, 59.86],
                    [30.22, 59.85],
                    [30.25, 59.85],
                    [30.25, 59.86],
                    [30.22, 59.86],
                ]
            ]
        ],
    )
    _shapely_geom: _BaseGeomTypes | geom.MultiPoint | geom.MultiLineString | geom.GeometryCollection | None = None

    def as_shapely_geometry(
        self,
    ) -> _BaseGeomTypes | geom.MultiPoint | geom.MultiLineString | geom.GeometryCollection:
        """Return Shapely geometry object from the parsed geometry."""
        if self._shapely_geom is None:
            self._shapely_geom = shapely.from_geojson(json.dumps({"type": self.type, "coordinates": self.coordinates}))
        return self._shapely_geom

    @classmethod
    def from_shapely_geometry(
        cls, geometry: _BaseGeomTypes | geom.MultiPoint | geom.MultiLineString | geom.GeometryCollection | None
    ) -> Self | None:
        """Construct Geometry model from shapely geometry."""
        if geometry is None:
            return None
        return cls(**geom.mapping(geometry))


class Geometry(BaseModel):
    """
    Geometry representation for GeoJSON model appliable for points, polygons, multipolygons and linestrings.
    """

    type: Literal["Point", "Polygon", "MultiPolygon", "LineString", "MultiLineString"] = Field(examples=["Polygon"])
    coordinates: list[Any] = Field(
        description="list[float] for Point,\n" "list[list[list[float]]] for Polygon",
        examples=[
            [
                [
                    [30.22, 59.86],
                    [30.22, 59.85],
                    [30.25, 59.85],
                    [30.25, 59.86],
                    [30.22, 59.86],
                ]
            ]
        ],
    )
    _shapely_geom: _BaseGeomTypes | None = None

    def as_shapely_geometry(self) -> _BaseGeomTypes:
        """
        Return Shapely geometry object from the parsed geometry.
        """
        if self._shapely_geom is None:
            self._shapely_geom = shapely.from_geojson(json.dumps({"type": self.type, "coordinates": self.coordinates}))
        return self._shapely_geom

    @classmethod
    def from_shapely_geometry(cls, geometry: _BaseGeomTypes | None) -> Self | None:
        """
        Construct Geometry model from shapely geometry.
        """
        if geometry is None:
            return None
        return cls(**geom.mapping(geometry))


T = TypeVar("T", bound="GeometryValidationModel")


class GeometryValidationModel(BaseModel):
    """Base model with geometry validation methods."""

    geometry: Geometry | None = None
    centre_point: Geometry | None = None

    @classmethod
    @field_validator("geometry")
    def validate_geometry(cls, geometry: "Geometry") -> "Geometry":
        """Validate that given geometry is valid by creating a Shapely object."""
        if geometry:
            try:
                geometry.as_shapely_geometry()
            except (AttributeError, ValueError, TypeError) as exc:
                _logger.debug("Exception on passing geometry: {!r}", exc)
                raise ValueError("Invalid geometry passed") from exc
        return geometry

    @classmethod
    @field_validator("centre_point")
    def validate_centre_point(cls, centre_point: Geometry | None) -> Geometry | None:
        """Validate that given centre_point is a valid Point geometry."""
        if centre_point:
            if centre_point.type != "Point":
                raise ValueError("Only Point geometry is accepted for centre_point")
            try:
                centre_point.as_shapely_geometry()
            except (AttributeError, ValueError, TypeError) as exc:
                _logger.debug("Exception on passing geometry: {!r}", exc)
                raise ValueError("Invalid centre_point passed") from exc
        return centre_point

    @model_validator(mode="after")
    @classmethod
    def validate_centre_point_from_geometry(cls: type[T], model: T) -> T:
        """Use the geometry's centroid for centre_point if it is missing."""
        if model.centre_point is None and model.geometry:
            model.centre_point = Geometry.from_shapely_geometry(model.geometry.as_shapely_geometry().centroid)
        return model


class NotPointGeometryValidationModel(BaseModel):
    """Base model with geometry validation methods (without points)."""

    geometry: Geometry | None = None

    @field_validator("geometry")
    @classmethod
    def validate_geometry(cls, geometry: "Geometry") -> "Geometry":
        """Validate that given geometry is valid by creating a Shapely object."""
        if geometry:
            try:
                geometry.as_shapely_geometry()
            except (AttributeError, ValueError, TypeError) as exc:
                _logger.debug("Exception on passing geometry: {!r}", exc)
                raise ValueError("Invalid geometry passed") from exc
        return geometry


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
            properties = feature.copy()
            if not centers_only:
                geometry = properties.pop("geometry", None)
                if properties.get("centre_point", False):
                    del properties["centre_point"]
            else:
                geometry = properties.pop("centre_point", None)
                if properties.get("geometry", False):
                    del properties["geometry"]

            feature_collection.append(Feature(type="Feature", geometry=geometry, properties=properties))

        return cls(features=feature_collection)

    def update_geometries(self, new_geoms: list[_BaseGeomTypes]) -> "GeoJSONResponse":
        """
        Updates the geometries in each Feature, accepting a list of new Shapely geometries.
        The number of new geometries must match the number of features.
        """
        if len(new_geoms) != len(self.features):
            raise ValueError(
                f"Number of new geometries ({len(new_geoms)}) does not match the number of features ({len(self.features)})"
            )

        updated_features = [
            feature.copy(update={"geometry": Geometry.from_shapely_geometry(new_geom)})
            for feature, new_geom in zip(self.features, new_geoms)
        ]

        return GeoJSONResponse(features=updated_features)
