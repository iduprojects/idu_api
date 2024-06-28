"""
Geojson response models is defined here.
"""

import json
from typing import Any, Generic, Iterable, Literal, Optional, TypeVar

import pandas as pd
import shapely.geometry as geom
from loguru import logger
from pydantic import BaseModel, Field
from sqlalchemy.engine.row import Row

FeaturePropertiesType = TypeVar("FeaturePropertiesType")  # pylint: disable=invalid-name


class Crs(BaseModel):
    """
    Projection SRID / CRS representation for GeoJSON model.
    """

    type: str
    properties: dict[str, Any]

    @property
    def code(self) -> int:
        """
        Return code of the projection. Would work only if CRS properties is set as name: ...<code>.
        """
        name: str = self.properties["name"]
        try:
            return int(name[name.rindex(":") + 1 :]) if ":" in name else int(name)
        except Exception as exc:
            logger.debug("Crs {} code is invalid? {!r}", self, exc)
            raise ValueError(f"something wrong with crs name: '{name}'") from exc


crs_4326 = Crs(type="name", properties={"name": "urn:ogc:def:crs:EPSG:4326"})
crs_3857 = Crs(type="name", properties={"name": "urn:ogc:def:crs:EPSG:3857"})


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


class Feature(BaseModel, Generic[FeaturePropertiesType]):
    """
    Feature representation for GeoJSON model.
    """

    type: Literal["Feature"] = "Feature"
    geometry: Geometry
    properties: FeaturePropertiesType = Field(default_factory=lambda: {})

    @classmethod
    def from_series(
        cls,
        series: pd.Series,
        geometry_column: str = "geometry",
        include_nulls: bool = True,
    ) -> "Feature[FeaturePropertiesType]":
        """
        Construct Feature object from series with a given geometrty column.
        """
        properties = series.to_dict()
        if not include_nulls:
            properties = {name: value for name, value in properties.items() if value is not None}
        geometry = properties[geometry_column]
        del properties[geometry_column]
        if isinstance(geometry, str):
            geometry = json.loads(geometry)
        return cls(geometry=geometry, properties=properties)

    @classmethod
    def from_dict(
        cls,
        feature: dict[str, Any],
        geometry_column: str = "geometry",
        include_nulls: bool = True,
    ) -> "Feature[FeaturePropertiesType]":
        """
        Construct Feature object from dictionary with a given geometrty field.
        """
        properties = dict(feature)
        if not include_nulls:
            properties = {name: value for name, value in properties.items() if value is not None}
        geometry = properties[geometry_column]
        del properties[geometry_column]
        if isinstance(geometry, str):
            geometry = json.loads(geometry)
        return cls(geometry=geometry, properties=properties)

    @classmethod
    def from_row(
        cls,
        row: dict[str, Any],
        geometry_column: str = "geometry",
        include_nulls: bool = True,
    ) -> "Feature[FeaturePropertiesType]":
        """
        Construct Feature object from dictionary with a given geometrty field.
        """
        geometry = row[geometry_column]
        if isinstance(geometry, str):
            geometry = json.loads(geometry)

        if include_nulls:
            properties = {name: row[name] for name in row.keys() if name != geometry_column}
        else:
            properties = {name: row[name] for name in row.keys() if name != geometry_column and row[name] is not None}

        return cls(geometry=geometry, properties=properties)


class GeoJSONResponse(BaseModel, Generic[FeaturePropertiesType]):
    """
    GeoJSON model representation.
    """

    crs: Crs
    type: Literal["FeatureCollection"] = "FeatureCollection"
    features: list[Feature[FeaturePropertiesType]]

    @classmethod
    async def from_df(
        cls,
        data_df: pd.DataFrame,
        geometry_column: str = "geometry",
        crs: Crs = crs_4326,
        include_nulls: bool = True,
    ) -> "GeoJSONResponse[FeaturePropertiesType]":
        """
        Construct GeoJSON model from pandas DataFrame with one column containing GeoJSON geometries.
        """
        return cls(
            crs=crs,
            features=list(
                data_df.apply(
                    lambda row: Feature.from_series(row, geometry_column, include_nulls),
                    axis=1,
                )
            ),
        )

    @classmethod
    async def from_list(
        cls,
        features: Iterable[dict[str, Any]],
        geometry_field: str = "geometry",
        crs: Crs = crs_4326,
        include_nulls: bool = True,
    ) -> "GeoJSONResponse[FeaturePropertiesType]":
        """
        Construct GeoJSON model from list of dictionaries or SQLAlchemy Row classes from the database,
        with one field in each containing GeoJSON geometries.
        """
        func = Feature.from_row if isinstance(next(iter(features), None), Row) else Feature.from_dict
        features = [
            func(feature, geometry_field, include_nulls) for feature in features
        ]  # TODO: move it to another process to increase performance
        return cls(
            crs=crs,
            features=features,
        )
