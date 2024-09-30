from typing import Any

from geojson import FeatureCollection

from idu_api.city_api.dto.base import Base
from idu_api.urban_api.schemas.geometries import Geometry
import shapely.geometry as geom


class Feature:
    @staticmethod
    async def generate_feature(
            geometry: geom.Point | geom.Polygon | geom.MultiPolygon,
            properties: dict = {}
    ) -> dict[str, Any]:
        return {
            "type": "Feature",
            "geometry": Geometry.from_shapely_geometry(geometry),
            "properties": properties
        }

    @staticmethod
    async def generate_feature_collection(
            features: list[Base],
            geometry_column: str,
            alternative_geometry_column: str,
            attribute_mapper: dict[str, str],
            exclude: list[str]
    ) -> FeatureCollection:
        return FeatureCollection([
            await Feature.generate_feature(
                elem.__dict__[alternative_geometry_column],
                elem.as_dict(attribute_mapper, exclude)
            ) if elem.__dict__[geometry_column] is None
            else await Feature.generate_feature(
                elem.__dict__[geometry_column],
                elem.as_dict(attribute_mapper, exclude)
            )
            for elem in features
        ])
