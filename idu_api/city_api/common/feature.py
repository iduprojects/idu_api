from idu_api.urban_api.schemas.geometries import Geometry
import shapely.geometry as geom


class Feature:
    @staticmethod
    async def generate_feature(
            geometry: geom.Point | geom.Polygon | geom.MultiPolygon,
            properties: dict = {}
    ) -> dict[str, dict | str]:
        return {
            "type": "Feature",
            "geometry": Geometry.from_shapely_geometry(geometry),
            "properties": properties
        }
