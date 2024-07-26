from dataclasses import dataclass
from typing import Optional, Dict, Any

import shapely.geometry as geom


@dataclass()
class CityServiceDTO:
    id: int
    name: Optional[str]
    capacity: Optional[int]
    properties: Dict[str, Any]
    geometry: geom.Polygon | geom.MultiPolygon | geom.Point
    centre_point: geom.Point

    def __post_init__(self) -> None:
        if isinstance(self.centre_point, dict):
            self.centre_point = geom.shape(self.centre_point)
        if self.geometry is None:
            self.geometry = self.centre_point
        if isinstance(self.geometry, dict):
            self.geometry = geom.shape(self.geometry)

    def as_dict(self, attribute_mapper: dict[str, str], exclude: list[str]) -> dict:
        result = {}
        for key, value in self.__dict__.items():
            if exclude is not None and key in exclude:
                continue
            if attribute_mapper is not None and key in attribute_mapper:
                result[attribute_mapper[key]] = value
            else:
                if key != "properties" and key in self.__annotations__.keys():
                    result[key] = value
        if self.__dict__["properties"] is not None:
            for key, value in self.__dict__["properties"].items():
                if key in self.__annotations__.keys():
                    result[key] = value
        return result
