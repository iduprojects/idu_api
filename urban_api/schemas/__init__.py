"""
Response and request schemas are defined here.
"""
from .health_check import PingResponse
from .territories import (TerritoryTypes,
                          TerritoryTypesPost,
                          TerritoriesData,
                          TerritoriesDataPost,
                          TerritoryWithoutGeometry
                          )
from .services import ServicesData, ServicesDataWithGeometry
from .indicators import Indicators, IndicatorValue
from .physical_objects import PhysicalObjectsData, PhysicalObjectWithGeometry
from .living_buildings import LivingBuildingsWithGeometry
from .functional_zones import FunctionalZoneData

__all__ = [
    "PingResponse",
    "TerritoryTypes",
    "TerritoryTypesPost",
    "TerritoriesData",
    "TerritoriesDataPost",
    "TerritoryWithoutGeometry",
    "ServicesData",
    "ServicesDataWithGeometry",
    "Indicators",
    "IndicatorValue",
    "PhysicalObjectsData",
    "PhysicalObjectWithGeometry",
    "LivingBuildingsWithGeometry",
    "FunctionalZoneData"
]
