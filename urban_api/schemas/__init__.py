"""
Response and request schemas are defined here.
"""

from .functional_zones import FunctionalZoneData
from .health_check import PingResponse
from .indicators import Indicators, IndicatorsPost, IndicatorValue, MeasurementUnit, MeasurementUnitPost
from .living_buildings import LivingBuildingsWithGeometry
from .pages import Page
from .physical_objects import (
    PhysicalObjectsData,
    PhysicalObjectsTypes,
    PhysicalObjectsTypesPost,
    PhysicalObjectWithGeometry
)
from .services import ServicesData, ServicesDataWithGeometry
from .service_types import (
    ServiceTypes,
    ServiceTypesPost,
    ServiceTypesNormativesData,
    ServiceTypesNormativesDataPost,
    UrbanFunction,
    UrbanFunctionPost
)
from .territories import (
    TerritoriesData,
    TerritoriesDataPost,
    TerritoriesDataPatch,
    TerritoriesDataPut,
    TerritoryTypes,
    TerritoryTypesPost,
    TerritoryWithoutGeometry,
)

__all__ = [
    "PingResponse",
    "TerritoryTypes",
    "TerritoryTypesPost",
    "TerritoriesData",
    "TerritoriesDataPost",
    "TerritoriesDataPatch",
    "TerritoriesDataPut",
    "TerritoryWithoutGeometry",
    "ServicesData",
    "ServicesDataWithGeometry",
    "ServiceTypes",
    "ServiceTypesPost",
    "ServiceTypesNormativesData",
    "ServiceTypesNormativesDataPost",
    "Indicators",
    "IndicatorsPost",
    "IndicatorValue",
    "MeasurementUnit",
    "MeasurementUnitPost",
    "PhysicalObjectsData",
    "PhysicalObjectWithGeometry",
    "PhysicalObjectsTypes",
    "PhysicalObjectsTypesPost",
    "LivingBuildingsWithGeometry",
    "FunctionalZoneData",
    "UrbanFunction",
    "UrbanFunctionPost",
    "Page",
]
