"""
Response and request schemas are defined here.
"""

from .functional_zones import FunctionalZoneData
from .health_check import PingResponse
from .indicators import Indicator, IndicatorsPost, IndicatorValue, MeasurementUnit, MeasurementUnitPost
from .living_buildings import LivingBuildingsData, LivingBuildingsDataPost, LivingBuildingsWithGeometry
from .object_geometries import ObjectGeometries
from .pages import Page
from .physical_objects import (
    PhysicalObjectsData,
    PhysicalObjectsDataPost,
    PhysicalObjectsTypes,
    PhysicalObjectsTypesPost,
    PhysicalObjectWithGeometry,
)
from .service_types import (
    ServiceTypes,
    ServiceTypesNormativesData,
    ServiceTypesNormativesDataPost,
    ServiceTypesPost,
    UrbanFunction,
    UrbanFunctionPost,
)
from .services import ServicesData, ServicesDataPost, ServicesDataWithGeometry
from .territories import (
    TerritoriesData,
    TerritoriesDataPatch,
    TerritoriesDataPost,
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
    "ServicesDataPost",
    "ServicesDataWithGeometry",
    "ServiceTypes",
    "ServiceTypesPost",
    "ServiceTypesNormativesData",
    "ServiceTypesNormativesDataPost",
    "Indicator",
    "IndicatorsPost",
    "IndicatorValue",
    "MeasurementUnit",
    "MeasurementUnitPost",
    "ObjectGeometries",
    "PhysicalObjectsData",
    "PhysicalObjectsDataPost",
    "PhysicalObjectWithGeometry",
    "PhysicalObjectsTypes",
    "PhysicalObjectsTypesPost",
    "LivingBuildingsData",
    "LivingBuildingsDataPost",
    "LivingBuildingsWithGeometry",
    "FunctionalZoneData",
    "UrbanFunction",
    "UrbanFunctionPost",
    "Page",
]
