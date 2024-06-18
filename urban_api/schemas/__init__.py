"""
Response and request schemas are defined here.
"""

from .functional_zones import FunctionalZoneData
from .health_check import PingResponse
from .indicators import Indicator, IndicatorsPost, IndicatorValue, MeasurementUnit, MeasurementUnitPost
from .living_buildings import (
    LivingBuildingsData,
    LivingBuildingsDataPatch,
    LivingBuildingsDataPost,
    LivingBuildingsDataPut,
    LivingBuildingsWithGeometry,
)
from .object_geometries import ObjectGeometries, ObjectGeometriesPatch, ObjectGeometriesPut
from .pages import Page
from .physical_objects import (
    PhysicalObjectsData,
    PhysicalObjectsDataPatch,
    PhysicalObjectsDataPost,
    PhysicalObjectsDataPut,
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
from .services import ServicesData, ServicesDataPatch, ServicesDataPost, ServicesDataPut, ServicesDataWithGeometry
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
    "ServicesDataPatch",
    "ServicesDataPost",
    "ServicesDataPut",
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
    "ObjectGeometriesPatch",
    "ObjectGeometriesPut",
    "PhysicalObjectsData",
    "PhysicalObjectsDataPatch",
    "PhysicalObjectsDataPost",
    "PhysicalObjectsDataPut",
    "PhysicalObjectWithGeometry",
    "PhysicalObjectsTypes",
    "PhysicalObjectsTypesPost",
    "LivingBuildingsData",
    "LivingBuildingsDataPatch",
    "LivingBuildingsDataPost",
    "LivingBuildingsDataPut",
    "LivingBuildingsWithGeometry",
    "FunctionalZoneData",
    "UrbanFunction",
    "UrbanFunctionPost",
    "Page",
]
