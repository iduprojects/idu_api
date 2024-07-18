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
from .normatives import Normative, NormativeDelete, NormativePatch, NormativePost
from .object_geometries import ObjectGeometries, ObjectGeometriesPatch, ObjectGeometriesPost, ObjectGeometriesPut
from .pages import Page
from .physical_objects import (
    PhysicalObjectsData,
    PhysicalObjectsDataPatch,
    PhysicalObjectsDataPost,
    PhysicalObjectsDataPut,
    PhysicalObjectsTypes,
    PhysicalObjectsTypesPost,
    PhysicalObjectsWithTerritory,
    PhysicalObjectWithGeometry,
    PhysicalObjectWithGeometryPost,
)
from .service_types import (
    ServiceTypes,
    ServiceTypesNormativesData,
    ServiceTypesNormativesDataPost,
    ServiceTypesPost,
    UrbanFunction,
    UrbanFunctionPost,
)
from .services import (
    ServicesData,
    ServicesDataPatch,
    ServicesDataPost,
    ServicesDataPut,
    ServicesDataWithGeometry,
    ServiceWithTerritories,
)
from .territories import (
    TerritoryData,
    TerritoryDataPatch,
    TerritoryDataPost,
    TerritoryDataPut,
    TerritoryType,
    TerritoryTypesPost,
    TerritoryWithIndicator,
    TerritoryWithIndicators,
    TerritoryWithNormatives,
    TerritoryWithoutGeometry,
)

__all__ = [
    "PingResponse",
    "TerritoryType",
    "TerritoryTypesPost",
    "TerritoryData",
    "TerritoryDataPost",
    "TerritoryDataPatch",
    "TerritoryDataPut",
    "TerritoryWithIndicator",
    "TerritoryWithIndicators",
    "TerritoryWithNormatives",
    "TerritoryWithoutGeometry",
    "ServicesData",
    "ServicesDataPatch",
    "ServicesDataPost",
    "ServicesDataPut",
    "ServiceWithTerritories",
    "ServiceTypes",
    "ServiceTypesPost",
    "ServiceTypesNormativesData",
    "ServiceTypesNormativesDataPost",
    "Indicator",
    "IndicatorsPost",
    "IndicatorValue",
    "MeasurementUnit",
    "MeasurementUnitPost",
    "Normative",
    "NormativeDelete",
    "NormativePatch",
    "NormativePost",
    "ObjectGeometries",
    "ObjectGeometriesPatch",
    "ObjectGeometriesPost",
    "ObjectGeometriesPut",
    "PhysicalObjectsData",
    "PhysicalObjectsDataPatch",
    "PhysicalObjectsDataPost",
    "PhysicalObjectWithGeometryPost",
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
