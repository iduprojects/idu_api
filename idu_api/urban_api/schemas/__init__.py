"""
Response and request schemas are defined here.
"""

from .functional_zones import FunctionalZoneData
from .health_check import PingResponse
from .indicators import (
    Indicator,
    IndicatorsGroup,
    IndicatorsGroupPost,
    IndicatorsPost,
    IndicatorValue,
    IndicatorValuePost,
    MeasurementUnit,
    MeasurementUnitPost,
)
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
from .projects import (
    Project,
    ProjectPatch,
    ProjectPost,
    ProjectPut,
    ProjectTerritory,
    ProjectTerritoryPatch,
    ProjectTerritoryPost,
    ProjectTerritoryPut,
)
from .scenarios import ScenariosData, ScenariosPatch, ScenariosPost, ScenariosPut
from .scenarios_urban_objects import ScenariosUrbanObject
from .service_types import (
    ServiceTypes,
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
    "ScenariosData",
    "ScenariosPatch",
    "ScenariosPost",
    "ScenariosPut",
    "ScenariosUrbanObject",
    "ServicesData",
    "ServicesDataPatch",
    "ServicesDataPost",
    "ServicesDataPut",
    "ServicesDataWithGeometry",
    "ServiceWithTerritories",
    "ServiceTypes",
    "ServiceTypesPost",
    "Indicator",
    "IndicatorsGroup",
    "IndicatorsGroupPost",
    "IndicatorsPost",
    "IndicatorValue",
    "IndicatorValuePost",
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
    "PhysicalObjectsWithTerritory",
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
    "Project",
    "ProjectPost",
    "ProjectPut",
    "ProjectPatch",
    "ProjectTerritory",
    "ProjectTerritoryPost",
    "ProjectTerritoryPut",
    "ProjectTerritoryPatch",
]
