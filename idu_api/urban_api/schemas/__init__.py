"""
Response and request schemas are defined here.
"""

from .functional_zones import (
    FunctionalZoneData,
    FunctionalZoneDataPatch,
    FunctionalZoneDataPost,
    FunctionalZoneDataPut,
    FunctionalZoneType,
    FunctionalZoneTypePost,
)
from .health_check import PingResponse
from .indicators import (
    Indicator,
    IndicatorsGroup,
    IndicatorsGroupPost,
    IndicatorsPatch,
    IndicatorsPost,
    IndicatorsPut,
    IndicatorValue,
    IndicatorValuePost,
    MeasurementUnit,
    MeasurementUnitPost,
    ProjectsIndicatorValue,
    ProjectsIndicatorValuePatch,
    ProjectsIndicatorValuePost,
    ProjectsIndicatorValuePut,
)
from .living_buildings import (
    LivingBuildingsData,
    LivingBuildingsDataPatch,
    LivingBuildingsDataPost,
    LivingBuildingsDataPut,
    LivingBuildingsWithGeometry,
)
from .minio import MinioImagesURL, MinioImageURL
from .normatives import Normative, NormativeDelete, NormativePatch, NormativePost
from .object_geometries import (
    AllObjects,
    GeometryAttributes,
    ObjectGeometries,
    ObjectGeometriesPatch,
    ObjectGeometriesPost,
    ObjectGeometriesPut,
    ScenarioAllObjects,
    ScenarioGeometryAttributes,
    ScenarioObjectGeometry,
)
from .pages import Page
from .physical_object_types import (
    PhysicalObjectFunction,
    PhysicalObjectFunctionPatch,
    PhysicalObjectFunctionPost,
    PhysicalObjectFunctionPut,
    PhysicalObjectsTypes,
    PhysicalObjectsTypesHierarchy,
    PhysicalObjectsTypesPatch,
    PhysicalObjectsTypesPost,
)
from .physical_objects import (
    PhysicalObjectsData,
    PhysicalObjectsDataPatch,
    PhysicalObjectsDataPost,
    PhysicalObjectsDataPut,
    PhysicalObjectsWithTerritory,
    PhysicalObjectWithGeometry,
    PhysicalObjectWithGeometryPost,
    ScenarioPhysicalObject,
)
from .profiles_reclamation import (
    ProfilesReclamationData,
    ProfilesReclamationDataMatrix,
    ProfilesReclamationDataPost,
    ProfilesReclamationDataPut,
)
from .projects import (
    Project,
    ProjectPatch,
    ProjectPost,
    ProjectPut,
    ProjectTerritory,
    ProjectTerritoryPost,
)
from .scenarios import ScenariosData, ScenariosPatch, ScenariosPost, ScenariosPut
from .service_types import (
    ServiceTypes,
    ServiceTypesHierarchy,
    ServiceTypesPatch,
    ServiceTypesPost,
    ServiceTypesPut,
    UrbanFunction,
    UrbanFunctionPatch,
    UrbanFunctionPost,
    UrbanFunctionPut,
)
from .services import (
    ScenarioService,
    ScenarioServicePost,
    ServicesCountCapacity,
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
    "ServicesCountCapacity",
    "ServicesData",
    "ServicesDataPatch",
    "ServicesDataPost",
    "ScenarioServicePost",
    "ServicesDataPut",
    "ServicesDataWithGeometry",
    "ServiceWithTerritories",
    "ServiceTypes",
    "ServiceTypesHierarchy",
    "ServiceTypesPatch",
    "ServiceTypesPost",
    "ServiceTypesPut",
    "Indicator",
    "IndicatorsGroup",
    "IndicatorsGroupPost",
    "IndicatorsPatch",
    "IndicatorsPost",
    "IndicatorsPut",
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
    "PhysicalObjectFunction",
    "PhysicalObjectFunctionPatch",
    "PhysicalObjectFunctionPost",
    "PhysicalObjectFunctionPut",
    "PhysicalObjectsTypes",
    "PhysicalObjectsTypesHierarchy",
    "PhysicalObjectsTypesPatch",
    "PhysicalObjectsTypesPost",
    "LivingBuildingsData",
    "LivingBuildingsDataPatch",
    "LivingBuildingsDataPost",
    "LivingBuildingsDataPut",
    "LivingBuildingsWithGeometry",
    "FunctionalZoneData",
    "FunctionalZoneDataPost",
    "FunctionalZoneDataPut",
    "FunctionalZoneDataPatch",
    "FunctionalZoneType",
    "FunctionalZoneTypePost",
    "UrbanFunction",
    "UrbanFunctionPatch",
    "UrbanFunctionPost",
    "UrbanFunctionPut",
    "Page",
    "Project",
    "ProjectPost",
    "ProjectPut",
    "ProjectPatch",
    "ProjectTerritory",
    "ProjectTerritoryPost",
    "ProfilesReclamationData",
    "ProfilesReclamationDataPost",
    "ProfilesReclamationDataPut",
    "ProfilesReclamationDataMatrix",
    "ProjectsIndicatorValue",
    "ProjectsIndicatorValuePatch",
    "ProjectsIndicatorValuePost",
    "ProjectsIndicatorValuePut",
    "MinioImageURL",
    "MinioImagesURL",
    "GeometryAttributes",
    "ScenarioGeometryAttributes",
    "AllObjects",
    "ScenarioAllObjects",
    "ScenarioPhysicalObject",
    "ScenarioService",
    "ScenarioObjectGeometry",
]
