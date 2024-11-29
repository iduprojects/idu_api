"""Data Transfer Objects (much like entities from database) are defined in this module."""

from .functional_zones import FunctionalZoneDataDTO, FunctionalZoneSourceDTO, FunctionalZoneTypeDTO, ProjectProfileDTO
from .hexagons import HexagonDTO, HexagonWithIndicatorsDTO
from .indicators import (
    IndicatorDTO,
    IndicatorsGroupDTO,
    IndicatorValueDTO,
    MeasurementUnitDTO,
    ProjectIndicatorValueDTO,
    ShortProjectIndicatorValueDTO,
)
from .living_buildings import LivingBuildingsDTO, LivingBuildingsWithGeometryDTO
from .normatives import NormativeDTO
from .object_geometries import ObjectGeometryDTO, ScenarioGeometryDTO, ScenarioGeometryWithAllObjectsDTO
from .pages import PageDTO
from .physical_object_types import (
    PhysicalObjectFunctionDTO,
    PhysicalObjectTypeDTO,
    PhysicalObjectTypesHierarchyDTO,
)
from .physical_objects import (
    PhysicalObjectDataDTO,
    PhysicalObjectWithGeometryDTO,
    PhysicalObjectWithTerritoryDTO,
    ScenarioPhysicalObjectDTO,
    ShortPhysicalObjectDTO,
    ShortScenarioPhysicalObjectDTO,
)
from .profiles_reclamation import ProfilesReclamationDataDTO, ProfilesReclamationDataMatrixDTO
from .projects import ProjectDTO, ProjectTerritoryDTO
from .scenarios import ScenarioDTO
from .service_types import ServiceTypesDTO, ServiceTypesHierarchyDTO, UrbanFunctionDTO
from .services import (
    ScenarioServiceDTO,
    ServiceDTO,
    ServicesCountCapacityDTO,
    ServiceWithGeometryDTO,
    ServiceWithTerritoriesDTO,
    ShortScenarioServiceDTO,
    ShortServiceDTO,
)
from .territories import (
    TerritoryDTO,
    TerritoryTypeDTO,
    TerritoryWithIndicatorDTO,
    TerritoryWithIndicatorsDTO,
    TerritoryWithNormativesDTO,
    TerritoryWithoutGeometryDTO,
)
from .urban_objects import ScenarioUrbanObjectDTO, UrbanObjectDTO
from .users import TokensTuple, UserDTO

__all__ = [
    "TerritoryTypeDTO",
    "TerritoryDTO",
    "UserDTO",
    "TokensTuple",
    "ScenarioDTO",
    "ServicesCountCapacityDTO",
    "ServiceDTO",
    "ServiceTypesDTO",
    "ServiceTypesHierarchyDTO",
    "ServiceWithGeometryDTO",
    "ServiceWithTerritoriesDTO",
    "IndicatorDTO",
    "IndicatorsGroupDTO",
    "IndicatorValueDTO",
    "MeasurementUnitDTO",
    "NormativeDTO",
    "ObjectGeometryDTO",
    "PageDTO",
    "PhysicalObjectDataDTO",
    "PhysicalObjectFunctionDTO",
    "PhysicalObjectTypeDTO",
    "PhysicalObjectTypesHierarchyDTO",
    "PhysicalObjectWithGeometryDTO",
    "PhysicalObjectWithTerritoryDTO",
    "LivingBuildingsDTO",
    "LivingBuildingsWithGeometryDTO",
    "FunctionalZoneDataDTO",
    "FunctionalZoneTypeDTO",
    "TerritoryWithIndicatorDTO",
    "TerritoryWithIndicatorsDTO",
    "TerritoryWithNormativesDTO",
    "TerritoryWithoutGeometryDTO",
    "UrbanFunctionDTO",
    "UrbanObjectDTO",
    "ProjectDTO",
    "ProjectTerritoryDTO",
    "ProfilesReclamationDataDTO",
    "ProfilesReclamationDataMatrixDTO",
    "ProjectIndicatorValueDTO",
    "ProjectProfileDTO",
    "ShortScenarioPhysicalObjectDTO",
    "ScenarioPhysicalObjectDTO",
    "ShortScenarioServiceDTO",
    "ScenarioServiceDTO",
    "ScenarioGeometryWithAllObjectsDTO",
    "ScenarioGeometryDTO",
    "ShortServiceDTO",
    "ShortPhysicalObjectDTO",
    "HexagonDTO",
    "HexagonWithIndicatorsDTO",
    "ShortProjectIndicatorValueDTO",
    "ScenarioUrbanObjectDTO",
    "FunctionalZoneSourceDTO",
]
