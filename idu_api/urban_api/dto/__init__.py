"""
Data Transfer Objects (much like entities from database) are defined in this module.
"""

from .functional_zones import FunctionalZoneDataDTO, FunctionalZoneTypeDTO
from .indicators import IndicatorDTO, IndicatorsGroupDTO, IndicatorValueDTO, MeasurementUnitDTO
from .living_buildings import LivingBuildingsDTO, LivingBuildingsWithGeometryDTO
from .normatives import NormativeDTO
from .object_geometries import ObjectGeometryDTO
from .pages import PageDTO
from .physical_objects import (
    PhysicalObjectDataDTO,
    PhysicalObjectTypeDTO,
    PhysicalObjectWithGeometryDTO,
    PhysicalObjectWithTerritoryDTO,
)
from .profiles_reclamation import ProfilesReclamationDataDTO, ProfilesReclamationDataMatrixDTO
from .projects import ProjectDTO, ProjectsIndicatorDTO, ProjectTerritoryDTO
from .scenarios import ScenarioDTO
from .scenarios_urban_objects import ScenarioUrbanObjectDTO
from .service_types import ServiceTypesDTO, ServiceTypesHierarchyDTO, UrbanFunctionDTO
from .services import ServiceDTO, ServicesCountCapacityDTO, ServiceWithGeometryDTO, ServiceWithTerritoriesDTO
from .territories import (
    TerritoryDTO,
    TerritoryTypeDTO,
    TerritoryWithIndicatorDTO,
    TerritoryWithIndicatorsDTO,
    TerritoryWithNormativesDTO,
    TerritoryWithoutGeometryDTO,
)
from .urban_objects import UrbanObjectDTO
from .users import TokensTuple, UserDTO

__all__ = [
    "TerritoryTypeDTO",
    "TerritoryDTO",
    "UserDTO",
    "TokensTuple",
    "ScenarioDTO",
    "ScenarioUrbanObjectDTO",
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
    "PhysicalObjectTypeDTO",
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
    "ProjectsIndicatorDTO",
]
