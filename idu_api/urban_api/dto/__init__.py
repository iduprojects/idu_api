"""
Data Transfer Objects (much like entities from database) are defined in this module.
"""

from .functional_zones import FunctionalZoneDataDTO
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
from .profiles import TargetProfileDTO
from .projects import ProjectDTO, ProjectTerritoryDTO
from .scenarios import ScenarioDTO
from .scenarios_urban_objects import ScenarioUrbanObjectDTO
from .service_types import ServiceTypesDTO, UrbanFunctionDTO
from .services import ServiceDTO, ServiceWithGeometryDTO, ServiceWithTerritoriesDTO
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
    "ServiceDTO",
    "ServiceTypesDTO",
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
    "TerritoryWithIndicatorDTO",
    "TerritoryWithIndicatorsDTO",
    "TerritoryWithNormativesDTO",
    "TerritoryWithoutGeometryDTO",
    "UrbanFunctionDTO",
    "UrbanObjectDTO",
    "ProjectDTO",
    "ProjectTerritoryDTO",
    "TargetProfileDTO",
]
