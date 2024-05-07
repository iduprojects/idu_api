"""
Data Transfer Objects (much like entities from database) are defined in this module.
"""
from .users import UserDTO, TokensTuple
from .territories import TerritoryTypeDTO, TerritoryDTO, TerritoryWithoutGeometryDTO
from .services import ServiceDTO, ServiceWithGeometryDTO
from .indicators import IndicatorsDTO, IndicatorValueDTO
from .physical_objects import PhysicalObjectsDataDTO, PhysicalObjectWithGeometryDTO
from .living_buildings import LivingBuildingsWithGeometryDTO
from .functional_zones import FunctionalZoneDataDTO

__all__ = [
    "TerritoryTypeDTO",
    "TerritoryDTO",
    "UserDTO",
    "TokensTuple",
    "ServiceDTO",
    "ServiceWithGeometryDTO",
    "IndicatorsDTO",
    "IndicatorValueDTO",
    "PhysicalObjectsDataDTO",
    "PhysicalObjectWithGeometryDTO",
    "LivingBuildingsWithGeometryDTO",
    "FunctionalZoneDataDTO",
    "TerritoryWithoutGeometryDTO"
]
