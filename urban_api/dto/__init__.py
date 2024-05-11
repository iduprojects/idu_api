"""
Data Transfer Objects (much like entities from database) are defined in this module.
"""

from .functional_zones import FunctionalZoneDataDTO
from .indicators import IndicatorsDTO, IndicatorValueDTO
from .living_buildings import LivingBuildingsWithGeometryDTO
from .physical_objects import PhysicalObjectsDataDTO, PhysicalObjectWithGeometryDTO
from .services import ServiceDTO, ServiceWithGeometryDTO
from .territories import TerritoryDTO, TerritoryTypeDTO, TerritoryWithoutGeometryDTO
from .users import TokensTuple, UserDTO

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
    "TerritoryWithoutGeometryDTO",
]
