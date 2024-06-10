"""
Data Transfer Objects (much like entities from database) are defined in this module.
"""

from .functional_zones import FunctionalZoneDataDTO
from .indicators import IndicatorDTO, IndicatorValueDTO, MeasurementUnitDTO
from .living_buildings import LivingBuildingsWithGeometryDTO
from .physical_objects import PhysicalObjectsDataDTO, PhysicalObjectsTypesDTO, PhysicalObjectWithGeometryDTO
from .service_types import ServiceTypesDTO, ServiceTypesNormativesDTO, UrbanFunctionDTO
from .services import ServiceDTO, ServiceWithGeometryDTO
from .territories import TerritoryDTO, TerritoryTypeDTO, TerritoryWithoutGeometryDTO
from .users import TokensTuple, UserDTO

__all__ = [
    "TerritoryTypeDTO",
    "TerritoryDTO",
    "UserDTO",
    "TokensTuple",
    "ServiceDTO",
    "ServiceTypesDTO",
    "ServiceTypesNormativesDTO",
    "ServiceWithGeometryDTO",
    "IndicatorDTO",
    "IndicatorValueDTO",
    "MeasurementUnitDTO",
    "PhysicalObjectsDataDTO",
    "PhysicalObjectsTypesDTO",
    "PhysicalObjectWithGeometryDTO",
    "LivingBuildingsWithGeometryDTO",
    "FunctionalZoneDataDTO",
    "TerritoryWithoutGeometryDTO",
    "UrbanFunctionDTO",
]
