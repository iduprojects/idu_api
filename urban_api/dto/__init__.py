"""
Data Transfer Objects (much like entities from database) are defined in this module.
"""

from .functional_zones import FunctionalZoneDataDTO
from .indicators import IndicatorDTO, IndicatorValueDTO, MeasurementUnitDTO
from .living_buildings import LivingBuildingsDTO, LivingBuildingsWithGeometryDTO
from .object_geometries import ObjectGeometryDTO
from .physical_objects import PhysicalObjectDataDTO, PhysicalObjectTypeDTO, PhysicalObjectWithGeometryDTO
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
    "ObjectGeometryDTO",
    "PhysicalObjectDataDTO",
    "PhysicalObjectTypeDTO",
    "PhysicalObjectWithGeometryDTO",
    "LivingBuildingsDTO",
    "LivingBuildingsWithGeometryDTO",
    "FunctionalZoneDataDTO",
    "TerritoryWithoutGeometryDTO",
    "UrbanFunctionDTO",
]
