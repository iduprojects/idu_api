"""Data Transfer Objects (much like entities from database) are defined in this module."""

from .buildings import BuildingDTO, BuildingWithGeometryDTO
from .functional_zones import (
    FunctionalZoneDTO,
    FunctionalZoneSourceDTO,
    FunctionalZoneTypeDTO,
    ScenarioFunctionalZoneDTO,
)
from .hexagons import HexagonDTO, HexagonWithIndicatorsDTO
from .indicators import (
    IndicatorDTO,
    IndicatorsGroupDTO,
    IndicatorValueDTO,
    MeasurementUnitDTO,
    ScenarioIndicatorValueDTO,
    ShortScenarioIndicatorValueDTO,
)
from .normatives import NormativeDTO
from .object_geometries import ObjectGeometryDTO, ScenarioGeometryDTO, ScenarioGeometryWithAllObjectsDTO
from .pages import PageDTO
from .physical_object_types import (
    PhysicalObjectFunctionDTO,
    PhysicalObjectTypeDTO,
    PhysicalObjectTypesHierarchyDTO,
)
from .physical_objects import (
    PhysicalObjectDTO,
    PhysicalObjectWithGeometryDTO,
    ScenarioPhysicalObjectDTO,
    ShortPhysicalObjectDTO,
    ShortScenarioPhysicalObjectDTO,
)
from .profiles_reclamation import ProfilesReclamationDataDTO, ProfilesReclamationDataMatrixDTO
from .projects import ProjectDTO, ProjectTerritoryDTO, ProjectWithTerritoryDTO
from .scenarios import ScenarioDTO
from .service_types import ServiceTypeDTO, ServiceTypesHierarchyDTO, UrbanFunctionDTO
from .services import (
    ScenarioServiceDTO,
    ServiceDTO,
    ServicesCountCapacityDTO,
    ServiceWithGeometryDTO,
    ShortScenarioServiceDTO,
    ShortServiceDTO,
)
from .soc_groups import (
    SocGroupDTO,
    SocGroupIndicatorValueDTO,
    SocGroupWithServiceTypesDTO,
    SocValueDTO,
    SocValueWithSocGroupsDTO,
)
from .territories import (
    TargetCityTypeDTO,
    TerritoryDTO,
    TerritoryTreeWithoutGeometryDTO,
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
    "ServiceTypeDTO",
    "ServiceTypesHierarchyDTO",
    "ServiceWithGeometryDTO",
    "IndicatorDTO",
    "IndicatorsGroupDTO",
    "IndicatorValueDTO",
    "MeasurementUnitDTO",
    "NormativeDTO",
    "ObjectGeometryDTO",
    "PageDTO",
    "PhysicalObjectDTO",
    "PhysicalObjectFunctionDTO",
    "PhysicalObjectTypeDTO",
    "PhysicalObjectTypesHierarchyDTO",
    "PhysicalObjectWithGeometryDTO",
    "BuildingDTO",
    "BuildingWithGeometryDTO",
    "FunctionalZoneDTO",
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
    "ScenarioIndicatorValueDTO",
    "ScenarioFunctionalZoneDTO",
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
    "ShortScenarioIndicatorValueDTO",
    "ScenarioUrbanObjectDTO",
    "FunctionalZoneSourceDTO",
    "ProjectWithTerritoryDTO",
    "TargetCityTypeDTO",
    "SocGroupDTO",
    "SocGroupWithServiceTypesDTO",
    "SocValueDTO",
    "SocValueWithSocGroupsDTO",
    "SocGroupIndicatorValueDTO",
]
