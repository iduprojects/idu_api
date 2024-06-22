"""Territories handlers logic of getting entities from the database is defined here."""

import abc
from datetime import date, datetime
from typing import Callable, List, Literal, Optional, Protocol

import shapely.geometry as geom

from urban_api.dto import (
    FunctionalZoneDataDTO,
    IndicatorDTO,
    IndicatorValueDTO,
    LivingBuildingsWithGeometryDTO,
    PhysicalObjectsDataDTO,
    PhysicalObjectWithGeometryDTO,
    ServiceDTO,
    ServiceWithGeometryDTO,
    TerritoryDTO,
    TerritoryTypeDTO,
    TerritoryWithoutGeometryDTO,
)
from urban_api.schemas import TerritoriesDataPatch, TerritoriesDataPost, TerritoriesDataPut, TerritoryTypesPost

func: Callable


class TerritoriesService(Protocol):
    """Service to manipulate territories objects."""

    @abc.abstractmethod
    async def get_territory_types_from_db(self) -> list[TerritoryDTO]:
        """Get all territory type objects."""

    @abc.abstractmethod
    async def add_territory_type_to_db(self, territory_type: TerritoryTypesPost) -> TerritoryTypeDTO:
        """Create territory type object."""

    @abc.abstractmethod
    async def get_territories_by_id_from_db(self, territory_ids: list[int]) -> List[TerritoryDTO]:
        """Get territory objects by ids list."""

    @abc.abstractmethod
    async def get_territory_by_id_from_db(self, territory_id: int) -> TerritoryDTO:
        """Get territory object by id."""

    @abc.abstractmethod
    async def add_territory_to_db(self, territory: TerritoriesDataPost) -> TerritoryDTO:
        """Create territory object."""

    @abc.abstractmethod
    async def get_services_by_territory_id_from_db(
        self, territory_id: int, service_type_id: Optional[int], name: Optional[str]
    ) -> list[ServiceDTO]:
        """Get service objects by territory id."""

    @abc.abstractmethod
    async def get_services_with_geometry_by_territory_id_from_db(
        self, territory_id: int, service_type_id: Optional[int], name: Optional[str]
    ) -> list[ServiceWithGeometryDTO]:
        """Get service objects with geometry by territory id."""

    @abc.abstractmethod
    async def get_services_capacity_by_territory_id_from_db(
        self, territory_id: int, service_type_id: Optional[int]
    ) -> int:
        """Get aggregated capacity of services by territory id."""

    @abc.abstractmethod
    async def get_indicators_by_territory_id_from_db(self, territory_id: int) -> List[IndicatorDTO]:
        """Get indicators by territory id."""

    async def get_indicator_values_by_territory_id_from_db(
        self, territory_id: int, date_type: Optional[str], date_value: Optional[datetime]
    ) -> List[IndicatorValueDTO]:
        """Get indicator values by territory id, optional time period."""

    @abc.abstractmethod
    async def get_physical_objects_by_territory_id_from_db(
        self, territory_id: int, physical_object_type: Optional[int], name: Optional[str]
    ) -> List[PhysicalObjectsDataDTO]:
        """Get physical objects by territory id, optional physical object type."""

    @abc.abstractmethod
    async def get_physical_objects_with_geometry_by_territory_id_from_db(
        self, territory_id: int, physical_object_type: Optional[int], name: Optional[str]
    ) -> List[PhysicalObjectWithGeometryDTO]:
        """Get physical objects with geometry by territory id, optional physical object type."""

    @abc.abstractmethod
    async def get_living_buildings_with_geometry_by_territory_id_from_db(
        self,
        territory_id: int,
    ) -> List[LivingBuildingsWithGeometryDTO]:
        """Get living buildings with geometry by territory id."""

    @abc.abstractmethod
    async def get_functional_zones_by_territory_id_from_db(
        self, territory_id: int, functional_zone_type_id: Optional[int]
    ) -> List[FunctionalZoneDataDTO]:
        """Get functional zones with geometry by territory id."""

    @abc.abstractmethod
    async def get_territories_by_parent_id_from_db(
        self, parent_id: Optional[int], get_all_levels: Optional[bool], territory_type_id: Optional[int]
    ) -> List[TerritoryDTO]:
        """Get a territory or list of territories by parent, territory type could be specified in parameters."""

    @abc.abstractmethod
    async def get_territories_without_geometry_by_parent_id_from_db(
        self,
        parent_id: Optional[int],
        get_all_levels: bool,
        order_by: Optional[Literal["created_at", "updated_at"]],
        created_at: Optional[date],
        name: Optional[str],
        ordering: Optional[Literal["asc", "desc"]] = "asc",
    ) -> List[TerritoryWithoutGeometryDTO]:
        """Get a territory or list of territories without geometry by parent,
        ordering and filters can be specified in parameters.
        """

    @abc.abstractmethod
    async def get_common_territory_for_geometry(
        self, geometry: geom.Polygon | geom.MultiPolygon | geom.Point
    ) -> TerritoryDTO | None:
        """Get the deepest territory which covers given geometry. None if there is no such territory."""

    @abc.abstractmethod
    async def get_intersecting_territories_for_geometry(
        self, parent_territory: int, geometry: geom.Polygon | geom.MultiPolygon | geom.Point
    ) -> list[TerritoryDTO]:
        """Get all territories of the (level of given parent + 1) which intersect with given geometry."""

    @abc.abstractmethod
    async def put_territory_to_db(self, territory_id: int, territory: TerritoriesDataPut) -> TerritoryDTO:
        """Update territory object (put, update all of the fields)."""

    @abc.abstractmethod
    async def patch_territory_to_db(self, territory_id: int, territory: TerritoriesDataPatch) -> TerritoryDTO:
        """Patch territory object (patch, update only non-None fields)."""
