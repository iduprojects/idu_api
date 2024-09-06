"""Territories handlers logic of getting entities from the database is defined here."""

import abc
from datetime import datetime
from typing import Literal, Optional, Protocol

import shapely.geometry as geom

from idu_api.urban_api.dto import (
    FunctionalZoneDataDTO,
    IndicatorDTO,
    IndicatorValueDTO,
    LivingBuildingsWithGeometryDTO,
    NormativeDTO,
    PageDTO,
    PhysicalObjectDataDTO,
    PhysicalObjectWithGeometryDTO,
    ServiceDTO,
    ServiceWithGeometryDTO,
    TerritoryDTO,
    TerritoryTypeDTO,
    TerritoryWithIndicatorsDTO,
    TerritoryWithNormativesDTO,
    TerritoryWithoutGeometryDTO,
)
from idu_api.urban_api.schemas import (
    NormativeDelete,
    NormativePatch,
    NormativePost,
    TerritoryDataPatch,
    TerritoryDataPost,
    TerritoryDataPut,
    TerritoryTypesPost,
)


class TerritoriesService(Protocol):  # pylint: disable=too-many-public-methods
    """Service to manipulate territories objects."""

    @abc.abstractmethod
    async def get_territory_types(self) -> list[TerritoryTypeDTO]:
        """Get all territory type objects."""

    @abc.abstractmethod
    async def add_territory_type(self, territory_type: TerritoryTypesPost) -> TerritoryTypeDTO:
        """Create territory type object."""

    @abc.abstractmethod
    async def get_territories_by_ids(self, territory_ids: list[int]) -> list[TerritoryDTO]:
        """Get territory objects by ids list."""

    @abc.abstractmethod
    async def get_territory_by_id(self, territory_id: int) -> TerritoryDTO:
        """Get territory object by id."""

    @abc.abstractmethod
    async def add_territory(self, territory: TerritoryDataPost) -> TerritoryDTO:
        """Create territory object."""

    @abc.abstractmethod
    async def put_territory(self, territory_id: int, territory: TerritoryDataPut) -> TerritoryDTO:
        """Update territory object (put, update all of the fields)."""

    @abc.abstractmethod
    async def patch_territory(self, territory_id: int, territory: TerritoryDataPatch) -> TerritoryDTO:
        """Patch territory object (patch, update only non-None fields)."""

    @abc.abstractmethod
    async def get_services_by_territory_id(
        self,
        territory_id: int,
        service_type_id: int | None,
        name: str | None,
        order_by: Optional[Literal["created_at", "updated_at"]],
        ordering: Optional[Literal["asc", "desc"]] = "asc",
        paginate: bool = False,
    ) -> list[ServiceDTO] | PageDTO[ServiceDTO]:
        """Get service objects by territory id."""

    @abc.abstractmethod
    async def get_services_with_geometry_by_territory_id(
        self,
        territory_id: int,
        service_type_id: int | None,
        name: str | None,
        order_by: Optional[Literal["created_at", "updated_at"]],
        ordering: Optional[Literal["asc", "desc"]] = "asc",
        paginate: bool = False,
    ) -> list[ServiceWithGeometryDTO] | PageDTO[ServiceWithGeometryDTO]:
        """Get service objects with geometry by territory id."""

    @abc.abstractmethod
    async def get_services_capacity_by_territory_id(self, territory_id: int, service_type_id: int | None) -> int:
        """Get aggregated capacity of services by territory id."""

    @abc.abstractmethod
    async def get_indicators_by_territory_id(self, territory_id: int) -> list[IndicatorDTO]:
        """Get indicators by territory id."""

    @abc.abstractmethod
    async def get_indicator_values_by_territory_id(
        self,
        territory_id: int,
        indicator_ids: str | None,
        start_date: datetime | None,
        end_date: datetime | None,
        value_type: Optional[Literal["real", "target", "forecast"]],
        information_source: str | None,
        last_only: bool,
    ) -> list[IndicatorValueDTO]:
        """Get indicator values by territory id, optional indicator_ids, value_type, source and time period.

        Could be specified by last_only flag to get only current indicator values.
        """

    @abc.abstractmethod
    async def get_indicator_values_by_parent_id(
        self,
        parent_id: int | None,
        indicator_ids: str | None,
        start_date: datetime | None,
        end_date: datetime | None,
        value_type: Optional[Literal["real", "target", "forecast"]],
        information_source: str | None,
        last_only: bool,
    ) -> list[TerritoryWithIndicatorsDTO]:
        """Get indicator values for child territories by parent id, optional indicator_ids, value_type, source and date.

        Could be specified by last_only flag to get only current indicator values.
        """

    @abc.abstractmethod
    async def get_normatives_by_territory_id(self, territory_id: int, year: int) -> list[NormativeDTO]:
        """Get normatives by territory id and year"""

    @abc.abstractmethod
    async def add_normatives_to_territory(
        self, territory_id: int, normatives: list[NormativePost]
    ) -> list[NormativeDTO]:
        """Add normatives to territory"""

    @abc.abstractmethod
    async def put_normatives_by_territory_id(
        self, territory_id: int, normatives: list[NormativePost]
    ) -> list[NormativeDTO]:
        """Put normatives by territory id"""

    @abc.abstractmethod
    async def patch_normatives_by_territory_id(
        self, territory_id: int, normatives: list[NormativePatch]
    ) -> list[NormativeDTO]:
        """Patch normatives by territory id"""

    @abc.abstractmethod
    async def delete_normatives_by_territory_id(self, territory_id: int, normatives: list[NormativeDelete]) -> dict:
        """Delete normatives by territory id"""

    @abc.abstractmethod
    async def get_normatives_values_by_parent_id(
        self,
        parent_id: int | None,
        year: int,
    ) -> list[TerritoryWithNormativesDTO]:
        """Get list of normatives with values for territory by parent id and year."""

    @abc.abstractmethod
    async def get_physical_objects_by_territory_id(
        self,
        territory_id: int,
        physical_object_type: int | None,
        name: str | None,
        order_by: Optional[Literal["created_at", "updated_at"]],
        ordering: Optional[Literal["asc", "desc"]] = "asc",
        paginate: bool = False,
    ) -> list[PhysicalObjectDataDTO] | PageDTO[PhysicalObjectDataDTO]:
        """Get physical objects by territory id, optional physical object type."""

    @abc.abstractmethod
    async def get_physical_objects_with_geometry_by_territory_id(
        self,
        territory_id: int,
        physical_object_type: int | None,
        name: str | None,
        order_by: Optional[Literal["created_at", "updated_at"]],
        ordering: Optional[Literal["asc", "desc"]] = "asc",
        paginate: bool = False,
    ) -> list[PhysicalObjectWithGeometryDTO] | PageDTO[PhysicalObjectWithGeometryDTO]:
        """Get physical objects with geometry by territory id, optional physical object type."""

    @abc.abstractmethod
    async def get_living_buildings_with_geometry_by_territory_id(
        self,
        territory_id: int,
    ) -> list[LivingBuildingsWithGeometryDTO] | PageDTO[LivingBuildingsWithGeometryDTO]:
        """Get living buildings with geometry by territory id."""

    @abc.abstractmethod
    async def get_functional_zones_by_territory_id(
        self, territory_id: int, functional_zone_type_id: int | None
    ) -> list[FunctionalZoneDataDTO]:
        """Get functional zones with geometry by territory id."""

    @abc.abstractmethod
    async def get_territories_by_parent_id(
        self, parent_id: int | None, get_all_levels: bool, territory_type_id: int | None, paginate: bool = False
    ) -> list[TerritoryDTO] | PageDTO[TerritoryDTO]:
        """Get a territory or list of territories by parent, territory type could be specified in parameters."""

    @abc.abstractmethod
    async def get_territories_without_geometry_by_parent_id(
        self,
        parent_id: int | None,
        get_all_levels: bool,
        order_by: Optional[Literal["created_at", "updated_at"]],
        created_at: datetime | None,
        name: str | None,
        ordering: Optional[Literal["asc", "desc"]] = "asc",
        paginate: bool = False,
    ) -> list[TerritoryWithoutGeometryDTO] | PageDTO[TerritoryWithoutGeometryDTO]:
        """Get a territory or list of territories without geometry by parent,
        ordering and filters can be specified in parameters."""

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
