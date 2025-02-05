"""Territories handlers logic of getting entities from the database is defined here."""

import abc
from datetime import date
from typing import Literal, Protocol

from shapely.geometry import LineString, MultiLineString, MultiPolygon, Point, Polygon

from idu_api.urban_api.dto import (
    FunctionalZoneDTO,
    FunctionalZoneSourceDTO,
    HexagonDTO,
    IndicatorDTO,
    IndicatorValueDTO,
    LivingBuildingWithGeometryDTO,
    NormativeDTO,
    PageDTO,
    PhysicalObjectDTO,
    PhysicalObjectTypeDTO,
    PhysicalObjectWithGeometryDTO,
    ServiceDTO,
    ServicesCountCapacityDTO,
    ServiceTypeDTO,
    ServiceWithGeometryDTO,
    TerritoryDTO,
    TerritoryTypeDTO,
    TerritoryWithIndicatorsDTO,
    TerritoryWithNormativesDTO,
    TerritoryWithoutGeometryDTO,
)
from idu_api.urban_api.schemas import (
    HexagonPost,
    NormativeDelete,
    NormativePatch,
    NormativePost,
    TerritoryPatch,
    TerritoryPost,
    TerritoryPut,
    TerritoryTypePost,
)

Geom = Point | Polygon | MultiPolygon | LineString | MultiLineString


# pylint: disable=too-many-arguments
class TerritoriesService(Protocol):  # pylint: disable=too-many-public-methods,
    """Service to manipulate territories objects."""

    @abc.abstractmethod
    async def get_territory_types(self) -> list[TerritoryTypeDTO]:
        """Get all territory type objects."""

    @abc.abstractmethod
    async def add_territory_type(self, territory_type: TerritoryTypePost) -> TerritoryTypeDTO:
        """Create territory type object."""

    @abc.abstractmethod
    async def get_territories_by_ids(self, territory_ids: list[int]) -> list[TerritoryDTO]:
        """Get territory objects by ids list."""

    @abc.abstractmethod
    async def get_territory_by_id(self, territory_id: int) -> TerritoryDTO:
        """Get territory object by id."""

    @abc.abstractmethod
    async def add_territory(self, territory: TerritoryPost) -> TerritoryDTO:
        """Create territory object."""

    @abc.abstractmethod
    async def put_territory(self, territory_id: int, territory: TerritoryPut) -> TerritoryDTO:
        """Update territory object (put, update all the fields)."""

    @abc.abstractmethod
    async def patch_territory(self, territory_id: int, territory: TerritoryPatch) -> TerritoryDTO:
        """Patch territory object (patch, update only non-None fields)."""

    @abc.abstractmethod
    async def get_service_types_by_territory_id(
        self, territory_id: int, include_child_territories: bool, cities_only: bool
    ) -> list[ServiceTypeDTO]:
        """Get all service types that are located in given territory."""

    @abc.abstractmethod
    async def get_services_by_territory_id(
        self,
        territory_id: int,
        service_type_id: int | None,
        urban_function_id: int | None,
        name: str | None,
        include_child_territories: bool,
        cities_only: bool,
        order_by: Literal["created_at", "updated_at"] | None,
        ordering: Literal["asc", "desc"],
        paginate: bool = False,
    ) -> list[ServiceDTO] | PageDTO[ServiceDTO]:
        """Get service objects by territory id."""

    @abc.abstractmethod
    async def get_services_with_geometry_by_territory_id(
        self,
        territory_id: int,
        service_type_id: int | None,
        urban_function_id: int | None,
        name: str | None,
        include_child_territories: bool,
        cities_only: bool,
        order_by: Literal["created_at", "updated_at"] | None,
        ordering: Literal["asc", "desc"],
        paginate: bool = False,
    ) -> list[ServiceWithGeometryDTO] | PageDTO[ServiceWithGeometryDTO]:
        """Get service objects with geometry by territory id."""

    @abc.abstractmethod
    async def get_services_capacity_by_territory_id(
        self, territory_id: int, level: int, service_type_id: int | None
    ) -> list[ServicesCountCapacityDTO]:
        """Get summary capacity and count of services for sub-territories of given territory at the given level."""

    @abc.abstractmethod
    async def get_indicators_by_territory_id(self, territory_id: int) -> list[IndicatorDTO]:
        """Get indicators for a given territory."""

    @abc.abstractmethod
    async def get_indicator_values_by_territory_id(
        self,
        territory_id: int,
        indicator_ids: str | None,
        indicators_group_id: int | None,
        start_date: date | None,
        end_date: date | None,
        value_type: Literal["real", "target", "forecast"] | None,
        information_source: str | None,
        last_only: bool,
        include_child_territories: bool,
        cities_only: bool,
    ) -> list[IndicatorValueDTO]:
        """Get indicator values by territory id, optional indicator_ids, indicators_group_id,
        value_type, source and time period.

        Could be specified by last_only flag to get only current indicator values.
        """

    @abc.abstractmethod
    async def get_indicator_values_by_parent_id(
        self,
        parent_id: int | None,
        indicator_ids: str | None,
        indicators_group_id: int | None,
        start_date: date | None,
        end_date: date | None,
        value_type: Literal["real", "target", "forecast"] | None,
        information_source: str | None,
        last_only: bool,
    ) -> list[TerritoryWithIndicatorsDTO]:
        """Get indicator values for child territories by parent id, optional indicator_ids, indicators_group_id,
        value_type, source and time period.

        Could be specified by last_only flag to get only current indicator values.
        """

    @abc.abstractmethod
    async def get_normatives_by_territory_id(
        self,
        territory_id: int,
        year: int,
        include_child_territories,
        cities_only,
    ) -> list[NormativeDTO]:
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
        self, parent_id: int | None, year: int
    ) -> list[TerritoryWithNormativesDTO]:
        """Get list of normatives with values for territory by parent id and year."""

    @abc.abstractmethod
    async def get_physical_object_types_by_territory_id(
        self, territory_id: int, include_child_territories: bool, cities_only: bool
    ) -> list[PhysicalObjectTypeDTO]:
        """Get all physical object types for given territory."""

    @abc.abstractmethod
    async def get_physical_objects_by_territory_id(
        self,
        territory_id: int,
        physical_object_type_id: int | None,
        physical_object_function_id: int | None,
        name: str | None,
        include_child_territories: bool,
        cities_only: bool,
        order_by: Literal["created_at", "updated_at"] | None,
        ordering: Literal["asc", "desc"],
        paginate: bool = False,
    ) -> list[PhysicalObjectDTO] | PageDTO[PhysicalObjectDTO]:
        """Get physical objects by territory id, optional physical object type, function and for cities only."""

    @abc.abstractmethod
    async def get_physical_objects_with_geometry_by_territory_id(
        self,
        territory_id: int,
        physical_object_type_id: int | None,
        physical_object_function_id: int | None,
        name: str | None,
        include_child_territories: bool,
        cities_only: bool,
        order_by: Literal["created_at", "updated_at"] | None,
        ordering: Literal["asc", "desc"],
        paginate: bool = False,
    ) -> list[PhysicalObjectWithGeometryDTO] | PageDTO[PhysicalObjectWithGeometryDTO]:
        """Get physical objects with geometry by territory id,
        optional physical object type and physical object function and for cities only."""

    @abc.abstractmethod
    async def get_living_buildings_with_geometry_by_territory_id(
        self,
        territory_id: int,
        include_child_territories: bool,
        cities_only: bool,
    ) -> PageDTO[LivingBuildingWithGeometryDTO]:
        """Get living buildings with geometry by territory id."""

    @abc.abstractmethod
    async def get_functional_zones_sources_by_territory_id(
        self, territory_id: int, include_child_territories: bool, cities_only: bool
    ) -> list[FunctionalZoneSourceDTO]:
        """Get list of pairs year + source for functional zones for given territory and its children (optional)."""

    @abc.abstractmethod
    async def get_functional_zones_by_territory_id(
        self,
        territory_id: int,
        year: int,
        source: str,
        functional_zone_type_id: int | None,
        include_child_territories: bool,
        cities_only: bool,
    ) -> list[FunctionalZoneDTO]:
        """Get functional zones with geometry by territory id."""

    @abc.abstractmethod
    async def delete_all_functional_zones_for_territory(
        self, territory_id: int, include_child_territories: bool, cities_only: bool
    ) -> dict:
        """Delete all functional zones for territory."""

    @abc.abstractmethod
    async def get_territories_by_parent_id(
        self,
        parent_id: int | None,
        get_all_levels: bool,
        territory_type_id: int | None,
        name: str | None,
        cities_only: bool,
        created_at: date | None,
        order_by: Literal["created_at", "updated_at"] | None,
        ordering: Literal["asc", "desc"],
        paginate: bool,
    ) -> list[TerritoryDTO] | PageDTO[TerritoryDTO]:
        """Get a territory or list of territories by parent, territory type could be specified in parameters."""

    @abc.abstractmethod
    async def get_territories_without_geometry_by_parent_id(
        self,
        parent_id: int | None,
        get_all_levels: bool,
        territory_type_id: int | None,
        name: str | None,
        cities_only: bool,
        created_at: date | None,
        order_by: Literal["created_at", "updated_at"] | None,
        ordering: Literal["asc", "desc"],
        paginate: bool,
    ) -> list[TerritoryWithoutGeometryDTO] | PageDTO[TerritoryWithoutGeometryDTO]:
        """Get a territory or list of territories without geometry by parent,
        ordering and filters can be specified in parameters."""

    @abc.abstractmethod
    async def get_common_territory_for_geometry(self, geometry: Geom) -> TerritoryDTO | None:
        """Get the deepest territory which covers given geometry. None if there is no such territory."""

    @abc.abstractmethod
    async def get_intersecting_territories_for_geometry(
        self,
        parent_territory: int,
        geometry: Geom,
    ) -> list[TerritoryDTO]:
        """Get all territories of the (level of given parent + 1) which intersect with given geometry."""

    @abc.abstractmethod
    async def get_hexagons_by_territory_id(self, territory_id: int) -> list[HexagonDTO]:
        """Get hexagons for a given territory."""

    @abc.abstractmethod
    async def add_hexagons_by_territory_id(self, territory_id: int, hexagons: list[HexagonPost]) -> list[HexagonDTO]:
        """Create hexagons for a given territory."""

    @abc.abstractmethod
    async def delete_hexagons_by_territory_id(self, territory_id: int) -> dict:
        """Delete hexagons for a given territory."""
