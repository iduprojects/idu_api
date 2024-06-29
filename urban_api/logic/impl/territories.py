"""Territories handlers logic is defined here."""

from datetime import date, datetime
from typing import Callable, Literal, Optional

import shapely.geometry as geom
from shapely.geometry import LineString, MultiPolygon, Point, Polygon
from sqlalchemy.ext.asyncio import AsyncConnection

from urban_api.dto import (
    FunctionalZoneDataDTO,
    IndicatorDTO,
    IndicatorValueDTO,
    LivingBuildingsWithGeometryDTO,
    PhysicalObjectDataDTO,
    PhysicalObjectWithGeometryDTO,
    ServiceDTO,
    ServiceWithGeometryDTO,
    TerritoryDTO,
    TerritoryTypeDTO,
    TerritoryWithoutGeometryDTO,
)
from urban_api.logic.impl.helpers.territories_buildings import (
    get_living_buildings_with_geometry_by_territory_id_from_db,
)
from urban_api.logic.impl.helpers.territories_functional_zones import get_functional_zones_by_territory_id_from_db
from urban_api.logic.impl.helpers.territories_indicators import (
    get_indicator_values_by_territory_id_from_db,
    get_indicators_by_territory_id_from_db,
)
from urban_api.logic.impl.helpers.territories_physical_objects import (
    get_physical_objects_by_territory_id_from_db,
    get_physical_objects_with_geometry_by_territory_id_from_db,
)
from urban_api.logic.impl.helpers.territory_objects import (
    add_territory_to_db,
    get_common_territory_for_geometry,
    get_intersecting_territories_for_geometry,
    get_territories_by_ids,
    get_territories_by_parent_id_from_db,
    get_territories_without_geometry_by_parent_id_from_db,
    get_territory_by_id,
    patch_territory_to_db,
    put_territory_to_db,
)
from urban_api.logic.impl.helpers.territory_services import (
    get_services_by_territory_id_from_db,
    get_services_capacity_by_territory_id_from_db,
    get_services_with_geometry_by_territory_id_from_db,
)
from urban_api.logic.impl.helpers.territory_types import add_territory_type_to_db, get_territory_types_from_db
from urban_api.logic.territories import TerritoriesService
from urban_api.schemas import TerritoryDataPatch, TerritoryDataPost, TerritoryDataPut, TerritoryTypesPost

func: Callable
Geom = Point | Polygon | MultiPolygon | LineString


class TerritoriesServiceImpl(TerritoriesService):
    """Service to manipulate territories entities.

    Based on async SQLAlchemy connection.
    """

    def __init__(self, conn: AsyncConnection):
        self._conn = conn

    async def get_territory_types(self) -> list[TerritoryTypeDTO]:
        return await get_territory_types_from_db(self._conn)

    async def add_territory_type(self, territory_type: TerritoryTypesPost) -> TerritoryTypeDTO:
        return await add_territory_type_to_db(self._conn, territory_type)

    async def get_territories_by_ids(self, territory_ids: list[int]) -> list[TerritoryDTO]:
        return await get_territories_by_ids(self._conn, territory_ids)

    async def get_territory_by_id(self, territory_id: int) -> TerritoryDTO:
        return await get_territory_by_id(self._conn, territory_id)

    async def add_territory(self, territory: TerritoryDataPost) -> TerritoryDTO:
        return await add_territory_to_db(self._conn, territory)

    async def put_territory(self, territory_id: int, territory: TerritoryDataPut) -> TerritoryDTO:
        return await put_territory_to_db(self._conn, territory_id, territory)

    async def patch_territory(self, territory_id: int, territory: TerritoryDataPatch) -> TerritoryDTO:
        return await patch_territory_to_db(self._conn, territory_id, territory)

    async def get_services_by_territory_id(
        self, territory_id: int, service_type_id: int | None, name: str | None
    ) -> list[ServiceDTO]:
        return await get_services_by_territory_id_from_db(self._conn, territory_id, service_type_id, name)

    async def get_services_with_geometry_by_territory_id(
        self, territory_id: int, service_type_id: int | None, name: str | None
    ) -> list[ServiceWithGeometryDTO]:
        return await get_services_with_geometry_by_territory_id_from_db(self._conn, territory_id, service_type_id, name)

    async def get_services_capacity_by_territory_id(self, territory_id: int, service_type_id: int | None) -> int:
        return await get_services_capacity_by_territory_id_from_db(self._conn, territory_id, service_type_id)

    async def get_indicators_by_territory_id(self, territory_id: int) -> list[IndicatorDTO]:
        return await get_indicators_by_territory_id_from_db(self._conn, territory_id)

    async def get_indicator_values_by_territory_id(
        self, territory_id: int, date_type: str | None, date_value: datetime | None
    ) -> list[IndicatorValueDTO]:
        return await get_indicator_values_by_territory_id_from_db(self._conn, territory_id, date_type, date_value)

    async def get_physical_objects_by_territory_id(
        self, territory_id: int, physical_object_type: int | None, name: str | None
    ) -> list[PhysicalObjectDataDTO]:
        return await get_physical_objects_by_territory_id_from_db(self._conn, territory_id, physical_object_type, name)

    async def get_physical_objects_with_geometry_by_territory_id(
        self, territory_id: int, physical_object_type: int | None, name: str | None
    ) -> list[PhysicalObjectWithGeometryDTO]:
        return await get_physical_objects_with_geometry_by_territory_id_from_db(
            self._conn, territory_id, physical_object_type, name
        )

    async def get_living_buildings_with_geometry_by_territory_id(
        self,
        territory_id: int,
    ) -> list[LivingBuildingsWithGeometryDTO]:
        return await get_living_buildings_with_geometry_by_territory_id_from_db(self._conn, territory_id)

    async def get_functional_zones_by_territory_id(
        self, territory_id: int, functional_zone_type_id: int | None
    ) -> list[FunctionalZoneDataDTO]:
        return await get_functional_zones_by_territory_id_from_db(self._conn, territory_id, functional_zone_type_id)

    async def get_territories_by_parent_id(
        self, parent_id: int | None, get_all_levels: bool | None, territory_type_id: int | None
    ) -> list[TerritoryDTO]:
        return await get_territories_by_parent_id_from_db(self._conn, parent_id, get_all_levels, territory_type_id)

    async def get_territories_without_geometry_by_parent_id(
        self,
        parent_id: int | None,
        get_all_levels: bool,
        order_by: Optional[Literal["created_at", "updated_at"]],
        created_at: date | None,
        name: str | None,
        ordering: Optional[Literal["asc", "desc"]] = "asc",
    ) -> list[TerritoryWithoutGeometryDTO]:
        return await get_territories_without_geometry_by_parent_id_from_db(
            self._conn, parent_id, get_all_levels, order_by, created_at, name, ordering
        )

    async def get_common_territory_for_geometry(
        self,
        geometry: geom.Polygon | geom.MultiPolygon | geom.Point,
    ) -> TerritoryDTO | None:
        return await get_common_territory_for_geometry(self._conn, geometry)

    async def get_intersecting_territories_for_geometry(
        self, parent_territory: int, geometry: geom.Polygon | geom.MultiPolygon | geom.Point
    ) -> list[TerritoryDTO]:
        return await get_intersecting_territories_for_geometry(self._conn, parent_territory, geometry)
