"""Territories handlers logic is defined here."""

from datetime import date, datetime
from typing import Callable, Literal

from shapely.geometry import LineString, MultiLineString, MultiPolygon, Point, Polygon
from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.urban_api.dto import (
    FunctionalZoneDataDTO,
    IndicatorDTO,
    IndicatorValueDTO,
    LivingBuildingsWithGeometryDTO,
    NormativeDTO,
    PageDTO,
    PhysicalObjectDataDTO,
    PhysicalObjectTypeDTO,
    PhysicalObjectWithGeometryDTO,
    ServiceDTO,
    ServicesCountCapacityDTO,
    ServiceTypesDTO,
    ServiceWithGeometryDTO,
    TerritoryDTO,
    TerritoryTypeDTO,
    TerritoryWithIndicatorsDTO,
    TerritoryWithNormativesDTO,
    TerritoryWithoutGeometryDTO,
)
from idu_api.urban_api.logic.impl.helpers.territories_buildings import (
    get_living_buildings_with_geometry_by_territory_id_from_db,
)
from idu_api.urban_api.logic.impl.helpers.territories_functional_zones import (
    get_functional_zones_by_territory_id_from_db,
)
from idu_api.urban_api.logic.impl.helpers.territories_indicators import (
    get_indicator_values_by_parent_id_from_db,
    get_indicator_values_by_territory_id_from_db,
    get_indicators_by_territory_id_from_db,
)
from idu_api.urban_api.logic.impl.helpers.territories_normatives import (
    add_normatives_to_territory_to_db,
    delete_normatives_by_territory_id_in_db,
    get_normatives_by_territory_id_from_db,
    get_normatives_values_by_parent_id_from_db,
    patch_normatives_by_territory_id_in_db,
    put_normatives_by_territory_id_in_db,
)
from idu_api.urban_api.logic.impl.helpers.territories_physical_objects import (
    get_physical_object_types_by_territory_id_from_db,
    get_physical_objects_by_territory_id_from_db,
    get_physical_objects_with_geometry_by_territory_id_from_db,
)
from idu_api.urban_api.logic.impl.helpers.territory_objects import (
    add_territory_to_db,
    get_common_territory_for_geometry,
    get_intersecting_territories_for_geometry,
    get_territories_by_ids,
    get_territories_by_parent_id_from_db,
    get_territories_without_geometry_by_parent_id_from_db,
    get_territory_by_id,
    get_territory_geojson_by_territory_id_from_db,
    patch_territory_to_db,
    put_territory_to_db,
)
from idu_api.urban_api.logic.impl.helpers.territory_services import (
    get_service_types_by_territory_id_from_db,
    get_services_by_territory_id_from_db,
    get_services_capacity_by_territory_id_from_db,
    get_services_with_geometry_by_territory_id_from_db,
)
from idu_api.urban_api.logic.impl.helpers.territory_types import add_territory_type_to_db, get_territory_types_from_db
from idu_api.urban_api.logic.territories import TerritoriesService
from idu_api.urban_api.schemas import (
    NormativeDelete,
    NormativePatch,
    NormativePost,
    TerritoryDataPatch,
    TerritoryDataPost,
    TerritoryDataPut,
    TerritoryTypesPost,
)

func: Callable
Geom = Point | Polygon | MultiPolygon | LineString | MultiLineString


class TerritoriesServiceImpl(TerritoriesService):  # pylint: disable=too-many-public-methods
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

    async def get_service_types_by_territory_id(self, territory_id: int) -> list[ServiceTypesDTO]:
        return await get_service_types_by_territory_id_from_db(self._conn, territory_id)

    async def get_services_by_territory_id(
        self,
        territory_id: int,
        service_type_id: int | None,
        name: str | None,
        order_by: Literal["created_at", "updated_at"] | None,
        ordering: Literal["asc", "desc"] | None = "asc",
        paginate: bool = False,
    ) -> list[ServiceDTO] | PageDTO[ServiceDTO]:
        return await get_services_by_territory_id_from_db(
            self._conn, territory_id, service_type_id, name, order_by, ordering, paginate
        )

    async def get_services_with_geometry_by_territory_id(
        self,
        territory_id: int,
        service_type_id: int | None,
        name: str | None,
        order_by: Literal["created_at", "updated_at"] | None,
        ordering: Literal["asc", "desc"] | None = "asc",
        paginate: bool = False,
    ) -> list[ServiceWithGeometryDTO] | PageDTO[ServiceWithGeometryDTO]:
        return await get_services_with_geometry_by_territory_id_from_db(
            self._conn, territory_id, service_type_id, name, order_by, ordering, paginate
        )

    async def get_services_capacity_by_territory_id(
        self, territory_id: int, level: int, service_type_id: int | None
    ) -> list[ServicesCountCapacityDTO]:
        return await get_services_capacity_by_territory_id_from_db(self._conn, territory_id, level, service_type_id)

    async def get_indicators_by_territory_id(self, territory_id: int) -> list[IndicatorDTO]:
        return await get_indicators_by_territory_id_from_db(self._conn, territory_id)

    async def get_indicator_values_by_territory_id(
        self,
        territory_id: int,
        indicator_ids: str | None,
        indicators_group_id: int | None,
        start_date: datetime | None,
        end_date: datetime | None,
        value_type: Literal["real", "target", "forecast"] | None,
        information_source: str | None,
        last_only: bool,
    ) -> list[IndicatorValueDTO]:
        return await get_indicator_values_by_territory_id_from_db(
            self._conn,
            territory_id,
            indicator_ids,
            indicators_group_id,
            start_date,
            end_date,
            value_type,
            information_source,
            last_only,
        )

    async def get_indicator_values_by_parent_id(
        self,
        parent_id: int | None,
        indicator_ids: str | None,
        indicators_group_id: int | None,
        start_date: datetime | None,
        end_date: datetime | None,
        value_type: Literal["real", "target", "forecast"] | None,
        information_source: str | None,
        last_only: bool,
    ) -> list[TerritoryWithIndicatorsDTO]:
        return await get_indicator_values_by_parent_id_from_db(
            self._conn,
            parent_id,
            indicator_ids,
            indicators_group_id,
            start_date,
            end_date,
            value_type,
            information_source,
            last_only,
        )

    async def get_normatives_by_territory_id(self, territory_id: int, year: int) -> list[NormativeDTO]:
        return await get_normatives_by_territory_id_from_db(self._conn, territory_id, year)

    async def add_normatives_to_territory(
        self, territory_id: int, normatives: list[NormativePost]
    ) -> list[NormativeDTO]:
        return await add_normatives_to_territory_to_db(self._conn, territory_id, normatives)

    async def put_normatives_by_territory_id(
        self, territory_id: int, normatives: list[NormativePost]
    ) -> list[NormativeDTO]:
        return await put_normatives_by_territory_id_in_db(self._conn, territory_id, normatives)

    async def patch_normatives_by_territory_id(
        self, territory_id: int, normatives: list[NormativePatch]
    ) -> list[NormativeDTO]:
        return await patch_normatives_by_territory_id_in_db(self._conn, territory_id, normatives)

    async def delete_normatives_by_territory_id(self, territory_id: int, normatives: list[NormativeDelete]) -> dict:
        return await delete_normatives_by_territory_id_in_db(self._conn, territory_id, normatives)

    async def get_normatives_values_by_parent_id(
        self, parent_id: int | None, year: int
    ) -> list[TerritoryWithNormativesDTO]:
        return await get_normatives_values_by_parent_id_from_db(self._conn, parent_id, year)

    async def get_physical_object_types_by_territory_id(self, territory_id: int) -> list[PhysicalObjectTypeDTO]:
        return await get_physical_object_types_by_territory_id_from_db(self._conn, territory_id)

    async def get_physical_objects_by_territory_id(
        self,
        territory_id: int,
        physical_object_type: int | None,
        name: str | None,
        order_by: Literal["created_at", "updated_at"] | None,
        ordering: Literal["asc", "desc"] | None = "asc",
        paginate: bool = False,
    ) -> list[PhysicalObjectDataDTO] | PageDTO[PhysicalObjectDataDTO]:
        return await get_physical_objects_by_territory_id_from_db(
            self._conn, territory_id, physical_object_type, name, order_by, ordering, paginate
        )

    async def get_physical_objects_with_geometry_by_territory_id(
        self,
        territory_id: int,
        physical_object_type: int | None,
        name: str | None,
        order_by: Literal["created_at", "updated_at"] | None,
        ordering: Literal["asc", "desc"] | None = "asc",
        paginate: bool = False,
    ) -> list[PhysicalObjectWithGeometryDTO] | PageDTO[PhysicalObjectWithGeometryDTO]:
        return await get_physical_objects_with_geometry_by_territory_id_from_db(
            self._conn, territory_id, physical_object_type, name, order_by, ordering, paginate
        )

    async def get_living_buildings_with_geometry_by_territory_id(
        self,
        territory_id: int,
    ) -> PageDTO[LivingBuildingsWithGeometryDTO]:
        return await get_living_buildings_with_geometry_by_territory_id_from_db(self._conn, territory_id)

    async def get_functional_zones_by_territory_id(
        self,
        territory_id: int,
        functional_zone_type_id: int | None,
        include_child_territories: bool,
    ) -> list[FunctionalZoneDataDTO]:
        return await get_functional_zones_by_territory_id_from_db(
            self._conn, territory_id, functional_zone_type_id, include_child_territories
        )

    async def get_territories_by_parent_id(
        self, parent_id: int | None, get_all_levels: bool | None, territory_type_id: int | None, paginate: bool = False
    ) -> list[TerritoryDTO] | PageDTO[TerritoryDTO]:
        return await get_territories_by_parent_id_from_db(
            self._conn, parent_id, get_all_levels, territory_type_id, paginate
        )

    async def get_territories_without_geometry_by_parent_id(
        self,
        parent_id: int | None,
        get_all_levels: bool,
        order_by: Literal["created_at", "updated_at"] | None,
        created_at: date | None,
        name: str | None,
        ordering: Literal["asc", "desc"] | None = "asc",
        paginate: bool = False,
    ) -> list[TerritoryWithoutGeometryDTO] | PageDTO[TerritoryWithoutGeometryDTO]:
        return await get_territories_without_geometry_by_parent_id_from_db(
            self._conn, parent_id, get_all_levels, order_by, created_at, name, ordering, paginate
        )

    async def get_common_territory_for_geometry(self, geometry: Geom) -> TerritoryDTO | None:
        return await get_common_territory_for_geometry(self._conn, geometry)

    async def get_intersecting_territories_for_geometry(
        self,
        parent_territory: int,
        geometry: Geom,
    ) -> list[TerritoryDTO]:
        return await get_intersecting_territories_for_geometry(self._conn, parent_territory, geometry)

    async def get_territory_geojson_by_territory_id(self, territory_id: int) -> TerritoryDTO:
        return await get_territory_geojson_by_territory_id_from_db(self._conn, territory_id)
