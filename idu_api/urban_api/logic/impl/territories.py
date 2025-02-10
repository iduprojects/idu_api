"""Territories handlers logic of getting entities from the database is defined here."""

from collections.abc import Callable
from datetime import date
from typing import Literal

from shapely.geometry import LineString, MultiLineString, MultiPolygon, Point, Polygon

from idu_api.common.db.connection.manager import PostgresConnectionManager
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
    TargetCityTypeDTO,
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
    delete_all_functional_zones_for_territory_from_db,
    get_functional_zones_by_territory_id_from_db,
    get_functional_zones_sources_by_territory_id_from_db,
)
from idu_api.urban_api.logic.impl.helpers.territories_hexagons import (
    add_hexagons_by_territory_id_to_db,
    delete_hexagons_by_territory_id_from_db,
    get_hexagons_by_territory_id_from_db,
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
from idu_api.urban_api.logic.impl.helpers.territories_objects import (
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
from idu_api.urban_api.logic.impl.helpers.territories_physical_objects import (
    get_physical_object_types_by_territory_id_from_db,
    get_physical_objects_by_territory_id_from_db,
    get_physical_objects_with_geometry_by_territory_id_from_db,
)
from idu_api.urban_api.logic.impl.helpers.territories_services import (
    get_service_types_by_territory_id_from_db,
    get_services_by_territory_id_from_db,
    get_services_capacity_by_territory_id_from_db,
    get_services_with_geometry_by_territory_id_from_db,
)
from idu_api.urban_api.logic.impl.helpers.territories_types import (
    add_target_city_type_to_db,
    add_territory_type_to_db,
    get_target_city_types_from_db,
    get_territory_types_from_db,
)
from idu_api.urban_api.logic.territories import TerritoriesService
from idu_api.urban_api.schemas import (
    HexagonPost,
    NormativeDelete,
    NormativePatch,
    NormativePost,
    TargetCityTypePost,
    TerritoryPatch,
    TerritoryPost,
    TerritoryPut,
    TerritoryTypePost,
)

func: Callable
Geom = Point | Polygon | MultiPolygon | LineString | MultiLineString


# pylint: disable=too-many-arguments
class TerritoriesServiceImpl(TerritoriesService):  # pylint: disable=too-many-public-methods
    """Service to manipulate territories entities.

    Based on async `PostgresConnectionManager`.
    """

    def __init__(self, connection_manager: PostgresConnectionManager):
        self._connection_manager = connection_manager

    async def get_territory_types(self) -> list[TerritoryTypeDTO]:
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_territory_types_from_db(conn)

    async def add_territory_type(self, territory_type: TerritoryTypePost) -> TerritoryTypeDTO:
        async with self._connection_manager.get_connection() as conn:
            return await add_territory_type_to_db(conn, territory_type)

    async def get_target_city_types(self) -> list[TargetCityTypeDTO]:
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_target_city_types_from_db(conn)

    async def add_target_city_type(self, target_city_type: TargetCityTypePost) -> TargetCityTypeDTO:
        async with self._connection_manager.get_connection() as conn:
            return await add_target_city_type_to_db(conn, target_city_type)

    async def get_territories_by_ids(self, territory_ids: list[int]) -> list[TerritoryDTO]:
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_territories_by_ids(conn, territory_ids)

    async def get_territory_by_id(self, territory_id: int) -> TerritoryDTO:
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_territory_by_id(conn, territory_id)

    async def add_territory(self, territory: TerritoryPost) -> TerritoryDTO:
        async with self._connection_manager.get_connection() as conn:
            return await add_territory_to_db(conn, territory)

    async def put_territory(self, territory_id: int, territory: TerritoryPut) -> TerritoryDTO:
        async with self._connection_manager.get_connection() as conn:
            return await put_territory_to_db(conn, territory_id, territory)

    async def patch_territory(self, territory_id: int, territory: TerritoryPatch) -> TerritoryDTO:
        async with self._connection_manager.get_connection() as conn:
            return await patch_territory_to_db(conn, territory_id, territory)

    async def get_service_types_by_territory_id(
        self, territory_id: int, include_child_territories: bool, cities_only: bool
    ) -> list[ServiceTypeDTO]:
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_service_types_by_territory_id_from_db(
                conn, territory_id, include_child_territories, cities_only
            )

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
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_services_by_territory_id_from_db(
                conn,
                territory_id,
                service_type_id,
                urban_function_id,
                name,
                include_child_territories,
                cities_only,
                order_by,
                ordering,
                paginate,
            )

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
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_services_with_geometry_by_territory_id_from_db(
                conn,
                territory_id,
                service_type_id,
                urban_function_id,
                name,
                include_child_territories,
                cities_only,
                order_by,
                ordering,
                paginate,
            )

    async def get_services_capacity_by_territory_id(
        self, territory_id: int, level: int, service_type_id: int | None
    ) -> list[ServicesCountCapacityDTO]:
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_services_capacity_by_territory_id_from_db(conn, territory_id, level, service_type_id)

    async def get_indicators_by_territory_id(self, territory_id: int) -> list[IndicatorDTO]:
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_indicators_by_territory_id_from_db(conn, territory_id)

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
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_indicator_values_by_territory_id_from_db(
                conn,
                territory_id,
                indicator_ids,
                indicators_group_id,
                start_date,
                end_date,
                value_type,
                information_source,
                last_only,
                include_child_territories,
                cities_only,
            )

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
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_indicator_values_by_parent_id_from_db(
                conn,
                parent_id,
                indicator_ids,
                indicators_group_id,
                start_date,
                end_date,
                value_type,
                information_source,
                last_only,
            )

    async def get_normatives_by_territory_id(
        self,
        territory_id: int,
        year: int,
        include_child_territories,
        cities_only,
    ) -> list[NormativeDTO]:
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_normatives_by_territory_id_from_db(
                conn, territory_id, year, include_child_territories, cities_only
            )

    async def add_normatives_to_territory(
        self, territory_id: int, normatives: list[NormativePost]
    ) -> list[NormativeDTO]:
        async with self._connection_manager.get_connection() as conn:
            return await add_normatives_to_territory_to_db(conn, territory_id, normatives)

    async def put_normatives_by_territory_id(
        self, territory_id: int, normatives: list[NormativePost]
    ) -> list[NormativeDTO]:
        async with self._connection_manager.get_connection() as conn:
            return await put_normatives_by_territory_id_in_db(conn, territory_id, normatives)

    async def patch_normatives_by_territory_id(
        self, territory_id: int, normatives: list[NormativePatch]
    ) -> list[NormativeDTO]:
        async with self._connection_manager.get_connection() as conn:
            return await patch_normatives_by_territory_id_in_db(conn, territory_id, normatives)

    async def delete_normatives_by_territory_id(self, territory_id: int, normatives: list[NormativeDelete]) -> dict:
        async with self._connection_manager.get_connection() as conn:
            return await delete_normatives_by_territory_id_in_db(conn, territory_id, normatives)

    async def get_normatives_values_by_parent_id(
        self, parent_id: int | None, year: int
    ) -> list[TerritoryWithNormativesDTO]:
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_normatives_values_by_parent_id_from_db(conn, parent_id, year)

    async def get_physical_object_types_by_territory_id(
        self, territory_id: int, include_child_territories: bool, cities_only: bool
    ) -> list[PhysicalObjectTypeDTO]:
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_physical_object_types_by_territory_id_from_db(
                conn, territory_id, include_child_territories, cities_only
            )

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
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_physical_objects_by_territory_id_from_db(
                conn,
                territory_id,
                physical_object_type_id,
                physical_object_function_id,
                name,
                include_child_territories,
                cities_only,
                order_by,
                ordering,
                paginate,
            )

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
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_physical_objects_with_geometry_by_territory_id_from_db(
                conn,
                territory_id,
                physical_object_type_id,
                physical_object_function_id,
                name,
                include_child_territories,
                cities_only,
                order_by,
                ordering,
                paginate,
            )

    async def get_living_buildings_with_geometry_by_territory_id(
        self,
        territory_id: int,
        include_child_territories: bool,
        cities_only: bool,
    ) -> PageDTO[LivingBuildingWithGeometryDTO]:
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_living_buildings_with_geometry_by_territory_id_from_db(
                conn, territory_id, include_child_territories, cities_only
            )

    async def get_functional_zones_sources_by_territory_id(
        self, territory_id: int, include_child_territories: bool, cities_only: bool
    ) -> list[FunctionalZoneSourceDTO]:
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_functional_zones_sources_by_territory_id_from_db(
                conn, territory_id, include_child_territories, cities_only
            )

    async def get_functional_zones_by_territory_id(
        self,
        territory_id: int,
        year: int,
        source: str,
        functional_zone_type_id: int | None,
        include_child_territories: bool,
        cities_only: bool,
    ) -> list[FunctionalZoneDTO]:
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_functional_zones_by_territory_id_from_db(
                conn,
                territory_id,
                year,
                source,
                functional_zone_type_id,
                include_child_territories,
                cities_only,
            )

    async def delete_all_functional_zones_for_territory(
        self, territory_id: int, include_child_territories: bool, cities_only: bool
    ) -> dict:
        async with self._connection_manager.get_connection() as conn:
            return await delete_all_functional_zones_for_territory_from_db(
                conn, territory_id, include_child_territories, cities_only
            )

    async def get_territories_by_parent_id(
        self,
        parent_id: int | None,
        get_all_levels: bool,
        territory_type_id: int | None,
        name: str | None,
        cities_only: bool,
        created_at: date | None,
        order_by: Literal["created_at", "updated_at"] | None,
        ordering: Literal["asc", "desc"] | None,
        paginate: bool,
    ) -> list[TerritoryDTO] | PageDTO[TerritoryDTO]:
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_territories_by_parent_id_from_db(
                conn,
                parent_id,
                get_all_levels,
                territory_type_id,
                name,
                cities_only,
                created_at,
                order_by,
                ordering,
                paginate,
            )

    async def get_territories_without_geometry_by_parent_id(
        self,
        parent_id: int | None,
        get_all_levels: bool,
        territory_type_id: int | None,
        name: str | None,
        cities_only: bool,
        created_at: date | None,
        order_by: Literal["created_at", "updated_at"] | None,
        ordering: Literal["asc", "desc"] | None,
        paginate: bool,
    ) -> list[TerritoryWithoutGeometryDTO] | PageDTO[TerritoryWithoutGeometryDTO]:
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_territories_without_geometry_by_parent_id_from_db(
                conn,
                parent_id,
                get_all_levels,
                territory_type_id,
                name,
                cities_only,
                created_at,
                order_by,
                ordering,
                paginate,
            )

    async def get_common_territory_for_geometry(self, geometry: Geom) -> TerritoryDTO | None:
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_common_territory_for_geometry(conn, geometry)

    async def get_intersecting_territories_for_geometry(
        self,
        parent_territory: int,
        geometry: Geom,
    ) -> list[TerritoryDTO]:
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_intersecting_territories_for_geometry(conn, parent_territory, geometry)

    async def get_hexagons_by_territory_id(self, territory_id: int) -> list[HexagonDTO]:
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_hexagons_by_territory_id_from_db(conn, territory_id)

    async def add_hexagons_by_territory_id(self, territory_id: int, hexagons: list[HexagonPost]) -> list[HexagonDTO]:
        async with self._connection_manager.get_connection() as conn:
            return await add_hexagons_by_territory_id_to_db(conn, territory_id, hexagons)

    async def delete_hexagons_by_territory_id(self, territory_id: int) -> dict:
        async with self._connection_manager.get_connection() as conn:
            return await delete_hexagons_by_territory_id_from_db(conn, territory_id)
