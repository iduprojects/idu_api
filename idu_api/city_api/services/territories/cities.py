from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.city_api.common.mapper import territory_dto_without_geometry
from idu_api.city_api.dto.territory import CATerritoryDTO, CATerritoryWithoutGeometryDTO
from idu_api.city_api.dto.territory_hierarchy import TerritoryHierarchyDTO
from idu_api.city_api.services.territories.territories import get_territory_hierarchy_by_parent_id, \
    get_territories_by_parent_id_and_level
from idu_api.urban_api.dto import TerritoryDTO, TerritoryWithoutGeometryDTO
from idu_api.urban_api.logic.impl.helpers.territory_objects import get_territory_by_id


class CitiesService:
    def __init__(self, conn: AsyncConnection):
        self.conn = conn

    async def get_city_by_id(
            self, city_id: int, no_geometry: bool = False
    ) -> TerritoryDTO | TerritoryWithoutGeometryDTO:
        result: TerritoryDTO = await get_territory_by_id(self.conn, city_id)
        if not no_geometry:
            return result
        else:
            return await territory_dto_without_geometry(result)

    async def get_hierarchy_by_city_id(self, city_id: int) -> list[TerritoryHierarchyDTO]:
        return await get_territory_hierarchy_by_parent_id(self.conn, city_id)

    async def get_territories_by_city_id_level_and_type(
            self, city_id: int, level: int, type: int, no_geometry: bool = False
    ) -> list[CATerritoryDTO | CATerritoryWithoutGeometryDTO]:
        _ = await get_territory_by_id(self.conn, city_id)

        return await get_territories_by_parent_id_and_level(self.conn, city_id, level=level, type=type, no_geometry=no_geometry)
