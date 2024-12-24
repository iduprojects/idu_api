from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.city_api.common.mapper import territory_dto_without_geometry
from idu_api.city_api.dto.territory import CATerritoryDTO, CATerritoryWithoutGeometryDTO
from idu_api.city_api.services.territories.territories import get_territories_by_parent_id_and_level
from idu_api.urban_api.dto import TerritoryDTO, TerritoryWithoutGeometryDTO
from idu_api.urban_api.logic.impl.helpers.territory_objects import get_territory_by_id


class TerritoryLevelsService:
    def __init__(self, conn: AsyncConnection):
        self.conn = conn

    async def get_level_by_id(
        self, territory_id: int, no_geometry: bool = False
    ) -> TerritoryDTO | TerritoryWithoutGeometryDTO:
        result: TerritoryDTO = await get_territory_by_id(self.conn, territory_id)

        if not no_geometry:
            return result
        return await territory_dto_without_geometry(result)

    async def get_level_by_territory_id_and_type(
        self, territory_id: int, level: int, type: int, no_geometry: bool = False  # pylint: disable=redefined-builtin
    ) -> list[CATerritoryDTO | CATerritoryWithoutGeometryDTO]:
        _ = await get_territory_by_id(self.conn, territory_id)

        result: list[CATerritoryDTO | CATerritoryWithoutGeometryDTO] = await get_territories_by_parent_id_and_level(
            self.conn, territory_id, level=level, type=type, no_geometry=no_geometry
        )  # returns list[TerritoryDTO]

        return result
