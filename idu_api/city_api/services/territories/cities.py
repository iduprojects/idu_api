from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.urban_api.dto import TerritoryDTO
from idu_api.urban_api.logic.impl.helpers.territory_objects import get_territory_by_id


class CitiesService:
    def __init__(self, conn: AsyncConnection):
        self.conn = conn

    async def get_city_by_id(self, city_id: int) -> TerritoryDTO:
        result: TerritoryDTO = await get_territory_by_id(self.conn, city_id)
        return result
