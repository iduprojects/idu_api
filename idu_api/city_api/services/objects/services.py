from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.city_api.dto.services import CityServiceDTO
from idu_api.city_api.dto.services_count import ServiceCountDTO
from idu_api.city_api.services.objects.urban_objects import (
    get_services_by_territory_ids,
    get_services_types_by_territory_ids,
)
from idu_api.city_api.services.territories.territories import get_ca_territory_by_id, get_territory_ids_by_parent_id


class ServicesService:
    def __init__(self, conn: AsyncConnection):
        self.conn = conn

    async def get_services_by_territory_id(self, city_id: int | str, territory_id: int | str) -> list[CityServiceDTO]:
        _ = await get_ca_territory_by_id(self.conn, city_id)

        children: list[int] = await get_territory_ids_by_parent_id(self.conn, territory_id)
        result: list[CityServiceDTO] = await get_services_by_territory_ids(self.conn, children)
        return result

    async def get_services_types_by_territory_id(
        self, city_id: int | str, territory_id: int | str
    ) -> list[ServiceCountDTO]:
        _ = await get_ca_territory_by_id(self.conn, city_id)
        territory = await get_ca_territory_by_id(self.conn, territory_id)

        children: list[int] = await get_territory_ids_by_parent_id(self.conn, territory_id)
        children.append(territory.territory_id)
        result: list[ServiceCountDTO] = await get_services_types_by_territory_ids(self.conn, children)
        return result
