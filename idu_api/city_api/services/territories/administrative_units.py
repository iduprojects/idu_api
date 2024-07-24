from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.city_api.dto.administrative_units import AdministrativeUnitsDTO
from idu_api.urban_api.dto import TerritoryDTO
from idu_api.urban_api.logic.impl.helpers.territory_objects import get_territories_by_parent_id_from_db, \
    get_territory_by_id
from idu_api.city_api.services.territories.territories import get_territories_by_parent_id_and_level


class AdministrativeUnitsService:
    def __init__(self, conn: AsyncConnection):
        self.conn = conn

    async def get_administrative_units_by_city_id(
            self,
            city_id: int
    ) -> list[AdministrativeUnitsDTO]:
        """

        """
        city: TerritoryDTO = await get_territory_by_id(self.conn, city_id)

        result: list[TerritoryDTO | AdministrativeUnitsDTO] = await get_territories_by_parent_id_and_level(
            self.conn,
            city_id,
            city.level + 1
        )  # returns list[TerritoryDTO]

        del city

        for i in range(len(result)):
            adm_unit = AdministrativeUnitsDTO()
            await adm_unit.map_from_territory_dto(
                result[i].__dict__,
                {
                    "territory_id": "id",
                    "territory_type_name": "type",
                    "centre_point": "center"
                }
            )
            result[i] = adm_unit
        result: list[AdministrativeUnitsDTO] = result
        return result
