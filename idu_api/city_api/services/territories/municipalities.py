from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.city_api.dto.administrative_units import AdministrativeUnitsDTO
from idu_api.city_api.dto.munipalities import MunicipalitiesDTO
from idu_api.urban_api.dto import TerritoryDTO
from idu_api.urban_api.logic.impl.helpers.territory_objects import get_territories_by_parent_id_from_db, \
    get_territory_by_id
from idu_api.city_api.services.territories.territories import get_territories_by_parent_id_and_level


class MunicipalitiesService:
    def __init__(self, conn: AsyncConnection):
        self.conn = conn

    async def get_municipalities_by_city_id(
            self,
            city_id: int
    ) -> list[MunicipalitiesDTO]:
        """

        """
        city: TerritoryDTO = await get_territory_by_id(self.conn, city_id)

        result: list[TerritoryDTO | MunicipalitiesDTO] = await get_territories_by_parent_id_and_level(
            self.conn,
            city_id,
            city.level + 2
        )  # returns list[TerritoryDTO]

        del city

        for i in range(len(result)):
            municipality = MunicipalitiesDTO()
            await municipality.map_from_territory_dto(
                result[i].__dict__,
                {
                    "territory_id": "id",
                    "territory_type_name": "type",
                    "centre_point": "center"
                }
            )
            result[i] = municipality
        result: list[MunicipalitiesDTO] = result
        return result
