from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.city_api.dto.administrative_units import AdministrativeUnitsDTO
from idu_api.city_api.dto.munipalities import MunicipalitiesDTO, MunicipalitiesWithoutGeometryDTO
from idu_api.urban_api.dto import TerritoryDTO, TerritoryWithoutGeometryDTO
from idu_api.urban_api.logic.impl.helpers.territory_objects import get_territories_by_parent_id_from_db, \
    get_territory_by_id
from idu_api.city_api.services.territories.territories import get_territories_by_parent_id_and_level


class MunicipalitiesService:
    def __init__(self, conn: AsyncConnection):
        self.conn = conn

    async def get_municipality_by_id(self, municipality_id: int) -> TerritoryDTO:
        result: TerritoryDTO = await get_territory_by_id(self.conn, municipality_id)
        return result

    async def get_municipalities_by_city_id(
            self,
            city_id: int
    ) -> list[MunicipalitiesDTO]:
        """

        """
        city: TerritoryDTO = await get_territory_by_id(self.conn, city_id)

        result: list[TerritoryDTO] = await get_territories_by_parent_id_and_level(
            self.conn,
            city_id,
            city.level + 2
        )  # returns list[TerritoryDTO]

        del city

        return await self.map_dto(result)

    async def get_municipalities_by_administrative_unit_id(
            self,
            city_id: int,
            administrative_unit_id: int
    ) -> list[MunicipalitiesDTO | MunicipalitiesWithoutGeometryDTO]:
        city: TerritoryDTO = await get_territory_by_id(self.conn, city_id)

        adm_unit: TerritoryDTO = await get_territory_by_id(self.conn, administrative_unit_id)
        if adm_unit.parent_id != city.territory_id:
            raise HTTPException(404, "ADMINISTRATIVE_UNIT_NOT_FOUND_BY_CITY")

        result: list[TerritoryDTO] = await get_territories_by_parent_id_and_level(
            self.conn,
            administrative_unit_id,
            adm_unit.level + 1
        )

        del city, adm_unit

        return await self.map_dto(result)

    @staticmethod
    async def map_dto(
            result: list[TerritoryDTO]
    ) -> list[MunicipalitiesDTO]:
        mapped_result: list[MunicipalitiesDTO] = []

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
            mapped_result.append(municipality)

        return mapped_result
