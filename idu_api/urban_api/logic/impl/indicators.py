"""Indicators handlers logic of getting entities from the database is defined here."""

from datetime import datetime

from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.urban_api.dto import (
    IndicatorDTO,
    IndicatorValueDTO,
    MeasurementUnitDTO,
)
from idu_api.urban_api.logic.impl.helpers.indicators import (
    add_indicator_to_db,
    add_indicator_value_to_db,
    add_measurement_unit_to_db,
    get_indicator_by_id_from_db,
    get_indicator_value_by_id_from_db,
    get_indicator_values_by_id_from_db,
    get_indicators_by_parent_id_from_db,
    get_measurement_units_from_db,
)
from idu_api.urban_api.logic.indicators import IndicatorsService
from idu_api.urban_api.schemas import IndicatorsPost, IndicatorValuePost, MeasurementUnitPost


class IndicatorsServiceImpl(IndicatorsService):
    """Service to manipulate indicators objects.

    Based on async SQLAlchemy connection.
    """

    def __init__(self, conn: AsyncConnection):
        self._conn = conn

    async def get_measurement_units(self) -> list[MeasurementUnitDTO]:
        return await get_measurement_units_from_db(self._conn)

    async def add_measurement_unit(self, measurement_unit: MeasurementUnitPost) -> MeasurementUnitDTO:
        return await add_measurement_unit_to_db(self._conn, measurement_unit)

    async def get_indicators_by_parent_id(
        self,
        parent_id: int | None,
        name: str | None,
        territory_id: int | None,
        get_all_subtree: bool,
    ) -> list[IndicatorDTO]:
        return await get_indicators_by_parent_id_from_db(self._conn, parent_id, name, territory_id, get_all_subtree)

    async def get_indicator_by_id(self, indicator_id: int) -> IndicatorDTO:
        return await get_indicator_by_id_from_db(self._conn, indicator_id)

    async def add_indicator(self, indicator: IndicatorsPost) -> IndicatorDTO:
        return await add_indicator_to_db(self._conn, indicator)

    async def get_indicator_value_by_id(
        self,
        indicator_id: int,
        territory_id: int,
        date_type: str,
        date_value: datetime,
        value_type: str,
        information_source: str,
    ) -> IndicatorValueDTO:
        return await get_indicator_value_by_id_from_db(
            self._conn, indicator_id, territory_id, date_type, date_value, value_type, information_source
        )

    async def add_indicator_value(self, indicator_value: IndicatorValuePost) -> IndicatorValueDTO:
        return await add_indicator_value_to_db(self._conn, indicator_value)

    async def get_indicator_values_by_id(
        self,
        indicator_id: int,
        territory_id: int | None,
        date_type: str | None,
        date_value: datetime | None,
        value_type: str | None,
        information_source: str | None,
    ) -> list[IndicatorValueDTO]:
        return await get_indicator_values_by_id_from_db(
            self._conn, indicator_id, territory_id, date_type, date_value, value_type, information_source
        )
