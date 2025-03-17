"""Indicators handlers logic of getting entities from the database is defined here."""

from datetime import datetime

from idu_api.common.db.connection.manager import PostgresConnectionManager
from idu_api.urban_api.dto import (
    IndicatorDTO,
    IndicatorsGroupDTO,
    IndicatorValueDTO,
    MeasurementUnitDTO,
)
from idu_api.urban_api.logic.impl.helpers.indicators import (
    add_indicator_to_db,
    add_indicator_value_to_db,
    add_indicators_group_to_db,
    add_measurement_unit_to_db,
    delete_indicator_from_db,
    delete_indicator_value_from_db,
    get_indicator_by_id_from_db,
    get_indicator_value_by_id_from_db,
    get_indicator_values_by_id_from_db,
    get_indicators_by_group_id_from_db,
    get_indicators_by_parent_from_db,
    get_indicators_groups_from_db,
    get_measurement_units_from_db,
    patch_indicator_to_db,
    put_indicator_to_db,
    put_indicator_value_to_db,
    update_indicators_group_from_db,
)
from idu_api.urban_api.logic.indicators import IndicatorsService
from idu_api.urban_api.schemas import (
    IndicatorsGroupPost,
    IndicatorsPatch,
    IndicatorPost,
    IndicatorPut,
    IndicatorValuePost,
    IndicatorValuePut,
    MeasurementUnitPost,
)


class IndicatorsServiceImpl(IndicatorsService):
    """Service to manipulate indicators objects.

    Based on async `PostgresConnectionManager`.
    """

    def __init__(self, connection_manager: PostgresConnectionManager):
        self._connection_manager = connection_manager

    async def get_measurement_units(self) -> list[MeasurementUnitDTO]:
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_measurement_units_from_db(conn)

    async def add_measurement_unit(self, measurement_unit: MeasurementUnitPost) -> MeasurementUnitDTO:
        async with self._connection_manager.get_connection() as conn:
            return await add_measurement_unit_to_db(conn, measurement_unit)

    async def get_indicators_groups(self) -> list[IndicatorsGroupDTO]:
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_indicators_groups_from_db(conn)

    async def add_indicators_group(self, indicators_group: IndicatorsGroupPost) -> IndicatorsGroupDTO:
        async with self._connection_manager.get_connection() as conn:
            return await add_indicators_group_to_db(conn, indicators_group)

    async def get_indicators_by_group_id(self, indicators_group_id: int) -> list[IndicatorDTO]:
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_indicators_by_group_id_from_db(conn, indicators_group_id)

    async def update_indicators_group(self, indicators_group: IndicatorsGroupPost) -> IndicatorsGroupDTO:
        async with self._connection_manager.get_connection() as conn:
            return await update_indicators_group_from_db(conn, indicators_group)

    async def get_indicators_by_parent(
        self,
        parent_id: int | None,
        parent_name: str | None,
        name: str | None,
        territory_id: int | None,
        get_all_subtree: bool,
    ) -> list[IndicatorDTO]:
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_indicators_by_parent_from_db(
                conn, parent_id, parent_name, name, territory_id, get_all_subtree
            )

    async def get_indicator_by_id(self, indicator_id: int) -> IndicatorDTO:
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_indicator_by_id_from_db(conn, indicator_id)

    async def add_indicator(self, indicator: IndicatorPost) -> IndicatorDTO:
        async with self._connection_manager.get_connection() as conn:
            return await add_indicator_to_db(conn, indicator)

    async def put_indicator(self, indicator: IndicatorPut) -> IndicatorDTO:
        async with self._connection_manager.get_connection() as conn:
            return await put_indicator_to_db(conn, indicator)

    async def patch_indicator(self, indicator_id: int, indicator: IndicatorsPatch) -> IndicatorDTO:
        async with self._connection_manager.get_connection() as conn:
            return await patch_indicator_to_db(conn, indicator_id, indicator)

    async def delete_indicator(self, indicator_id: int) -> dict:
        async with self._connection_manager.get_connection() as conn:
            return await delete_indicator_from_db(conn, indicator_id)

    async def get_indicator_value_by_id(
        self,
        indicator_id: int,
        territory_id: int,
        date_type: str,
        date_value: datetime,
        value_type: str,
        information_source: str,
    ) -> IndicatorValueDTO:
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_indicator_value_by_id_from_db(
                conn, indicator_id, territory_id, date_type, date_value, value_type, information_source
            )

    async def add_indicator_value(self, indicator_value: IndicatorValuePost) -> IndicatorValueDTO:
        async with self._connection_manager.get_connection() as conn:
            return await add_indicator_value_to_db(conn, indicator_value)

    async def put_indicator_value(self, indicator_value: IndicatorValuePut) -> IndicatorValueDTO:
        async with self._connection_manager.get_connection() as conn:
            return await put_indicator_value_to_db(conn, indicator_value)

    async def delete_indicator_value(
        self,
        indicator_id: int,
        territory_id: int,
        date_type: str,
        date_value: datetime,
        value_type: str,
        information_source: str,
    ) -> dict:
        async with self._connection_manager.get_connection() as conn:
            return await delete_indicator_value_from_db(
                conn, indicator_id, territory_id, date_type, date_value, value_type, information_source
            )

    async def get_indicator_values_by_id(
        self,
        indicator_id: int,
        territory_id: int | None,
        date_type: str | None,
        date_value: datetime | None,
        value_type: str | None,
        information_source: str | None,
    ) -> list[IndicatorValueDTO]:
        async with self._connection_manager.get_ro_connection() as conn:
            return await get_indicator_values_by_id_from_db(
                conn, indicator_id, territory_id, date_type, date_value, value_type, information_source
            )
