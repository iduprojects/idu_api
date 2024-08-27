"""Indicators handlers logic of getting entities from the database is defined here."""

import abc
from datetime import datetime
from typing import Protocol

from idu_api.urban_api.dto import (
    IndicatorDTO,
    IndicatorValueDTO,
    MeasurementUnitDTO,
)
from idu_api.urban_api.schemas import IndicatorsPost, IndicatorValuePost, MeasurementUnitPost


class IndicatorsService(Protocol):
    """Service to manipulate indicators objects."""

    @abc.abstractmethod
    async def get_measurement_units(self) -> list[MeasurementUnitDTO]:
        """Get all measurement unit objects."""

    @abc.abstractmethod
    async def add_measurement_unit(self, measurement_unit: MeasurementUnitPost) -> MeasurementUnitDTO:
        """Create measurement unit object."""

    @abc.abstractmethod
    async def get_indicators_by_parent_id(
        self,
        parent_id: int | None,
        name: str | None,
        territory_id: int | None,
        get_all_subtree: bool,
    ) -> list[IndicatorDTO]:
        """Get an indicator or list of indicators by parent."""

    @abc.abstractmethod
    async def get_indicator_by_id(self, indicator_id: int) -> IndicatorDTO:
        """Get indicator object by id."""

    @abc.abstractmethod
    async def add_indicator(self, indicator: IndicatorsPost) -> IndicatorDTO:
        """Create indicator object."""

    @abc.abstractmethod
    async def get_indicator_value_by_id(
        self,
        indicator_id: int,
        territory_id: int,
        date_type: str,
        date_value: datetime,
        value_type: str,
        information_source: str,
    ) -> IndicatorValueDTO:
        """Get indicator value object by id."""

    @abc.abstractmethod
    async def add_indicator_value(self, indicator_value: IndicatorValuePost) -> IndicatorValueDTO:
        """Create indicator value object."""

    @abc.abstractmethod
    async def get_indicator_values_by_id(
        self,
        indicator_id: int,
        territory_id: int | None,
        date_type: str | None,
        date_value: datetime | None,
        value_type: str | None,
        information_source: str | None,
    ) -> list[IndicatorValueDTO]:
        """Get indicator values objects by indicator id."""
