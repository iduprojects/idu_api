"""Territories indicators internal logic is defined here."""

from datetime import datetime
from typing import Callable, Optional

from geoalchemy2.functions import ST_AsGeoJSON
from sqlalchemy import cast, select
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.common.db.entities import (
    indicators_dict,
    measurement_units_dict,
    territories_data,
    territory_indicators_data,
)
from idu_api.urban_api.dto import IndicatorDTO, IndicatorValueDTO, TerritoryWithIndicatorDTO, TerritoryWithIndicatorsDTO
from idu_api.urban_api.exceptions.logic.common import EntityNotFoundById

func: Callable


async def get_indicators_by_territory_id_from_db(
    conn: AsyncConnection,
    territory_id: int,
) -> list[IndicatorDTO]:
    """Get indicators by territory id."""

    statement = select(territories_data).where(territories_data.c.territory_id == territory_id)
    territory = (await conn.execute(statement)).one_or_none()
    if territory is None:
        raise EntityNotFoundById(territory_id, "territory")

    statement = (
        select(indicators_dict, measurement_units_dict.c.name.label("measurement_unit_name"))
        .select_from(
            territory_indicators_data.join(
                indicators_dict, territory_indicators_data.c.indicator_id == indicators_dict.c.indicator_id
            ).outerjoin(
                measurement_units_dict,
                measurement_units_dict.c.measurement_unit_id == indicators_dict.c.measurement_unit_id,
            )
        )
        .where(territory_indicators_data.c.territory_id == territory_id)
    )

    result = (await conn.execute(statement)).mappings().all()

    return [IndicatorDTO(**indicator) for indicator in result]


async def get_indicator_values_by_territory_id_from_db(
    conn: AsyncConnection, territory_id: int, date_type: Optional[str], date_value: Optional[datetime]
) -> list[IndicatorValueDTO]:
    """Get indicator values by territory id, optional time period."""

    statement = select(territories_data).where(territories_data.c.territory_id == territory_id)
    territory = (await conn.execute(statement)).one_or_none()
    if territory is None:
        raise EntityNotFoundById(territory_id, "territory")

    statement = select(territory_indicators_data).where(territory_indicators_data.c.territory_id == territory_id)

    if date_type is not None:
        statement = statement.where(territory_indicators_data.c.date_type == date_type)
    if date_value is not None:
        statement = statement.where(territory_indicators_data.c.date_value == date_value)

    result = (await conn.execute(statement)).mappings().all()

    return [IndicatorValueDTO(**indicator_value) for indicator_value in result]


async def get_indicator_values_by_parent_id_from_db(
    conn: AsyncConnection, parent_id: Optional[int], date_type: str, date_value: datetime, indicator_id: int
) -> list[TerritoryWithIndicatorDTO]:
    """Get indicator values for child territories by parent id and time period."""

    if parent_id is not None:
        statement = select(territories_data).where(territories_data.c.territory_id == parent_id)
        territory = (await conn.execute(statement)).one_or_none()
        if territory is None:
            raise EntityNotFoundById(parent_id, "territory")

    statement = select(indicators_dict).where(indicators_dict.c.indicator_id == indicator_id)
    indicator = (await conn.execute(statement)).one_or_none()
    if indicator is None:
        raise EntityNotFoundById(indicator_id, "indicator")

    statement = (
        select(
            territories_data.c.territory_id,
            territories_data.c.name,
            cast(ST_AsGeoJSON(territories_data.c.geometry), JSONB).label("geometry"),
            cast(ST_AsGeoJSON(territories_data.c.centre_point), JSONB).label("centre_point"),
            indicators_dict.c.name_full.label("indicator_name"),
            territory_indicators_data.c.value.label("indicator_value"),
            measurement_units_dict.c.name.label("measurement_unit_name"),
        )
        .select_from(
            territory_indicators_data.join(
                territories_data,
                territories_data.c.territory_id == territory_indicators_data.c.territory_id,
            )
            .join(
                indicators_dict,
                indicators_dict.c.indicator_id == territory_indicators_data.c.indicator_id,
            )
            .outerjoin(
                measurement_units_dict,
                measurement_units_dict.c.measurement_unit_id == indicators_dict.c.measurement_unit_id,
            )
        )
        .where(
            (
                territories_data.c.parent_id == parent_id
                if parent_id is not None
                else territories_data.c.parent_id.is_(None)
            ),
            territory_indicators_data.c.date_type == date_type,
            territory_indicators_data.c.date_value == date_value,
            territory_indicators_data.c.indicator_id == indicator_id,
        )
    )

    child_territories = (await conn.execute(statement)).mappings().all()

    return [TerritoryWithIndicatorDTO(**child_territory) for child_territory in child_territories]


async def get_indicators_values_by_parent_id_from_db(
    conn: AsyncConnection,
    parent_id: Optional[int],
    date_type: str,
    date_value: datetime,
) -> list[TerritoryWithIndicatorsDTO]:
    """Get list of indicators with values for child territories by parent id and time period."""

    if parent_id is not None:
        statement = select(territories_data).where(territories_data.c.territory_id == parent_id)
        territory = (await conn.execute(statement)).one_or_none()
        if territory is None:
            raise EntityNotFoundById(parent_id, "territory")

    statement = select(
        territories_data.c.territory_id,
        territories_data.c.name,
        cast(ST_AsGeoJSON(territories_data.c.geometry), JSONB).label("geometry"),
        cast(ST_AsGeoJSON(territories_data.c.centre_point), JSONB).label("centre_point"),
    ).where(
        territories_data.c.parent_id == parent_id if parent_id is not None else territories_data.c.parent_id.is_(None),
    )
    child_territories = (await conn.execute(statement)).mappings().all()

    results = []
    for child_territory in child_territories:
        statement = (
            select(
                indicators_dict.c.name_full,
                territory_indicators_data.c.value,
                measurement_units_dict.c.name.label("measurement_unit_name"),
            )
            .select_from(
                territory_indicators_data.join(
                    indicators_dict,
                    indicators_dict.c.indicator_id == territory_indicators_data.c.indicator_id,
                ).outerjoin(
                    measurement_units_dict,
                    measurement_units_dict.c.measurement_unit_id == indicators_dict.c.measurement_unit_id,
                )
            )
            .where(
                territory_indicators_data.c.territory_id == child_territory.territory_id,
                territory_indicators_data.c.date_type == date_type,
                territory_indicators_data.c.date_value == date_value,
            )
        )

        indicators = (await conn.execute(statement)).mappings().all()
        results.append(TerritoryWithIndicatorsDTO(**child_territory, indicators=indicators))

    return results
