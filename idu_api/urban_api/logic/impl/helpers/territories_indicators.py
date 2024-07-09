"""Territories indicators internal logic is defined here."""

from datetime import datetime
from typing import Callable, Optional

from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.common.db.entities import (
    indicators_dict,
    measurement_units_dict,
    territories_data,
    territory_indicators_data,
)
from idu_api.urban_api.dto import IndicatorDTO, IndicatorValueDTO

func: Callable


async def get_indicators_by_territory_id_from_db(
    conn: AsyncConnection,
    territory_id: int,
) -> list[IndicatorDTO]:
    """Get indicators by territory id."""

    statement = select(territories_data).where(territories_data.c.territory_id == territory_id)
    territory = (await conn.execute(statement)).one_or_none()
    if territory is None:
        raise HTTPException(status_code=404, detail="Given territory id is not found")

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
        raise HTTPException(status_code=404, detail="Given territory id is not found")

    statement = select(territory_indicators_data).where(territory_indicators_data.c.territory_id == territory_id)

    if date_type is not None:
        statement = statement.where(territory_indicators_data.c.date_type == date_type)
    if date_value is not None:
        statement = statement.where(territory_indicators_data.c.date_value == date_value)

    result = (await conn.execute(statement)).mappings().all()

    return [IndicatorValueDTO(**indicator_value) for indicator_value in result]
