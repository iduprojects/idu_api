"""Indicators handlers logic of getting entities from the database is defined here."""

from datetime import datetime
from typing import List, Optional

from fastapi import HTTPException
from sqlalchemy import insert, select
from sqlalchemy.ext.asyncio import AsyncConnection

from urban_api.db.entities import (
    indicators_dict,
    measurement_units_dict,
    territory_indicators_data,
)
from urban_api.dto import (
    IndicatorDTO,
    IndicatorValueDTO,
    MeasurementUnitDTO,
)
from urban_api.schemas import IndicatorsPost, IndicatorValue, MeasurementUnitPost
from urban_api.schemas.enums import DateType


async def get_measurement_units_from_db(conn: AsyncConnection) -> list[MeasurementUnitDTO]:
    """
    Get all measurement unit objects
    """

    statement = select(measurement_units_dict).order_by(measurement_units_dict.c.measurement_unit_id)

    return [MeasurementUnitDTO(*unit) for unit in await conn.execute(statement)]


async def add_measurement_unit_to_db(
    conn: AsyncConnection,
    measurement_unit: MeasurementUnitPost,
) -> MeasurementUnitDTO:
    """
    Create measurement unit object
    """

    statement = select(measurement_units_dict).where(measurement_units_dict.c.name == measurement_unit.name)
    result = (await conn.execute(statement)).scalar()
    if result is not None:
        raise HTTPException(status_code=400, detail="Invalid input (measurement unit already exists)")

    statement = (
        insert(measurement_units_dict)
        .values(
            name=measurement_unit.name,
        )
        .returning(measurement_units_dict)
    )
    result = (await conn.execute(statement)).mappings().one()

    await conn.commit()

    return MeasurementUnitDTO(**result)


async def get_indicators_by_parent_id_from_db(
    conn: AsyncConnection,
    parent_id: Optional[int],
    name: Optional[str],
    territory_id: Optional[int],
    get_all_subtree: bool,
) -> List[IndicatorDTO]:
    """
    Get an indicator or list of indicators by parent
    """

    if parent_id is not None:
        statement = select(indicators_dict).where(indicators_dict.c.indicator_id == parent_id)
        is_found_parent_id = (await conn.execute(statement)).one_or_none()
        if is_found_parent_id is None:
            raise HTTPException(status_code=404, detail="Given parent id is not found")

    statement = select(
        indicators_dict.c.indicator_id,
        indicators_dict.c.name_full,
        indicators_dict.c.name_short,
        indicators_dict.c.measurement_unit_id,
        indicators_dict.c.level,
        indicators_dict.c.list_label,
        indicators_dict.c.parent_id,
        measurement_units_dict.c.name.label("measurement_unit_name"),
    ).select_from(
        indicators_dict.join(
            measurement_units_dict,
            indicators_dict.c.measurement_unit_id == measurement_units_dict.c.measurement_unit_id,
            isouter=True,
        )
    )

    if get_all_subtree:
        cte_statement = statement.where(
            (
                indicators_dict.c.parent_id == parent_id
                if parent_id is not None
                else indicators_dict.c.parent_id.is_(None)
            )
        )
        cte_statement = cte_statement.cte(name="territories_recursive", recursive=True)

        recursive_part = statement.join(cte_statement, indicators_dict.c.parent_id == cte_statement.c.indicator_id)

        statement = select(cte_statement.union_all(recursive_part))
    else:
        statement = statement.where(
            indicators_dict.c.parent_id == parent_id if parent_id is not None else indicators_dict.c.parent_id.is_(None)
        )

    requested_indicators = statement.cte("requested_indicators")

    statement = select(requested_indicators)

    if territory_id is not None:
        territory_filter = (
            select(territory_indicators_data.c.indicator_id.distinct().label("indicator_id"))
            .where(territory_indicators_data.c.territory_id == territory_id)
            .cte("territory_filter")
        )
        statement = statement.where(requested_indicators.c.indicator_id.in_(select(territory_filter.c.indicator_id)))

    if name is not None:
        statement = statement.where(
            requested_indicators.c.name_full.ilike(f"%{name}%") | requested_indicators.c.name_short.ilike(f"%{name}%")
        )

    result = (await conn.execute(statement)).mappings().all()

    return [IndicatorDTO(**indicator) for indicator in result]


async def get_indicator_by_id_from_db(conn: AsyncConnection, indicator_id: int) -> IndicatorDTO:
    """Get indicator object by id."""

    statement = (
        select(
            indicators_dict,
            measurement_units_dict.c.name.label("measurement_unit_name"),
        )
        .select_from(
            indicators_dict.outerjoin(
                measurement_units_dict,
                measurement_units_dict.c.measurement_unit_id == indicators_dict.c.measurement_unit_id,
            )
        )
        .where(indicators_dict.c.indicator_id == indicator_id)
    )

    result = (await conn.execute(statement)).mappings().one_or_none()
    if result is None:
        raise HTTPException(status_code=404, detail="Given id is not found")

    return IndicatorDTO(**result)


async def add_indicator_to_db(conn: AsyncConnection, indicator: IndicatorsPost) -> IndicatorDTO:
    """Create indicator object."""

    if indicator.parent_id is not None:
        statement = select(indicators_dict).where(indicators_dict.c.indicator_id == indicator.parent_id)
        check_parent_id = (await conn.execute(statement)).one_or_none()
        if check_parent_id is None:
            raise HTTPException(status_code=404, detail="Given parent_id is not found")

    statement = select(indicators_dict).where(indicators_dict.c.name_full == indicator.name_full)
    check_indicator_name = (await conn.execute(statement)).one_or_none()
    if check_indicator_name is not None:
        raise HTTPException(status_code=400, detail="Invalid input (indicator already exists)")

    statement = (
        insert(indicators_dict)
        .values(
            name_full=indicator.name_full,
            name_short=indicator.name_short,
            measurement_unit_id=indicator.measurement_unit_id,
            level=indicator.level,
            list_label=indicator.list_label,
            parent_id=indicator.parent_id,
        )
        .returning(indicators_dict)
    )
    result = (await conn.execute(statement)).mappings().one()

    await conn.commit()

    return await get_indicator_by_id_from_db(conn, result.indicator_id)


async def get_indicator_value_by_id_from_db(
    conn: AsyncConnection, indicator_id: int, territory_id: int, date_type: DateType, date_value: datetime
) -> IndicatorValueDTO:
    """Get indicator value object by id."""

    statement = select(territory_indicators_data).where(
        territory_indicators_data.c.indicator_id == indicator_id,
        territory_indicators_data.c.territory_id == territory_id,
        territory_indicators_data.c.date_type == date_type,
        territory_indicators_data.c.date_value == date_value,
    )
    result = (await conn.execute(statement)).mappings().one_or_none()
    if result is None:
        raise HTTPException(status_code=404, detail="Given id is not found")

    return IndicatorValueDTO(**result)


async def add_indicator_value_to_db(
    conn: AsyncConnection,
    indicator_value: IndicatorValue,
) -> IndicatorValueDTO:
    """Create indicator value object."""

    statement = select(territory_indicators_data).where(
        territory_indicators_data.c.indicator_id == indicator_value.indicator_id,
        territory_indicators_data.c.territory_id == indicator_value.territory_id,
        territory_indicators_data.c.date_type == indicator_value.date_type,
        territory_indicators_data.c.date_value == indicator_value.date_value,
    )
    result = (await conn.execute(statement)).one_or_none()
    if result is not None:
        raise HTTPException(status_code=400, detail="Invalid input (indicator already exists)")

    statement = (
        insert(territory_indicators_data)
        .values(
            indicator_id=indicator_value.indicator_id,
            territory_id=indicator_value.territory_id,
            date_type=indicator_value.date_type,
            date_value=indicator_value.date_value,
            value=indicator_value.value,
            value_type=indicator_value.value_type,
            information_source=indicator_value.information_source,
        )
        .returning(territory_indicators_data)
    )
    result = (await conn.execute(statement)).mappings().one()

    await conn.commit()

    return IndicatorValueDTO(**result)


async def get_indicator_values_by_id_from_db(
    conn: AsyncConnection,
    indicator_id: int,
    territory_id: Optional[int],
    date_type: Optional[str],
    date_value: Optional[datetime],
) -> List[IndicatorValueDTO]:
    """Get indicator value objects by id."""

    statement = select(territory_indicators_data).where(territory_indicators_data.c.indicator_id == indicator_id)

    if territory_id is not None:
        statement = statement.where(territory_indicators_data.c.territory_id == territory_id)
    if date_type is not None:
        statement = statement.where(territory_indicators_data.c.date_type == date_type)
    if date_value is not None:
        statement = statement.where(territory_indicators_data.c.date_value == date_value)

    result = (await conn.execute(statement)).mappings().all()
    if result is None:
        raise HTTPException(status_code=404, detail="Given id is not found")

    return [IndicatorValueDTO(**value) for value in result]
