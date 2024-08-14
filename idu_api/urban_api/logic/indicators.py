"""Indicators handlers logic of getting entities from the database is defined here."""

from datetime import datetime
from typing import List, Optional

from sqlalchemy import insert, select
from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.common.db.entities import (
    indicators_dict,
    measurement_units_dict,
    territories_data,
    territory_indicators_data,
)
from idu_api.urban_api.dto import (
    IndicatorDTO,
    IndicatorValueDTO,
    MeasurementUnitDTO,
)
from idu_api.urban_api.exceptions.logic.common import EntityAlreadyExists, EntityNotFoundById, EntityNotFoundByParams
from idu_api.urban_api.schemas import IndicatorsPost, IndicatorValuePost, MeasurementUnitPost


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
        raise EntityAlreadyExists("measurement unit", measurement_unit.name)

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
        parent_indicator = (await conn.execute(statement)).one_or_none()
        if parent_indicator is None:
            raise EntityNotFoundById(parent_id, "indicator")

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
        raise EntityNotFoundById(indicator_id, "indicator")

    return IndicatorDTO(**result)


async def add_indicator_to_db(conn: AsyncConnection, indicator: IndicatorsPost) -> IndicatorDTO:
    """Create indicator object."""

    if indicator.parent_id is not None:
        statement = select(indicators_dict).where(indicators_dict.c.indicator_id == indicator.parent_id)
        parent_indicator = (await conn.execute(statement)).one_or_none()
        if parent_indicator is None:
            raise EntityNotFoundById(indicator.parent_id, "indicator")

    statement = select(indicators_dict).where(indicators_dict.c.name_full == indicator.name_full)
    indicator_name = (await conn.execute(statement)).one_or_none()
    if indicator_name is not None:
        raise EntityAlreadyExists("indicator", indicator.name_full)

    statement = select(measurement_units_dict).where(
        indicators_dict.c.measurement_unit_id == indicator.measurement_unit_id
    )
    measurement_unit = (await conn.execute(statement)).one_or_none()
    if measurement_unit is None:
        raise EntityNotFoundById(indicator.measurement_unit_id, "measurement unit")

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
    conn: AsyncConnection,
    indicator_id: int,
    territory_id: int,
    date_type: str,
    date_value: datetime,
    value_type: str,
    information_source: str,
) -> IndicatorValueDTO:
    """Get indicator value object by id."""

    statement = (
        select(
            territory_indicators_data,
            indicators_dict.c.name_full,
            measurement_units_dict.c.measurement_unit_id,
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
            territory_indicators_data.c.indicator_id == indicator_id,
            territory_indicators_data.c.territory_id == territory_id,
            territory_indicators_data.c.date_type == date_type,
            territory_indicators_data.c.date_value == date_value,
            territory_indicators_data.c.value_type == value_type,
            territory_indicators_data.c.information_source == information_source,
        )
    )
    result = (await conn.execute(statement)).mappings().one_or_none()
    if result is None:
        raise EntityNotFoundByParams(
            "indicator value",
            indicator_id,
            territory_id,
            date_type,
            date_value,
            value_type,
            information_source,
        )

    return IndicatorValueDTO(**result)


async def add_indicator_value_to_db(
    conn: AsyncConnection,
    indicator_value: IndicatorValuePost,
) -> IndicatorValueDTO:
    """Create indicator value object."""

    statement = select(indicators_dict).where(indicators_dict.c.indicator_id == indicator_value.indicator_id)
    indicator = (await conn.execute(statement)).one_or_none()
    if indicator is None:
        raise EntityNotFoundById(indicator_value.indicator_id, "indicator")

    statement = select(territory_indicators_data).where(
        territory_indicators_data.c.indicator_id == indicator_value.indicator_id,
        territory_indicators_data.c.territory_id == indicator_value.territory_id,
        territory_indicators_data.c.date_type == indicator_value.date_type,
        territory_indicators_data.c.date_value == indicator_value.date_value,
        territory_indicators_data.c.value_type == indicator_value.value_type,
        territory_indicators_data.c.information_source == indicator_value.information_source,
    )
    result = (await conn.execute(statement)).one_or_none()
    if result is not None:
        raise EntityAlreadyExists(
            "indicator value",
            indicator_value.indicator_id,
            indicator_value.territory_id,
            indicator_value.date_type,
            indicator_value.date_value,
            indicator_value.value_type,
            indicator_value.information_source,
        )

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

    return await get_indicator_value_by_id_from_db(
        conn,
        result.indicator_id,
        result.territory_id,
        result.date_type,
        result.date_value,
        result.value_type,
        result.information_source,
    )


async def get_indicator_values_by_id_from_db(
    conn: AsyncConnection,
    indicator_id: int,
    territory_id: Optional[int],
    date_type: Optional[str],
    date_value: Optional[datetime],
    value_type: Optional[str],
    information_source: Optional[str],
) -> List[IndicatorValueDTO]:
    """Get indicator values objects by indicator id."""

    statement = select(indicators_dict).where(indicators_dict.c.indicator_id == indicator_id)
    parent_indicator = (await conn.execute(statement)).one_or_none()
    if parent_indicator is None:
        raise EntityNotFoundById(indicator_id, "indicator")

    statement = (
        select(
            territory_indicators_data,
            indicators_dict.c.name_full,
            measurement_units_dict.c.measurement_unit_id,
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
        .where(territory_indicators_data.c.indicator_id == indicator_id)
    )

    if territory_id is not None:
        query = select(territories_data).where(territories_data.c.territory_id == territory_id)
        territory = (await conn.execute(query)).one_or_none()
        if territory is None:
            raise EntityNotFoundById(territory_id, "territory")
        statement = statement.where(territory_indicators_data.c.territory_id == territory_id)
    if date_type is not None:
        statement = statement.where(territory_indicators_data.c.date_type == date_type)
    if date_value is not None:
        statement = statement.where(territory_indicators_data.c.date_value == date_value)
    if value_type is not None:
        statement = statement.where(territory_indicators_data.c.value_type == value_type)
    if information_source is not None:
        statement = statement.where(territory_indicators_data.c.information_source.ilike(f"%{information_source}%"))

    result = (await conn.execute(statement)).mappings().all()

    return [IndicatorValueDTO(**value) for value in result]
