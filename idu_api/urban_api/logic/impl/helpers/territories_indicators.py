"""Territories indicators internal logic is defined here."""

from datetime import datetime
from typing import Callable

from geoalchemy2.functions import ST_AsGeoJSON
from sqlalchemy import cast, func, select
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.common.db.entities import (
    indicators_dict,
    indicators_groups_data,
    indicators_groups_dict,
    measurement_units_dict,
    territories_data,
    territory_indicators_data,
)
from idu_api.urban_api.dto import IndicatorDTO, IndicatorValueDTO, TerritoryWithIndicatorsDTO
from idu_api.urban_api.exceptions.logic.common import EntitiesNotFoundByIds, EntityNotFoundById

func: Callable


async def get_indicators_by_territory_id_from_db(
    conn: AsyncConnection,
    territory_id: int,
) -> list[IndicatorDTO]:
    """Get indicators for a given territory."""

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
    conn: AsyncConnection,
    territory_id: int,
    indicator_ids: str | None,
    indicators_group_id: int | None,
    start_date: datetime | None,
    end_date: datetime | None,
    value_type: str | None,
    information_source: str | None,
    last_only: bool,
) -> list[IndicatorValueDTO]:
    """Get indicator values by territory id, optional indicator_ids, value_type, source and time period.

    Could be specified by last_only to get only last indicator values.
    """

    statement = select(territories_data).where(territories_data.c.territory_id == territory_id)
    territory = (await conn.execute(statement)).one_or_none()
    if territory is None:
        raise EntityNotFoundById(territory_id, "territory")

    if last_only:
        subquery = (
            select(
                territory_indicators_data.c.indicator_id,
                territory_indicators_data.c.value_type,
                territory_indicators_data.c.information_source,
                func.max(func.date(territory_indicators_data.c.date_value)).label("max_date"),
            )
            .where(territory_indicators_data.c.territory_id == territory_id)
            .group_by(
                territory_indicators_data.c.indicator_id,
                territory_indicators_data.c.value_type,
                territory_indicators_data.c.information_source,
            )
            .subquery()
        )

        statement = (
            select(
                territory_indicators_data,
                indicators_dict.c.name_full,
                indicators_dict.c.level,
                indicators_dict.c.list_label,
                measurement_units_dict.c.measurement_unit_id,
                measurement_units_dict.c.name.label("measurement_unit_name"),
            )
            .select_from(
                territory_indicators_data.join(
                    subquery,
                    (territory_indicators_data.c.indicator_id == subquery.c.indicator_id)
                    & (territory_indicators_data.c.value_type == subquery.c.value_type)
                    & (territory_indicators_data.c.information_source == subquery.c.information_source)
                    & (territory_indicators_data.c.date_value == subquery.c.max_date),
                )
                .join(
                    indicators_dict,
                    indicators_dict.c.indicator_id == territory_indicators_data.c.indicator_id,
                )
                .outerjoin(
                    measurement_units_dict,
                    measurement_units_dict.c.measurement_unit_id == indicators_dict.c.measurement_unit_id,
                )
                .outerjoin(
                    indicators_groups_data,
                    indicators_groups_data.c.indicator_id == indicators_dict.c.indicator_id,
                )
            )
            .where(territory_indicators_data.c.territory_id == territory_id)
        )
    else:
        statement = (
            select(
                territory_indicators_data,
                indicators_dict.c.name_full,
                indicators_dict.c.level,
                indicators_dict.c.list_label,
                measurement_units_dict.c.measurement_unit_id,
                measurement_units_dict.c.name.label("measurement_unit_name"),
            )
            .select_from(
                territory_indicators_data.join(
                    indicators_dict,
                    indicators_dict.c.indicator_id == territory_indicators_data.c.indicator_id,
                )
                .outerjoin(
                    measurement_units_dict,
                    measurement_units_dict.c.measurement_unit_id == indicators_dict.c.measurement_unit_id,
                )
                .outerjoin(
                    indicators_groups_data,
                    indicators_groups_data.c.indicator_id == indicators_dict.c.indicator_id,
                )
            )
            .where(
                territory_indicators_data.c.territory_id == territory_id,
            )
        )

    if indicators_group_id is not None:
        query = select(indicators_groups_dict).where(
            indicators_groups_dict.c.indicators_group_id == indicators_group_id
        )
        indicators_group = (await conn.execute(query)).scalar_one_or_none()
        if indicators_group is None:
            raise EntityNotFoundById(indicators_group_id, "indicators group")

        statement = statement.where(indicators_groups_data.c.indicators_group_id == indicators_group_id)

    if indicator_ids is not None:
        ids = [int(indicator.strip()) for indicator in indicator_ids.split(",")]
        query = select(indicators_dict.c.indicator_id).where(indicators_dict.c.indicator_id.in_(ids))
        indicators = (await conn.execute(query)).scalars()
        if not list(indicators):
            raise EntitiesNotFoundByIds("indicator")

        statement = statement.where(territory_indicators_data.c.indicator_id.in_(ids))

    if start_date is not None:
        statement = statement.where(func.date(territory_indicators_data.c.date_value) >= start_date)
    if end_date is not None:
        statement = statement.where(func.date(territory_indicators_data.c.date_value) <= end_date)
    if value_type is not None:
        statement = statement.where(territory_indicators_data.c.value_type == value_type)
    if information_source is not None:
        statement = statement.where(territory_indicators_data.c.information_source.ilike(f"%{information_source}%"))

    result = (await conn.execute(statement)).mappings().all()

    return [IndicatorValueDTO(**indicator_value) for indicator_value in result]


async def get_indicator_values_by_parent_id_from_db(
    conn: AsyncConnection,
    parent_id: int | None,
    indicator_ids: str | None,
    indicators_group_id: int | None,
    start_date: datetime | None,
    end_date: datetime | None,
    value_type: str | None,
    information_source: str | None,
    last_only: bool,
) -> list[TerritoryWithIndicatorsDTO]:
    """Get indicator values for child territories by parent id, optional indicator_ids, value_type, source and date.

    Could be specified by last_only to get only last indicator values.
    """

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
        (territories_data.c.parent_id == parent_id if parent_id is not None else territories_data.c.parent_id.is_(None))
    )
    child_territories = (await conn.execute(statement)).mappings().all()

    results = []
    for child_territory in child_territories:
        indicators = await get_indicator_values_by_territory_id_from_db(
            conn,
            child_territory.territory_id,
            indicator_ids,
            indicators_group_id,
            start_date,
            end_date,
            value_type,
            information_source,
            last_only,
        )
        results.append(TerritoryWithIndicatorsDTO(**child_territory, indicators=indicators))

    return results
