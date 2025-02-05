"""Territories indicators internal logic is defined here."""

from collections import defaultdict
from datetime import date
from typing import Callable

from geoalchemy2.functions import ST_AsGeoJSON
from sqlalchemy import cast, func, select, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.common.db.entities import (
    indicators_dict,
    indicators_groups_data,
    measurement_units_dict,
    territories_data,
    territory_indicators_data,
)
from idu_api.urban_api.dto import IndicatorDTO, IndicatorValueDTO, TerritoryWithIndicatorsDTO
from idu_api.urban_api.exceptions.logic.common import EntityNotFoundById
from idu_api.urban_api.logic.impl.helpers.utils import (
    DECIMAL_PLACES,
    check_existence,
    include_child_territories_cte,
)

func: Callable


async def get_indicators_by_territory_id_from_db(conn: AsyncConnection, territory_id: int) -> list[IndicatorDTO]:
    """Get indicators for a given territory."""

    if not await check_existence(conn, territories_data, conditions={"territory_id": territory_id}):
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
    start_date: date | None,
    end_date: date | None,
    value_type: str | None,
    information_source: str | None,
    last_only: bool,
    include_child_territories: bool,
    cities_only: bool,
) -> list[IndicatorValueDTO]:
    """Get indicator values by territory id, optional indicator_ids, value_type, source and time period.

    Could be specified by last_only to get only last indicator values.
    """

    if not await check_existence(conn, territories_data, conditions={"territory_id": territory_id}):
        raise EntityNotFoundById(territory_id, "territory")

    statement = select(
        territory_indicators_data,
        indicators_dict.c.parent_id,
        indicators_dict.c.name_full,
        indicators_dict.c.level,
        indicators_dict.c.list_label,
        measurement_units_dict.c.measurement_unit_id,
        measurement_units_dict.c.name.label("measurement_unit_name"),
        territories_data.c.name.label("territory_name"),
    )

    if last_only:
        subquery = (
            select(
                territory_indicators_data.c.indicator_id,
                territory_indicators_data.c.value_type,
                func.max(func.date(territory_indicators_data.c.date_value)).label("max_date"),
            )
            .select_from(
                territory_indicators_data.join(
                    territories_data, territories_data.c.territory_id == territory_indicators_data.c.territory_id
                )
            )
            .where(territory_indicators_data.c.territory_id == territory_id)
            .group_by(
                territory_indicators_data.c.indicator_id,
                territory_indicators_data.c.value_type,
            )
            .subquery()
        )

        statement = statement.select_from(
            territory_indicators_data.join(
                subquery,
                (territory_indicators_data.c.indicator_id == subquery.c.indicator_id)
                & (territory_indicators_data.c.value_type == subquery.c.value_type)
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
            .join(
                territories_data,
                territories_data.c.territory_id == territory_indicators_data.c.territory_id,
            )
        )
    else:
        statement = statement.select_from(
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
            .join(
                territories_data,
                territories_data.c.territory_id == territory_indicators_data.c.territory_id,
            )
        )

    if indicator_ids is not None:
        ids = {int(indicator.strip()) for indicator in indicator_ids.split(",")}
        statement = statement.where(territory_indicators_data.c.indicator_id.in_(ids))
    if indicators_group_id is not None:
        statement = statement.where(indicators_groups_data.c.indicators_group_id == indicators_group_id)
    if start_date is not None:
        statement = statement.where(func.date(territory_indicators_data.c.date_value) >= start_date)
    if end_date is not None:
        statement = statement.where(func.date(territory_indicators_data.c.date_value) <= end_date)
    if value_type is not None:
        statement = statement.where(territory_indicators_data.c.value_type == value_type)
    if information_source is not None:
        statement = statement.where(territory_indicators_data.c.information_source.ilike(f"%{information_source}%"))
    if include_child_territories:
        territories_cte = include_child_territories_cte(territory_id, cities_only)
        statement = statement.where(
            territory_indicators_data.c.territory_id.in_(select(territories_cte.c.territory_id))
        )
    else:
        statement = statement.where(territory_indicators_data.c.territory_id == territory_id)

    result = (await conn.execute(statement)).mappings().all()

    return [IndicatorValueDTO(**indicator_value) for indicator_value in result]


async def get_indicator_values_by_parent_id_from_db(
    conn: AsyncConnection,
    parent_id: int | None,
    indicator_ids: str | None,
    indicators_group_id: int | None,
    start_date: date | None,
    end_date: date | None,
    value_type: str | None,
    information_source: str | None,
    last_only: bool,
) -> list[TerritoryWithIndicatorsDTO]:
    """Get indicator values for child territories by parent id, optional indicator_ids, value_type, source and date.

    Could be specified by last_only to get only last indicator values.
    """

    if parent_id is not None:
        if not await check_existence(conn, territories_data, conditions={"territory_id": parent_id}):
            raise EntityNotFoundById(parent_id, "territory")

    statement = select(
        territories_data.c.territory_id,
        territories_data.c.name,
        territories_data.c.geometry,
        territories_data.c.centre_point,
    ).where(territories_data.c.parent_id == parent_id)
    territories_cte = statement.cte(name="territories_cte")

    base_select_from = (
        territories_cte.join(
            territory_indicators_data, territories_cte.c.territory_id == territory_indicators_data.c.territory_id
        )
        .join(indicators_dict, indicators_dict.c.indicator_id == territory_indicators_data.c.indicator_id)
        .outerjoin(
            measurement_units_dict,
            measurement_units_dict.c.measurement_unit_id == indicators_dict.c.measurement_unit_id,
        )
    )

    statement = select(
        territories_cte.c.name.label("territory_name"),
        cast(ST_AsGeoJSON(territories_cte.c.geometry, DECIMAL_PLACES), JSONB).label("geometry"),
        cast(ST_AsGeoJSON(territories_cte.c.centre_point, DECIMAL_PLACES), JSONB).label("centre_point"),
        territory_indicators_data,
        indicators_dict.c.parent_id,
        indicators_dict.c.name_full,
        indicators_dict.c.level,
        indicators_dict.c.list_label,
        measurement_units_dict.c.measurement_unit_id,
        measurement_units_dict.c.name.label("measurement_unit_name"),
    )

    if indicator_ids is not None:
        ids = {int(i.strip()) for i in indicator_ids.split(",") if i.strip()}
        statement = statement.where(territory_indicators_data.c.indicator_id.in_(ids))
    if indicators_group_id is not None:
        base_select_from = base_select_from.outerjoin(
            indicators_groups_data, indicators_groups_data.c.indicator_id == indicators_dict.c.indicator_id
        )
        statement = statement.where(indicators_groups_data.c.indicators_group_id == indicators_group_id)
    if start_date is not None:
        statement = statement.where(func.date(territory_indicators_data.c.date_value) >= start_date)
    if end_date is not None:
        statement = statement.where(func.date(territory_indicators_data.c.date_value) <= end_date)
    if value_type is not None:
        statement = statement.where(territory_indicators_data.c.value_type == value_type)
    if information_source is not None:
        statement = statement.where(territory_indicators_data.c.information_source.ilike(f"%{information_source}%"))

    statement = statement.select_from(base_select_from)

    if last_only:
        statement = (
            statement.add_columns(
                func.row_number()
                .over(
                    partition_by=[
                        territory_indicators_data.c.territory_id,
                        territory_indicators_data.c.indicator_id,
                        territory_indicators_data.c.value_type,
                    ],
                    order_by=territory_indicators_data.c.date_value.desc(),
                )
                .label("row_num")
            )
            .alias("last_values")
            .select()
            .where(text("row_num = 1"))
        )

    result = (await conn.execute(statement)).mappings().all()

    territories = defaultdict(list)
    for row in result:
        territories[row.territory_id].append(row)

    return [
        TerritoryWithIndicatorsDTO(
            territory_id=territory_id,
            name=rows[0].territory_name,
            geometry=rows[0].geometry,
            centre_point=rows[0].centre_point,
            indicators=[
                IndicatorValueDTO(**{k: v for k, v in item.items() if k in IndicatorValueDTO.fields()}) for item in rows
            ],
        )
        for territory_id, rows in territories.items()
    ]
