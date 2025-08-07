"""Territories indicators internal logic is defined here."""

from collections import defaultdict
from datetime import date
from typing import Callable

from geoalchemy2.functions import ST_AsEWKB
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.common.db.entities import (
    indicators_dict,
    indicators_groups_data,
    measurement_units_dict,
    physical_object_types_dict,
    service_types_dict,
    territories_data,
    territory_indicators_data,
)
from idu_api.urban_api.dto import IndicatorDTO, IndicatorValueDTO, TerritoryWithIndicatorsDTO
from idu_api.urban_api.exceptions.logic.common import EntityNotFoundById
from idu_api.urban_api.logic.impl.helpers.utils import (
    check_existence,
    include_child_territories_cte,
)
from idu_api.urban_api.utils.query_filters import CustomFilter, EqFilter, ILikeFilter, InFilter, apply_filters

func: Callable


async def get_indicators_by_territory_id_from_db(conn: AsyncConnection, territory_id: int) -> list[IndicatorDTO]:
    """Get indicators for a given territory."""

    if not await check_existence(conn, territories_data, conditions={"territory_id": territory_id}):
        raise EntityNotFoundById(territory_id, "territory")

    statement = (
        select(
            indicators_dict,
            measurement_units_dict.c.name.label("measurement_unit_name"),
            service_types_dict.c.name.label("service_type_name"),
            physical_object_types_dict.c.name.label("physical_object_type_name"),
        )
        .select_from(
            territory_indicators_data.join(
                indicators_dict,
                indicators_dict.c.indicator_id == territory_indicators_data.c.indicator_id,
            )
            .outerjoin(
                measurement_units_dict,
                indicators_dict.c.measurement_unit_id == measurement_units_dict.c.measurement_unit_id,
            )
            .outerjoin(service_types_dict, service_types_dict.c.service_type_id == indicators_dict.c.service_type_id)
            .outerjoin(
                physical_object_types_dict,
                physical_object_types_dict.c.physical_object_type_id == indicators_dict.c.physical_object_type_id,
            )
        )
        .where(territory_indicators_data.c.territory_id == territory_id)
    )

    result = (await conn.execute(statement)).mappings().all()

    return [IndicatorDTO(**indicator) for indicator in result]


async def get_indicator_values_by_territory_id_from_db(
    conn: AsyncConnection,
    territory_id: int,
    indicator_ids: set[int] | None,
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
    ).distinct()

    if last_only:
        subquery = (
            select(
                territory_indicators_data.c.indicator_id,
                territory_indicators_data.c.value_type,
                territory_indicators_data.c.territory_id,
                func.max(func.date(territory_indicators_data.c.date_value)).label("max_date"),
            )
            .group_by(
                territory_indicators_data.c.indicator_id,
                territory_indicators_data.c.value_type,
                territory_indicators_data.c.territory_id,
            )
            .subquery()
        )

        statement = statement.select_from(
            territory_indicators_data.join(
                subquery,
                (territory_indicators_data.c.indicator_id == subquery.c.indicator_id)
                & (territory_indicators_data.c.value_type == subquery.c.value_type)
                & (territory_indicators_data.c.date_value == subquery.c.max_date)
                & (territory_indicators_data.c.territory_id == subquery.c.territory_id),
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

    if include_child_territories:
        territories_cte = include_child_territories_cte(territory_id, cities_only)
        territory_filter = CustomFilter(
            lambda q: q.where(territory_indicators_data.c.territory_id.in_(select(territories_cte.c.territory_id)))
        )
    else:
        territory_filter = EqFilter(territory_indicators_data, "territory_id", territory_id)

    statement = apply_filters(
        statement,
        InFilter(territory_indicators_data, "indicator_id", indicator_ids),
        EqFilter(indicators_groups_data, "indicators_group_id", indicators_group_id),
        CustomFilter(
            lambda q: q.where(func.date(territory_indicators_data.c.date_value) >= start_date) if start_date else q
        ),
        CustomFilter(
            lambda q: q.where(func.date(territory_indicators_data.c.date_value) <= end_date) if end_date else q
        ),
        EqFilter(territory_indicators_data, "value_type", value_type),
        ILikeFilter(territory_indicators_data, "information_source", information_source),
        territory_filter,
    )

    result = (await conn.execute(statement)).mappings().all()

    return [IndicatorValueDTO(**indicator_value) for indicator_value in result]


async def get_indicator_values_by_parent_id_from_db(
    conn: AsyncConnection,
    parent_id: int | None,
    indicator_ids: set[int] | None,
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

    statement = (
        select(
            territories_data.c.name.label("territory_name"),
            ST_AsEWKB(territories_data.c.geometry).label("geometry"),
            ST_AsEWKB(territories_data.c.centre_point).label("centre_point"),
            territory_indicators_data,
            indicators_dict.c.parent_id,
            indicators_dict.c.name_full,
            indicators_dict.c.level,
            indicators_dict.c.list_label,
            measurement_units_dict.c.measurement_unit_id,
            measurement_units_dict.c.name.label("measurement_unit_name"),
        )
        .where(
            territories_data.c.parent_id == parent_id
            if parent_id is not None
            else territories_data.c.parent_id.is_(None)
        )
        .distinct()
    )

    if last_only:
        subquery = (
            select(
                territory_indicators_data.c.indicator_id,
                territory_indicators_data.c.value_type,
                territory_indicators_data.c.territory_id,
                func.max(func.date(territory_indicators_data.c.date_value)).label("max_date"),
            )
            .group_by(
                territory_indicators_data.c.indicator_id,
                territory_indicators_data.c.value_type,
                territory_indicators_data.c.territory_id,
            )
            .subquery()
        )

        statement = statement.select_from(
            territory_indicators_data.join(
                subquery,
                (territory_indicators_data.c.indicator_id == subquery.c.indicator_id)
                & (territory_indicators_data.c.value_type == subquery.c.value_type)
                & (territory_indicators_data.c.date_value == subquery.c.max_date)
                & (territory_indicators_data.c.territory_id == subquery.c.territory_id),
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
            .join(territories_data, territories_data.c.territory_id == territory_indicators_data.c.territory_id)
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
            .join(territories_data, territories_data.c.territory_id == territory_indicators_data.c.territory_id)
        )

    statement = apply_filters(
        statement,
        InFilter(territory_indicators_data, "indicator_id", indicator_ids),
        EqFilter(indicators_groups_data, "indicators_group_id", indicators_group_id),
        CustomFilter(
            lambda q: q.where(func.date(territory_indicators_data.c.date_value) >= start_date) if start_date else q
        ),
        CustomFilter(
            lambda q: q.where(func.date(territory_indicators_data.c.date_value) <= end_date) if end_date else q
        ),
        EqFilter(territory_indicators_data, "value_type", value_type),
        ILikeFilter(territory_indicators_data, "information_source", information_source),
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
                IndicatorValueDTO(**{k: v for k, v in row.items() if k in IndicatorValueDTO.fields()}) for row in rows
            ],
        )
        for territory_id, rows in territories.items()
    ]
