"""Indicators internal logic is defined here."""

from collections import defaultdict
from collections.abc import Callable
from datetime import datetime, timezone

from sqlalchemy import delete, func, insert, select, update
from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.common.db.entities import (
    indicators_dict,
    indicators_groups_data,
    indicators_groups_dict,
    measurement_units_dict,
    territories_data,
    territory_indicators_data, service_types_dict, physical_object_types_dict,
)
from idu_api.urban_api.dto import (
    IndicatorDTO,
    IndicatorsGroupDTO,
    IndicatorValueDTO,
    MeasurementUnitDTO,
)
from idu_api.urban_api.exceptions.logic.common import (
    EntitiesNotFoundByIds,
    EntityAlreadyExists,
    EntityNotFoundById,
    EntityNotFoundByParams,
)
from idu_api.urban_api.logic.impl.helpers.utils import build_recursive_query, check_existence, extract_values_from_model
from idu_api.urban_api.schemas import (
    IndicatorsGroupPost,
    IndicatorsPatch,
    IndicatorPost,
    IndicatorPut,
    IndicatorValuePost,
    IndicatorValuePut,
    MeasurementUnitPost,
)

func: Callable


async def get_measurement_units_from_db(conn: AsyncConnection) -> list[MeasurementUnitDTO]:
    """Get all measurement unit objects."""

    statement = select(measurement_units_dict).order_by(measurement_units_dict.c.measurement_unit_id)

    return [MeasurementUnitDTO(**unit) for unit in (await conn.execute(statement)).mappings().all()]


async def add_measurement_unit_to_db(
    conn: AsyncConnection,
    measurement_unit: MeasurementUnitPost,
) -> MeasurementUnitDTO:
    """Create measurement unit object."""

    if await check_existence(conn, measurement_units_dict, conditions={"name": measurement_unit.name}):
        raise EntityAlreadyExists("measurement unit", measurement_unit.name)

    statement = insert(measurement_units_dict).values(name=measurement_unit.name).returning(measurement_units_dict)
    result = (await conn.execute(statement)).mappings().one()

    await conn.commit()

    return MeasurementUnitDTO(**result)


async def get_indicators_groups_from_db(conn: AsyncConnection) -> list[IndicatorsGroupDTO]:
    """Get all indicators group objects."""

    statement = (
        select(
            indicators_groups_dict.c.indicators_group_id,
            indicators_groups_dict.c.name.label("group_name"),
            indicators_dict,
            measurement_units_dict.c.name.label("measurement_unit_name"),
            service_types_dict.c.name.label("service_type_name"),
            physical_object_types_dict.c.name.label("physical_object_type_name"),
        )
        .select_from(
            indicators_groups_dict.outerjoin(
                indicators_groups_data,
                indicators_groups_dict.c.indicators_group_id == indicators_groups_data.c.indicators_group_id,
            )
            .outerjoin(indicators_dict, indicators_groups_data.c.indicator_id == indicators_dict.c.indicator_id)
            .outerjoin(
                measurement_units_dict,
                indicators_dict.c.measurement_unit_id == measurement_units_dict.c.measurement_unit_id,
            )
            .outerjoin(service_types_dict, service_types_dict.c.service_type_id == indicators_dict.c.service_type_id)
            .outerjoin(
                physical_object_types_dict,
                physical_object_types_dict.c.physical_object_type_id == indicators_dict.c.physical_object_type_id
            )
        )
        .order_by(indicators_groups_dict.c.indicators_group_id, indicators_dict.c.indicator_id)
    )

    rows = (await conn.execute(statement)).mappings().all()

    result = defaultdict(lambda: {"indicators": []})
    for row in rows:
        key = row.indicators_group_id
        if key not in result:
            result[key].update({"indicators_group_id": key, "name": row.group_name})
        if row.indicator_id is not None:
            indicator = {key: row[key] for key in IndicatorDTO.fields()}
            result[key]["indicators"].append(IndicatorDTO(**indicator))

    return [IndicatorsGroupDTO(**group) for group in result.values()]


async def add_indicators_group_to_db(
    conn: AsyncConnection,
    indicators_group: IndicatorsGroupPost,
) -> IndicatorsGroupDTO:
    """Create indicators group object."""

    statement = (
        select(
            indicators_dict,
            measurement_units_dict.c.name.label("measurement_unit_name"),
            service_types_dict.c.name.label("service_type_name"),
            physical_object_types_dict.c.name.label("physical_object_type_name"),
        ).select_from(
            indicators_dict.outerjoin(
                measurement_units_dict,
                indicators_dict.c.measurement_unit_id == measurement_units_dict.c.measurement_unit_id,
            )
            .outerjoin(service_types_dict, service_types_dict.c.service_type_id == indicators_dict.c.service_type_id)
            .outerjoin(
                physical_object_types_dict,
                physical_object_types_dict.c.physical_object_type_id == indicators_dict.c.physical_object_type_id
            )
        )
    ).where(indicators_dict.c.indicator_id.in_(indicators_group.indicators_ids))
    indicators = (await conn.execute(statement)).mappings().all()
    indicators = [IndicatorDTO(**indicator) for indicator in indicators]
    if len(indicators) < len(indicators_group.indicators_ids):
        raise EntitiesNotFoundByIds("indicator")

    if await check_existence(conn, indicators_groups_dict, conditions={"name": indicators_group.name}):
        raise EntityAlreadyExists("indicators group", indicators_group.name)

    statement = (
        insert(indicators_groups_dict)
        .values(name=indicators_group.name)
        .returning(indicators_groups_dict.c.indicators_group_id)
    )
    indicators_group_id = (await conn.execute(statement)).scalar_one()

    statement = insert(indicators_groups_data).values(
        [
            {"indicators_group_id": indicators_group_id, "indicator_id": indicator_id}
            for indicator_id in indicators_group.indicators_ids
        ]
    )

    await conn.execute(statement)
    await conn.commit()

    return IndicatorsGroupDTO(
        indicators_group_id=indicators_group_id,
        name=indicators_group.name,
        indicators=indicators,
    )


async def update_indicators_group_from_db(
    conn: AsyncConnection, indicators_group: IndicatorsGroupPost
) -> IndicatorsGroupDTO:
    """Update indicators group object."""

    statement = (
        select(
            indicators_dict,
            measurement_units_dict.c.name.label("measurement_unit_name"),
            service_types_dict.c.name.label("service_type_name"),
            physical_object_types_dict.c.name.label("physical_object_type_name"),
        ).select_from(
            indicators_dict.outerjoin(
                measurement_units_dict,
                indicators_dict.c.measurement_unit_id == measurement_units_dict.c.measurement_unit_id,
            )
            .outerjoin(service_types_dict, service_types_dict.c.service_type_id == indicators_dict.c.service_type_id)
            .outerjoin(
                physical_object_types_dict,
                physical_object_types_dict.c.physical_object_type_id == indicators_dict.c.physical_object_type_id
            )
        )
    ).where(indicators_dict.c.indicator_id.in_(indicators_group.indicators_ids))
    indicators = (await conn.execute(statement)).mappings().all()
    indicators = [IndicatorDTO(**indicator) for indicator in indicators]
    if len(indicators) < len(indicators_group.indicators_ids):
        raise EntitiesNotFoundByIds("indicator")

    statement = select(indicators_groups_dict.c.indicators_group_id).where(
        indicators_groups_dict.c.name == indicators_group.name
    )
    indicators_group_id = (await conn.execute(statement)).scalar_one_or_none()

    if indicators_group_id is not None:
        statement = delete(indicators_groups_data).where(
            indicators_groups_data.c.indicators_group_id == indicators_group_id
        )
        await conn.execute(statement)
    else:
        statement = (
            insert(indicators_groups_dict)
            .values(name=indicators_group.name)
            .returning(indicators_groups_dict.c.indicators_group_id)
        )
        indicators_group_id = (await conn.execute(statement)).scalar_one()

    statement = insert(indicators_groups_data).values(
        [
            {"indicators_group_id": indicators_group_id, "indicator_id": indicator_id}
            for indicator_id in indicators_group.indicators_ids
        ]
    )

    await conn.execute(statement)
    await conn.commit()

    return IndicatorsGroupDTO(
        indicators_group_id=indicators_group_id,
        name=indicators_group.name,
        indicators=indicators,
    )


async def get_indicators_by_group_id_from_db(conn: AsyncConnection, indicators_group_id: int) -> list[IndicatorDTO]:
    """Get all indicators by indicators group id."""

    if not await check_existence(conn, indicators_groups_data, conditions={"indicators_group_id": indicators_group_id}):
        raise EntityNotFoundById(indicators_group_id, "indicators group")

    statement = (
        select(
            indicators_dict,
            measurement_units_dict.c.name.label("measurement_unit_name"),
            service_types_dict.c.name.label("service_type_name"),
            physical_object_types_dict.c.name.label("physical_object_type_name"),
        )
        .select_from(
            indicators_groups_data.join(
                indicators_dict,
                indicators_dict.c.indicator_id == indicators_groups_data.c.indicator_id,
            ).outerjoin(
                measurement_units_dict,
                indicators_dict.c.measurement_unit_id == measurement_units_dict.c.measurement_unit_id,
            )
            .outerjoin(service_types_dict, service_types_dict.c.service_type_id == indicators_dict.c.service_type_id)
            .outerjoin(
                physical_object_types_dict,
                physical_object_types_dict.c.physical_object_type_id == indicators_dict.c.physical_object_type_id
            )
        )
        .where(indicators_groups_data.c.indicators_group_id == indicators_group_id)
    )
    indicators = (await conn.execute(statement)).mappings().all()

    return [IndicatorDTO(**indicator) for indicator in indicators]


async def get_indicators_by_parent_from_db(
    conn: AsyncConnection,
    parent_id: int | None,
    parent_name: str | None,
    name: str | None,
    territory_id: int | None,
    service_type_id: int | None,
    physical_object_type_id: int | None,
    get_all_subtree: bool,
) -> list[IndicatorDTO]:
    """Get an indicator or list of indicators by parent id or name."""

    if parent_id is not None:
        if not await check_existence(conn, indicators_dict, conditions={"indicator_id": parent_id}):
            raise EntityNotFoundById(parent_id, "indicator")
    if parent_name is not None:
        statement = select(indicators_dict.c.indicator_id).where(indicators_dict.c.name_full == parent_name.strip())
        parent_indicator_id = (await conn.execute(statement)).scalar_one_or_none()
        if parent_indicator_id is None:
            raise EntityNotFoundByParams("indicator", parent_name)
        parent_id = parent_indicator_id

    statement = (
        select(
            indicators_dict,
            measurement_units_dict.c.name.label("measurement_unit_name"),
            service_types_dict.c.name.label("service_type_name"),
            physical_object_types_dict.c.name.label("physical_object_type_name"),
        )
        .select_from(
            indicators_dict.outerjoin(
                measurement_units_dict,
                indicators_dict.c.measurement_unit_id == measurement_units_dict.c.measurement_unit_id,
            )
            .outerjoin(service_types_dict, service_types_dict.c.service_type_id == indicators_dict.c.service_type_id)
            .outerjoin(
                physical_object_types_dict,
                physical_object_types_dict.c.physical_object_type_id == indicators_dict.c.physical_object_type_id
            )
        )
    )

    if get_all_subtree:
        statement = build_recursive_query(statement, indicators_dict, parent_id, "indicators_recursive", "indicator_id")
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

    if service_type_id is not None:
        statement = statement.where(requested_indicators.c.service_type_id == service_type_id)
    if physical_object_type_id is not None:
        statement = statement.where(requested_indicators.c.physical_object_type_id == physical_object_type_id)

    result = (await conn.execute(statement)).mappings().all()

    return [IndicatorDTO(**indicator) for indicator in result]


async def get_indicator_by_id_from_db(conn: AsyncConnection, indicator_id: int) -> IndicatorDTO:
    """Get indicator object by id."""

    statement = (
        select(
            indicators_dict,
            measurement_units_dict.c.name.label("measurement_unit_name"),
            service_types_dict.c.name.label("service_type_name"),
            physical_object_types_dict.c.name.label("physical_object_type_name"),
        )
        .select_from(
            indicators_dict.outerjoin(
                measurement_units_dict,
                indicators_dict.c.measurement_unit_id == measurement_units_dict.c.measurement_unit_id,
            )
            .outerjoin(service_types_dict, service_types_dict.c.service_type_id == indicators_dict.c.service_type_id)
            .outerjoin(
                physical_object_types_dict,
                physical_object_types_dict.c.physical_object_type_id == indicators_dict.c.physical_object_type_id
            )
        )
        .where(indicators_dict.c.indicator_id == indicator_id)
    )

    result = (await conn.execute(statement)).mappings().one_or_none()
    if result is None:
        raise EntityNotFoundById(indicator_id, "indicator")

    return IndicatorDTO(**result)


async def add_indicator_to_db(conn: AsyncConnection, indicator: IndicatorPost) -> IndicatorDTO:
    """Create indicator object."""

    if indicator.parent_id is not None:
        if not await check_existence(conn, indicators_dict, conditions={"indicator_id": indicator.parent_id}):
            raise EntityNotFoundById(indicator.parent_id, "indicator")

    if indicator.measurement_unit_id is not None:
        if not await check_existence(
            conn, measurement_units_dict, conditions={"measurement_unit_id": indicator.measurement_unit_id}
        ):
            raise EntityNotFoundById(indicator.measurement_unit_id, "measurement unit")

    if indicator.service_type_id is not None:
        if not await check_existence(
            conn, service_types_dict, conditions={"service_type_id": indicator.service_type_id}
        ):
            raise EntityNotFoundById(indicator.service_type_id, "service type")

    if indicator.physical_object_type_id is not None:
        if not await check_existence(
            conn, physical_object_types_dict, conditions={"physical_object_type_id": indicator.physical_object_type_id}
        ):
            raise EntityNotFoundById(indicator.physical_object_type_id, "physical object type")

    if await check_existence(conn, indicators_dict, conditions={"name_full": indicator.name_full}):
        raise EntityAlreadyExists("indicator", indicator.name_full)

    statement = insert(indicators_dict).values(**indicator.model_dump()).returning(indicators_dict.c.indicator_id)
    indicator_id = (await conn.execute(statement)).scalar_one()

    await conn.commit()

    return await get_indicator_by_id_from_db(conn, indicator_id)


async def put_indicator_to_db(conn: AsyncConnection, indicator: IndicatorPut) -> IndicatorDTO:
    """Update indicator object by all its attributes."""

    if indicator.parent_id is not None:
        if not await check_existence(conn, indicators_dict, conditions={"indicator_id": indicator.parent_id}):
            raise EntityNotFoundById(indicator.parent_id, "indicator")

    if indicator.measurement_unit_id is not None:
        if not await check_existence(
            conn, measurement_units_dict, conditions={"measurement_unit_id": indicator.measurement_unit_id}
        ):
            raise EntityNotFoundById(indicator.measurement_unit_id, "measurement unit")

    if indicator.service_type_id is not None:
        if not await check_existence(
            conn, service_types_dict, conditions={"service_type_id": indicator.service_type_id}
        ):
            raise EntityNotFoundById(indicator.service_type_id, "service type")

    if indicator.physical_object_type_id is not None:
        if not await check_existence(
            conn, physical_object_types_dict, conditions={"physical_object_type_id": indicator.physical_object_type_id}
        ):
            raise EntityNotFoundById(indicator.physical_object_type_id, "physical object type")

    if await check_existence(conn, indicators_dict, conditions={"name_full": indicator.name_full}):
        values = extract_values_from_model(indicator, to_update=True)
        statement = (
            update(indicators_dict)
            .where(indicators_dict.c.name_full == indicator.name_full)
            .values(**values)
            .returning(indicators_dict.c.indicator_id)
        )
    else:
        statement = insert(indicators_dict).values(**indicator.model_dump()).returning(indicators_dict.c.indicator_id)

    indicator_id = (await conn.execute(statement)).scalar_one()
    await conn.commit()

    return await get_indicator_by_id_from_db(conn, indicator_id)


async def patch_indicator_to_db(conn: AsyncConnection, indicator_id: int, indicator: IndicatorsPatch) -> IndicatorDTO:
    """Update indicator object by only given attributes."""

    if not await check_existence(conn, indicators_dict, conditions={"indicator_id": indicator_id}):
        raise EntityNotFoundById(indicator_id, "indicator")

    if indicator.parent_id is not None:
        if not await check_existence(conn, indicators_dict, conditions={"indicator_id": indicator.parent_id}):
            raise EntityNotFoundById(indicator.parent_id, "indicator")

    if indicator.measurement_unit_id is not None:
        if not await check_existence(
            conn, measurement_units_dict, conditions={"measurement_unit_id": indicator.measurement_unit_id}
        ):
            raise EntityNotFoundById(indicator.measurement_unit_id, "measurement unit")

    if indicator.service_type_id is not None:
        if not await check_existence(
            conn, service_types_dict, conditions={"service_type_id": indicator.service_type_id}
        ):
            raise EntityNotFoundById(indicator.service_type_id, "service type")

    if indicator.physical_object_type_id is not None:
        if not await check_existence(
            conn, physical_object_types_dict, conditions={"physical_object_type_id": indicator.physical_object_type_id}
        ):
            raise EntityNotFoundById(indicator.physical_object_type_id, "physical object type")

    if indicator.name_full is not None:
        if await check_existence(
            conn,
            indicators_dict,
            conditions={"name_full": indicator.name_full},
            not_conditions={"indicator_id": indicator_id},
        ):
            raise EntityAlreadyExists("indicator", indicator.name_full)

    values = extract_values_from_model(indicator, exclude_unset=True, to_update=True)

    statement = update(indicators_dict).where(indicators_dict.c.indicator_id == indicator_id).values(**values)

    await conn.execute(statement)
    await conn.commit()

    return await get_indicator_by_id_from_db(conn, indicator_id)


async def delete_indicator_from_db(conn: AsyncConnection, indicator_id: int) -> dict:
    """Delete indicator object by id."""

    if not await check_existence(conn, indicators_dict, conditions={"indicator_id": indicator_id}):
        raise EntityNotFoundById(indicator_id, "indicator")

    statement = delete(indicators_dict).where(indicators_dict.c.indicator_id == indicator_id)
    await conn.execute(statement)
    await conn.commit()

    return {"status": "ok"}


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
            indicators_dict.c.parent_id,
            indicators_dict.c.name_full,
            indicators_dict.c.level,
            indicators_dict.c.list_label,
            measurement_units_dict.c.measurement_unit_id,
            measurement_units_dict.c.name.label("measurement_unit_name"),
            territories_data.c.name.label("territory_name"),
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
            .join(territories_data, territories_data.c.territory_id == territory_indicators_data.c.territory_id)
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


async def get_indicator_values_by_id_from_db(
    conn: AsyncConnection,
    indicator_id: int,
    territory_id: int | None,
    date_type: str | None,
    date_value: datetime | None,
    value_type: str | None,
    information_source: str | None,
) -> list[IndicatorValueDTO]:
    """Get indicator values objects by indicator id."""

    if not await check_existence(conn, indicators_dict, conditions={"indicator_id": indicator_id}):
        raise EntityNotFoundById(indicator_id, "indicator")

    statement = (
        select(
            territory_indicators_data,
            indicators_dict.c.parent_id,
            indicators_dict.c.name_full,
            indicators_dict.c.level,
            indicators_dict.c.list_label,
            measurement_units_dict.c.measurement_unit_id,
            measurement_units_dict.c.name.label("measurement_unit_name"),
            territories_data.c.name.label("territory_name"),
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
            .join(territories_data, territories_data.c.territory_id == territory_indicators_data.c.territory_id)
        )
        .where(territory_indicators_data.c.indicator_id == indicator_id)
    )

    if territory_id is not None:
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


async def add_indicator_value_to_db(
    conn: AsyncConnection,
    indicator_value: IndicatorValuePost,
) -> IndicatorValueDTO:
    """Create indicator value object."""

    if not await check_existence(conn, indicators_dict, conditions={"indicator_id": indicator_value.indicator_id}):
        raise EntityNotFoundById(indicator_value.indicator_id, "indicator")

    if not await check_existence(conn, territories_data, conditions={"territory_id": indicator_value.territory_id}):
        raise EntityNotFoundById(indicator_value.territory_id, "territory")

    if await check_existence(
        conn,
        territory_indicators_data,
        conditions={
            "indicator_id": indicator_value.indicator_id,
            "territory_id": indicator_value.territory_id,
            "date_type": indicator_value.date_type,
            "date_value": func.date(indicator_value.date_value),
            "value_type": indicator_value.value_type,
            "information_source": indicator_value.information_source,
        },
    ):
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
        insert(territory_indicators_data).values(**indicator_value.model_dump()).returning(territory_indicators_data)
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


async def put_indicator_value_to_db(conn: AsyncConnection, indicator_value: IndicatorValuePut) -> IndicatorValueDTO:
    """Update existing indicator value or create new if it doesn't exist."""

    if not await check_existence(conn, indicators_dict, conditions={"indicator_id": indicator_value.indicator_id}):
        raise EntityNotFoundById(indicator_value.indicator_id, "indicator")

    if not await check_existence(conn, territories_data, conditions={"territory_id": indicator_value.territory_id}):
        raise EntityNotFoundById(indicator_value.territory_id, "territory")

    if await check_existence(
        conn,
        territory_indicators_data,
        conditions={
            "indicator_id": indicator_value.indicator_id,
            "territory_id": indicator_value.territory_id,
            "date_type": indicator_value.date_type,
            "date_value": indicator_value.date_value,
            "value_type": indicator_value.value_type,
            "information_source": indicator_value.information_source,
        },
    ):
        statement = (
            update(territory_indicators_data)
            .values(value=indicator_value.value, updated_at=datetime.now(timezone.utc))
            .where(
                territory_indicators_data.c.indicator_id == indicator_value.indicator_id,
                territory_indicators_data.c.territory_id == indicator_value.territory_id,
                territory_indicators_data.c.date_type == indicator_value.date_type,
                territory_indicators_data.c.date_value == indicator_value.date_value,
                territory_indicators_data.c.value_type == indicator_value.value_type,
                territory_indicators_data.c.information_source == indicator_value.information_source,
            )
        )
    else:
        statement = insert(territory_indicators_data).values(**indicator_value.model_dump())

    await conn.execute(statement)
    await conn.commit()

    return await get_indicator_value_by_id_from_db(
        conn,
        indicator_value.indicator_id,
        indicator_value.territory_id,
        indicator_value.date_type,
        indicator_value.date_value,
        indicator_value.value_type,
        indicator_value.information_source,
    )


async def delete_indicator_value_from_db(
    conn: AsyncConnection,
    indicator_id: int,
    territory_id: int,
    date_type: str,
    date_value: datetime,
    value_type: str,
    information_source: str,
) -> dict:
    """Delete indicator value object by id."""

    if not await check_existence(
        conn,
        territory_indicators_data,
        conditions={
            "indicator_id": indicator_id,
            "territory_id": territory_id,
            "date_type": date_type,
            "date_value": date_value,
            "value_type": value_type,
            "information_source": information_source,
        },
    ):
        raise EntityNotFoundByParams(
            "indicator value",
            indicator_id,
            territory_id,
            date_type,
            date_value,
            value_type,
            information_source,
        )

    statement = delete(territory_indicators_data).where(
        territory_indicators_data.c.indicator_id == indicator_id,
        territory_indicators_data.c.territory_id == territory_id,
        territory_indicators_data.c.date_type == date_type,
        territory_indicators_data.c.date_value == date_value,
        territory_indicators_data.c.value_type == value_type,
        territory_indicators_data.c.information_source == information_source,
    )
    await conn.execute(statement)
    await conn.commit()

    return {"status": "ok"}
