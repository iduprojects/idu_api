"""Social groups internal logic is defined here."""

from collections.abc import Callable
from typing import Literal

from sqlalchemy import delete, func, insert, select, update
from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.common.db.entities import (
    service_types_dict,
    soc_group_values_data,
    soc_groups_dict,
    soc_value_indicators_data,
    soc_values_dict,
    soc_values_service_types_dict,
    territories_data,
    urban_functions_dict,
)
from idu_api.urban_api.dto import (
    ServiceTypeDTO,
    SocGroupDTO,
    SocGroupWithServiceTypesDTO,
    SocValueDTO,
    SocValueIndicatorValueDTO,
    SocValueWithServiceTypesDTO,
)
from idu_api.urban_api.exceptions.logic.common import EntityAlreadyExists, EntityNotFoundById, EntityNotFoundByParams
from idu_api.urban_api.logic.impl.helpers.utils import check_existence, extract_values_from_model
from idu_api.urban_api.schemas import (
    SocGroupPost,
    SocServiceTypePost,
    SocValueIndicatorValuePost,
    SocValueIndicatorValuePut,
    SocValuePost,
)

func: Callable


async def get_social_groups_from_db(conn: AsyncConnection) -> list[SocGroupDTO]:
    """Get a list of all social groups."""

    statement = select(soc_groups_dict).order_by(soc_groups_dict.c.soc_group_id)

    return [SocGroupDTO(**group) for group in (await conn.execute(statement)).mappings().all()]


async def get_social_group_by_id_from_db(conn: AsyncConnection, soc_group_id: int) -> SocGroupWithServiceTypesDTO:
    """Get social group with associated service types by identifier."""

    statement = (
        select(
            soc_groups_dict,
            service_types_dict.c.service_type_id.label("id"),
            service_types_dict.c.name.label("service_type_name"),
            soc_group_values_data.c.infrastructure_type,
        )
        .select_from(
            soc_groups_dict.outerjoin(
                soc_group_values_data,
                soc_group_values_data.c.soc_group_id == soc_groups_dict.c.soc_group_id,
            ).outerjoin(
                service_types_dict, service_types_dict.c.service_type_id == soc_group_values_data.c.service_type_id
            )
        )
        .where(soc_groups_dict.c.soc_group_id == soc_group_id)
        .order_by(service_types_dict.c.service_type_id)
        .distinct()
    )

    result = (await conn.execute(statement)).mappings().all()
    if not result:
        raise EntityNotFoundById(soc_group_id, "social group")

    service_types = [
        {"id": row["id"], "name": row["service_type_name"], "infrastructure_type": row["infrastructure_type"]}
        for row in result
        if row["id"] is not None
    ]

    return SocGroupWithServiceTypesDTO(
        soc_group_id=result[0]["soc_group_id"], name=result[0]["name"], service_types=service_types
    )


async def add_social_group_to_db(conn: AsyncConnection, soc_group: SocGroupPost) -> SocGroupWithServiceTypesDTO:
    """Create a new social group."""

    if await check_existence(conn, soc_groups_dict, conditions={"name": soc_group.name}):
        raise EntityAlreadyExists("social group", soc_group.name)

    statement = (
        insert(soc_groups_dict).values(**extract_values_from_model(soc_group)).returning(soc_groups_dict.c.soc_group_id)
    )
    soc_group_id = (await conn.execute(statement)).scalar_one()
    await conn.commit()

    return await get_social_group_by_id_from_db(conn, soc_group_id)


async def add_service_type_to_social_value_to_db(
    conn: AsyncConnection, soc_value_id: int, service_type_id: int
) -> SocValueWithServiceTypesDTO:
    """Add service type to social group"""

    if not await check_existence(conn, soc_values_dict, conditions={"soc_value_id": soc_value_id}):
        raise EntityNotFoundById(soc_value_id, "social value")

    if not await check_existence(conn, service_types_dict, conditions={"service_type_id": service_type_id}):
        raise EntityNotFoundById(service_type_id, "service type")

    if await check_existence(
        conn,
        soc_values_service_types_dict,
        conditions={"service_type_id": service_type_id, "soc_value_id": soc_value_id},
    ):
        raise EntityAlreadyExists("service type connection with soc value", service_type_id, soc_value_id)

    statement = (
        insert(soc_values_service_types_dict)
        .values(soc_value_id=soc_value_id, service_type_id=service_type_id)
        .returning(soc_values_service_types_dict.c.soc_value_id)
    )
    await conn.execute(statement)
    await conn.commit()

    return await get_social_value_with_service_types_by_id_from_db(conn, soc_value_id)


async def add_service_type_to_social_group_to_db(
    conn: AsyncConnection,
    soc_group_id: int,
    service_type: SocServiceTypePost,
) -> SocGroupWithServiceTypesDTO:
    """Add service type to social group."""

    if not await check_existence(conn, soc_groups_dict, conditions={"soc_group_id": soc_group_id}):
        raise EntityNotFoundById(soc_group_id, "social group")

    if not await check_existence(
        conn, service_types_dict, conditions={"service_type_id": service_type.service_type_id}
    ):
        raise EntityNotFoundById(service_type.service_type_id, "service type")

    if await check_existence(
        conn,
        soc_group_values_data,
        conditions={"service_type_id": service_type.service_type_id, "soc_group_id": soc_group_id},
    ):
        raise EntityAlreadyExists("service type", service_type.service_type_id, soc_group_id)

    statement = (
        insert(soc_group_values_data)
        .values(soc_group_id=soc_group_id, **extract_values_from_model(service_type))
        .returning(soc_group_values_data.c.soc_group_id)
    )
    await conn.execute(statement)
    await conn.commit()

    return await get_social_group_by_id_from_db(conn, soc_group_id)


async def delete_social_group_from_db(conn: AsyncConnection, soc_group_id: int) -> dict[str, str]:
    """Delete social group by identifier."""

    if not await check_existence(conn, soc_groups_dict, conditions={"soc_group_id": soc_group_id}):
        raise EntityNotFoundById(soc_group_id, "social group")

    statement = delete(soc_groups_dict).where(soc_groups_dict.c.soc_group_id == soc_group_id)
    await conn.execute(statement)
    await conn.commit()

    return {"status": "ok"}


async def get_social_values_from_db(conn: AsyncConnection) -> list[SocValueDTO]:
    """Get a list of all social values."""

    statement = select(soc_values_dict).order_by(soc_values_dict.c.soc_value_id)

    return [SocValueDTO(**value) for value in (await conn.execute(statement)).mappings().all()]


async def get_social_value_by_id_from_db(conn: AsyncConnection, soc_value_id: int) -> SocValueDTO:
    """Get social value by identifier."""
    if not await check_existence(conn, soc_values_dict, conditions={"soc_value_id": soc_value_id}):
        raise EntityNotFoundById(soc_value_id, "social value")

    statement = select(soc_values_dict).where(soc_values_dict.c.soc_value_id == soc_value_id)

    soc_value = (await conn.execute(statement)).mappings().one_or_none()

    return SocValueDTO(**soc_value)


async def get_social_value_with_service_types_by_id_from_db(
    conn: AsyncConnection, soc_value_id: int
) -> SocValueWithServiceTypesDTO:
    """Get social value with associated service types by identifier."""
    if not await check_existence(conn, soc_values_dict, conditions={"soc_value_id": soc_value_id}):
        raise EntityNotFoundById(soc_value_id, "social value")

    select_from = (
        soc_values_dict.join(
            soc_values_service_types_dict,
            soc_values_service_types_dict.c.soc_value_id == soc_values_dict.c.soc_value_id,
        )
        .join(
            service_types_dict,
            service_types_dict.c.service_type_id == soc_values_service_types_dict.c.service_type_id,
        )
        .join(urban_functions_dict, urban_functions_dict.c.urban_function_id == service_types_dict.c.urban_function_id)
    )

    statement = (
        select(
            soc_values_dict.c.soc_value_id,
            soc_values_dict.c.name.label("soc_value_name"),
            soc_values_dict.c.rank,
            soc_values_dict.c.decree_value,
            soc_values_dict.c.normative_value,
            service_types_dict,
            urban_functions_dict.c.name.label("urban_function_name"),
        )
        .select_from(select_from)
        .where(soc_values_dict.c.soc_value_id == soc_value_id)
        .distinct()
    )

    result = (await conn.execute(statement)).mappings().all()

    service_types = [ServiceTypeDTO(**{field: row[field] for field in ServiceTypeDTO.fields()}) for row in result]

    return SocValueWithServiceTypesDTO(
        soc_value_id=result[0]["soc_value_id"],
        name=result[0]["name"],
        rank=result[0]["rank"],
        decree_value=result[0]["decree_value"],
        normative_value=result[0]["normative_value"],
        service_types=service_types,
    )


async def add_social_value_to_db(conn: AsyncConnection, soc_value: SocValuePost) -> SocValueDTO:
    """Create a new social value."""

    if await check_existence(conn, soc_values_dict, conditions={"name": soc_value.name}):
        raise EntityAlreadyExists("social value", soc_value.name)

    statement = (
        insert(soc_values_dict).values(**extract_values_from_model(soc_value)).returning(soc_values_dict.c.soc_value_id)
    )
    soc_value_id = (await conn.execute(statement)).scalar_one()
    await conn.commit()

    return await get_social_value_by_id_from_db(conn, soc_value_id)


async def add_value_to_social_group_from_db(
    conn: AsyncConnection,
    soc_group_id: int,
    service_type_id: int,
    soc_value_id: int,
) -> SocValueDTO:
    """Add value to social group."""

    if not await check_existence(conn, soc_groups_dict, conditions={"soc_group_id": soc_group_id}):
        raise EntityNotFoundById(soc_group_id, "social group")

    if not await check_existence(conn, soc_values_dict, conditions={"soc_value_id": soc_value_id}):
        raise EntityNotFoundById(soc_value_id, "social value")

    if not await check_existence(conn, service_types_dict, conditions={"service_type_id": service_type_id}):
        raise EntityNotFoundById(service_type_id, "service type")

    statement = select(soc_group_values_data).where(
        soc_group_values_data.c.soc_group_id == soc_group_id,
        soc_group_values_data.c.service_type_id == service_type_id,
    )
    soc_group_values = (await conn.execute(statement)).mappings().all()
    if not soc_group_values:
        raise EntityNotFoundByParams("social group value", soc_group_id, service_type_id)

    flag = False
    for group_value in soc_group_values:
        if group_value.soc_value_id is None:
            statement = (
                update(soc_group_values_data)
                .where(soc_group_values_data.c.soc_group_value_id == group_value.soc_group_value_id)
                .values(soc_value_id=soc_value_id)
            )
            flag = True
        if group_value.soc_value_id == soc_value_id:
            raise EntityAlreadyExists("social group value", soc_group_id, service_type_id, soc_value_id)

    if not flag:
        statement = insert(soc_group_values_data).values(
            soc_group_id=soc_group_id,
            service_type_id=service_type_id,
            soc_value_id=soc_value_id,
            infrastructure_type=soc_group_values[0].infrastructure_type,
        )

    await conn.execute(statement)
    await conn.commit()

    return await get_social_value_by_id_from_db(conn, soc_value_id)


async def delete_social_value_from_db(conn: AsyncConnection, soc_value_id: int) -> dict[str, str]:
    """Delete social value by identifier."""

    if not await check_existence(conn, soc_values_dict, conditions={"soc_value_id": soc_value_id}):
        raise EntityNotFoundById(soc_value_id, "social value")

    statement = delete(soc_values_dict).where(soc_values_dict.c.soc_value_id == soc_value_id)
    await conn.execute(statement)
    await conn.commit()

    return {"status": "ok"}


async def get_social_value_indicator_values_from_db(
    conn: AsyncConnection,
    soc_value_id: int,
    territory_id: int | None,
    year: int | None,
    last_only: bool,
) -> list[SocValueIndicatorValueDTO]:
    """Get social value's indicator values by social group identifier."""

    if not await check_existence(conn, soc_values_dict, conditions={"soc_value_id": soc_value_id}):
        raise EntityNotFoundById(soc_value_id, "social value id")

    if territory_id is not None:
        if not await check_existence(conn, territories_data, conditions={"territory_id": territory_id}):
            raise EntityNotFoundById(territory_id, "territory")

    select_from = soc_value_indicators_data.join(
        soc_values_dict,
        soc_values_dict.c.soc_value_id == soc_value_indicators_data.c.soc_value_id,
    ).join(territories_data, territories_data.c.territory_id == soc_value_indicators_data.c.territory_id)

    if last_only:
        subquery = (
            select(
                soc_value_indicators_data.c.soc_value_id,
                soc_value_indicators_data.c.territory_id,
                func.max(soc_value_indicators_data.c.year).label("max_date"),
            )
            .group_by(
                soc_value_indicators_data.c.soc_value_id,
                soc_value_indicators_data.c.territory_id,
            )
            .subquery()
        )

        select_from = select_from.join(
            subquery,
            (soc_value_indicators_data.c.soc_value_id == subquery.c.soc_value_id)
            & (soc_value_indicators_data.c.territory_id == subquery.c.territory_id)
            & (soc_value_indicators_data.c.year == subquery.c.max_date),
        )

    statement = (
        select(
            soc_value_indicators_data,
            soc_values_dict.c.name.label("soc_value_name"),
            territories_data.c.name.label("territory_name"),
        )
        .select_from(select_from)
        .where(soc_value_indicators_data.c.soc_value_id == soc_value_id)
    )

    if territory_id is not None:
        statement = statement.where(soc_value_indicators_data.c.territory_id == territory_id)
    if year is not None:
        statement = statement.where(soc_value_indicators_data.c.year == year)

    result = (await conn.execute(statement)).mappings().all()
    if not result:
        raise EntityNotFoundByParams("social value indicator value", soc_value_id, territory_id, year)

    return [SocValueIndicatorValueDTO(**indicator) for indicator in result]


async def add_social_value_indicator_value_to_db(
    conn: AsyncConnection,
    soc_group_indicator: SocValueIndicatorValuePost,
) -> SocValueIndicatorValueDTO:
    """Create a new social value indicator value."""

    if not await check_existence(conn, soc_values_dict, conditions={"soc_value_id": soc_group_indicator.soc_value_id}):
        raise EntityNotFoundById(soc_group_indicator.soc_value_id, "social value")

    if not await check_existence(conn, territories_data, conditions={"territory_id": soc_group_indicator.territory_id}):
        raise EntityNotFoundById(soc_group_indicator.territory_id, "territory")

    if await check_existence(
        conn,
        soc_value_indicators_data,
        conditions={
            "soc_value_id": soc_group_indicator.soc_value_id,
            "territory_id": soc_group_indicator.territory_id,
            "year": soc_group_indicator.year,
        },
    ):
        raise EntityAlreadyExists(
            "social value indicator value",
            soc_group_indicator.soc_value_id,
            soc_group_indicator.territory_id,
            soc_group_indicator.year,
        )

    statement = (
        insert(soc_value_indicators_data)
        .values(**soc_group_indicator.model_dump())
        .returning(soc_value_indicators_data)
    )

    result = (await conn.execute(statement)).mappings().one()
    await conn.commit()

    return (
        await get_social_value_indicator_values_from_db(
            conn,
            result.soc_value_id,
            result.territory_id,
            result.year,
            last_only=False,
        )
    )[0]


async def put_social_value_indicator_value_to_db(
    conn: AsyncConnection,
    soc_group_indicator: SocValueIndicatorValuePut,
) -> SocValueIndicatorValueDTO:
    """Update or create a social value indicator value."""

    if not await check_existence(conn, soc_values_dict, conditions={"soc_value_id": soc_group_indicator.soc_value_id}):
        raise EntityNotFoundById(soc_group_indicator.soc_value_id, "social value")

    if not await check_existence(conn, territories_data, conditions={"territory_id": soc_group_indicator.territory_id}):
        raise EntityNotFoundById(soc_group_indicator.territory_id, "territory")

    if await check_existence(
        conn,
        soc_value_indicators_data,
        conditions={
            "soc_value_id": soc_group_indicator.soc_value_id,
            "territory_id": soc_group_indicator.territory_id,
            "year": soc_group_indicator.year,
        },
    ):
        statement = (
            update(soc_value_indicators_data)
            .values(**extract_values_from_model(soc_group_indicator, to_update=True))
            .where(
                soc_value_indicators_data.c.soc_value_id == soc_group_indicator.soc_value_id,
                soc_value_indicators_data.c.territory_id == soc_group_indicator.territory_id,
                soc_value_indicators_data.c.year == soc_group_indicator.year,
            )
            .returning(soc_value_indicators_data)
        )

    else:
        statement = (
            insert(soc_value_indicators_data)
            .values(**soc_group_indicator.model_dump())
            .returning(soc_value_indicators_data)
        )

    result = (await conn.execute(statement)).mappings().one()
    await conn.commit()

    return (
        await get_social_value_indicator_values_from_db(
            conn,
            result.soc_value_id,
            result.territory_id,
            result.year,
            last_only=False,
        )
    )[0]


async def delete_social_value_indicator_value_from_db(
    conn: AsyncConnection,
    soc_value_id: int,
    territory_id: int | None,
    year: int | None,
) -> dict[str, str]:
    """Delete social value indicator value."""

    conditions = {
        "soc_value_id": soc_value_id,
    }

    if not await check_existence(conn, soc_values_dict, conditions={"soc_value_id": soc_value_id}):
        raise EntityNotFoundById(soc_value_id, "social value id")

    if territory_id is not None:
        conditions["territory_id"] = territory_id
        if not await check_existence(conn, territories_data, conditions={"territory_id": territory_id}):
            raise EntityNotFoundById(territory_id, "territory")

    if year is not None:
        conditions["year"] = year

    if not await check_existence(conn, soc_value_indicators_data, conditions=conditions):
        raise EntityNotFoundByParams("social value indicator value", soc_value_id, territory_id)

    statement = delete(soc_value_indicators_data).where(soc_value_indicators_data.c.soc_value_id == soc_value_id)

    if territory_id is not None:
        statement = statement.where(soc_value_indicators_data.c.territory_id == territory_id)
    if year is not None:
        statement = statement.where(
            soc_value_indicators_data.c.year == year,
        )

    await conn.execute(statement)
    await conn.commit()

    return {"status": "ok"}


async def get_service_types_by_social_value_id_from_db(
    conn: AsyncConnection, social_value_id: int, ordering: Literal["asc", "desc"] | None = None
) -> list[ServiceTypeDTO]:
    """Get all service type objects by social_value_id."""

    if not await check_existence(
        conn,
        soc_values_dict,
        conditions={
            "soc_value_id": social_value_id,
        },
    ):
        raise EntityNotFoundById(social_value_id, "social value")

    statement = (
        select(service_types_dict, urban_functions_dict.c.name.label("urban_function_name"))
        .select_from(
            soc_values_service_types_dict.join(
                service_types_dict,
                service_types_dict.c.service_type_id == soc_values_service_types_dict.c.service_type_id,
            ).join(
                urban_functions_dict, urban_functions_dict.c.urban_function_id == service_types_dict.c.urban_function_id
            )
        )
        .where(soc_values_service_types_dict.c.soc_value_id == social_value_id)
    )

    if ordering == "desc":
        statement = statement.order_by(service_types_dict.c.service_type_id.desc())
    else:
        statement = statement.order_by(service_types_dict.c.service_type_id)

    result = await conn.execute(statement)
    return [ServiceTypeDTO(**service_type) for service_type in result.mappings().all()]
