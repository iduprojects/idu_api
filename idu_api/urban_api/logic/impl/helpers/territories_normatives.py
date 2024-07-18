from datetime import datetime, timezone
from typing import Callable, Optional

from geoalchemy2.functions import ST_AsGeoJSON
from sqlalchemy import and_, case, cast, delete, func, insert, literal, select, update
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.common.db.entities import (
    service_types_dict,
    service_types_normatives_data,
    territories_data,
    urban_functions_dict,
)
from idu_api.urban_api.dto import NormativeDTO, TerritoryWithNormativesDTO
from idu_api.urban_api.exceptions.logic.common import EntityAlreadyExists, EntityNotFoundById, EntityNotFoundByParams
from idu_api.urban_api.schemas import NormativeDelete, NormativePatch, NormativePost

func: Callable


async def get_normatives_by_territory_id_from_db(conn: AsyncConnection, territory_id: int) -> list[NormativeDTO]:
    """Get a list of normatives for given territory."""
    result = []

    statement = select(
        territories_data.c.territory_id,
        territories_data.c.parent_id,
        territories_data.c.level,
    ).where(territories_data.c.territory_id == territory_id)
    territory = (await conn.execute(statement)).one_or_none()
    if territory is None:
        raise EntityNotFoundById(territory_id, "territory")

    # Recursive request to obtain a "branch" of territories
    cte_statement = statement.cte(name="territories_recursive", recursive=True)
    recursive_part = (
        select(
            territories_data.c.territory_id,
            territories_data.c.parent_id,
            territories_data.c.level,
        )
        .join(cte_statement, territories_data.c.territory_id == cte_statement.c.parent_id)
        .where(territories_data.c.territory_id != cte_statement.c.territory_id)
    )
    cte_statement = cte_statement.union_all(recursive_part)

    # Subquery to determine the territory of the service type (using level)
    subquery_service_types = (
        select(service_types_normatives_data.c.service_type_id, func.max(cte_statement.c.level).label("max_level"))
        .join(cte_statement, service_types_normatives_data.c.territory_id == cte_statement.c.territory_id)
        .group_by(service_types_normatives_data.c.service_type_id)
    ).subquery()

    # Main query to get list of normative with service types
    statement = (
        select(
            service_types_normatives_data.c.service_type_id,
            service_types_dict.c.name.label("service_type_name"),
            service_types_normatives_data.c.urban_function_id,
            urban_functions_dict.c.name.label("urban_function_name"),
            service_types_normatives_data.c.is_regulated,
            service_types_normatives_data.c.radius_availability_meters,
            service_types_normatives_data.c.time_availability_minutes,
            service_types_normatives_data.c.services_per_1000_normative,
            service_types_normatives_data.c.services_capacity_per_1000_normative,
            case((cte_statement.c.territory_id == territory_id, "self"), else_="parent").label("normative_type"),
            service_types_normatives_data.c.year,
            service_types_normatives_data.c.source,
            service_types_normatives_data.c.created_at,
            service_types_normatives_data.c.updated_at,
        )
        .join(cte_statement, service_types_normatives_data.c.territory_id == cte_statement.c.territory_id)
        .join(
            subquery_service_types,
            (service_types_normatives_data.c.service_type_id == subquery_service_types.c.service_type_id)
            & (cte_statement.c.level == subquery_service_types.c.max_level),
        )
        .outerjoin(
            service_types_dict, service_types_dict.c.service_type_id == service_types_normatives_data.c.service_type_id
        )
        .outerjoin(
            urban_functions_dict,
            urban_functions_dict.c.urban_function_id == service_types_normatives_data.c.urban_function_id,
        )
    )
    result.extend(list((await conn.execute(statement)).mappings().all()))

    # Subquery to determine the territory of the urban function (using level)
    subquery_urban_functions = (
        select(service_types_normatives_data.c.urban_function_id, func.max(cte_statement.c.level).label("max_level"))
        .join(cte_statement, service_types_normatives_data.c.territory_id == cte_statement.c.territory_id)
        .group_by(service_types_normatives_data.c.urban_function_id)
    ).subquery()

    # Main query to get list of normative with urban functions
    statement = (
        select(
            service_types_normatives_data.c.service_type_id,
            service_types_dict.c.name.label("service_type_name"),
            service_types_normatives_data.c.urban_function_id,
            urban_functions_dict.c.name.label("urban_function_name"),
            service_types_normatives_data.c.is_regulated,
            service_types_normatives_data.c.radius_availability_meters,
            service_types_normatives_data.c.time_availability_minutes,
            service_types_normatives_data.c.services_per_1000_normative,
            service_types_normatives_data.c.services_capacity_per_1000_normative,
            case((cte_statement.c.territory_id == territory_id, "self"), else_="parent").label("normative_type"),
            service_types_normatives_data.c.year,
            service_types_normatives_data.c.source,
            service_types_normatives_data.c.created_at,
            service_types_normatives_data.c.updated_at,
        )
        .join(cte_statement, service_types_normatives_data.c.territory_id == cte_statement.c.territory_id)
        .join(
            subquery_urban_functions,
            (service_types_normatives_data.c.urban_function_id == subquery_urban_functions.c.urban_function_id)
            & (cte_statement.c.level == subquery_urban_functions.c.max_level),
        )
        .outerjoin(
            service_types_dict, service_types_dict.c.service_type_id == service_types_normatives_data.c.service_type_id
        )
        .outerjoin(
            urban_functions_dict,
            urban_functions_dict.c.urban_function_id == service_types_normatives_data.c.urban_function_id,
        )
    )
    result.extend(list((await conn.execute(statement)).mappings().all()))

    service_type_ids = [
        normative["service_type_id"] for normative in result if normative["service_type_id"] is not None
    ]
    urban_function_ids = [
        normative["urban_function_id"] for normative in result if normative["urban_function_id"] is not None
    ]

    # Query to get list of normative for high-level territories (except those that have already been taken)
    statement = (
        select(
            service_types_normatives_data.c.service_type_id,
            service_types_dict.c.name.label("service_type_name"),
            service_types_normatives_data.c.urban_function_id,
            urban_functions_dict.c.name.label("urban_function_name"),
            service_types_normatives_data.c.is_regulated,
            service_types_normatives_data.c.radius_availability_meters,
            service_types_normatives_data.c.time_availability_minutes,
            service_types_normatives_data.c.services_per_1000_normative,
            service_types_normatives_data.c.services_capacity_per_1000_normative,
            literal("global").label("normative_type"),
            service_types_normatives_data.c.year,
            service_types_normatives_data.c.source,
            service_types_normatives_data.c.created_at,
            service_types_normatives_data.c.updated_at,
        )
        .outerjoin(
            service_types_dict, service_types_dict.c.service_type_id == service_types_normatives_data.c.service_type_id
        )
        .outerjoin(
            urban_functions_dict,
            urban_functions_dict.c.urban_function_id == service_types_normatives_data.c.urban_function_id,
        )
        .where(service_types_normatives_data.c.territory_id.is_(None))
    )
    high_level_normative = list(
        filter(
            lambda x: x["service_type_id"] not in service_type_ids and x["urban_function_id"] not in urban_function_ids,
            list((await conn.execute(statement)).mappings().all()),
        )
    )
    result.extend(high_level_normative)

    return [NormativeDTO(**normative) for normative in result]


async def get_normatives_by_ids_from_db(conn: AsyncConnection, normative_ids: list[int]) -> list[NormativeDTO]:
    statement = (
        select(
            service_types_normatives_data.c.service_type_id,
            service_types_dict.c.name.label("service_type_name"),
            service_types_normatives_data.c.urban_function_id,
            urban_functions_dict.c.name.label("urban_function_name"),
            service_types_normatives_data.c.is_regulated,
            service_types_normatives_data.c.radius_availability_meters,
            service_types_normatives_data.c.time_availability_minutes,
            service_types_normatives_data.c.services_per_1000_normative,
            service_types_normatives_data.c.services_capacity_per_1000_normative,
            literal("self").label("normative_type"),
            service_types_normatives_data.c.year,
            service_types_normatives_data.c.source,
            service_types_normatives_data.c.created_at,
            service_types_normatives_data.c.updated_at,
        )
        .outerjoin(
            service_types_dict, service_types_dict.c.service_type_id == service_types_normatives_data.c.service_type_id
        )
        .outerjoin(
            urban_functions_dict,
            urban_functions_dict.c.urban_function_id == service_types_normatives_data.c.urban_function_id,
        )
        .where(service_types_normatives_data.c.normative_id.in_(normative_ids))
    )

    result = (await conn.execute(statement)).mappings().all()

    return [NormativeDTO(**normative) for normative in result]


async def add_normatives_to_territory_to_db(
    conn: AsyncConnection, territory_id: int, normatives: list[NormativePost]
) -> list[NormativeDTO]:
    """
    Create normative object
    """

    statement = select(territories_data).where(territories_data.c.territory_id == territory_id)
    territory = (await conn.execute(statement)).one_or_none()
    if territory is None:
        raise EntityNotFoundById(territory_id, "territory")

    for normative in normatives:
        if normative.service_type_id is not None:
            statement = select(service_types_dict).where(
                service_types_dict.c.service_type_id == normative.service_type_id
            )
            service_type = (await conn.execute(statement)).one_or_none()
            if service_type is None:
                raise EntityNotFoundById(normative.service_type_id, "service type")

            statement = select(service_types_normatives_data).where(
                service_types_normatives_data.c.service_type_id == normative.service_type_id,
                service_types_normatives_data.c.territory_id == territory_id,
                service_types_normatives_data.c.year == normative.year,
            )
            requested_normative = (await conn.execute(statement)).one_or_none()
            if requested_normative is not None:
                raise EntityAlreadyExists("normative", normative.service_type_id, territory_id, normative.year)

        if normative.urban_function_id is not None:
            statement = select(urban_functions_dict).where(
                urban_functions_dict.c.urban_function_id == normative.urban_function_id
            )
            urban_function = (await conn.execute(statement)).one_or_none()
            if urban_function is None:
                raise EntityNotFoundById(normative.urban_function_id, "urban function")

            statement = select(service_types_normatives_data).where(
                service_types_normatives_data.c.urban_function_id == normative.urban_function_id,
                service_types_normatives_data.c.territory_id == territory_id,
                service_types_normatives_data.c.year == normative.year,
            )
            requested_normative = (await conn.execute(statement)).one_or_none()
            if requested_normative is not None:
                raise EntityAlreadyExists("normative", normative.urban_function_id, territory_id, normative.year)

    normative_ids = []
    for normative in normatives:
        statement = (
            insert(service_types_normatives_data)
            .values(
                service_type_id=normative.service_type_id,
                urban_function_id=normative.urban_function_id,
                territory_id=territory_id,
                is_regulated=normative.is_regulated,
                radius_availability_meters=normative.radius_availability_meters,
                time_availability_minutes=normative.time_availability_minutes,
                services_per_1000_normative=normative.services_per_1000_normative,
                services_capacity_per_1000_normative=normative.services_capacity_per_1000_normative,
                year=normative.year,
                source=normative.source,
            )
            .returning(service_types_normatives_data.c.normative_id)
        )
        normative_ids.append((await conn.execute(statement)).scalar_one())

    await conn.commit()

    return await get_normatives_by_ids_from_db(conn, normative_ids)


async def put_normatives_by_territory_id_in_db(
    conn: AsyncConnection, territory_id: int, normatives: list[NormativePost]
) -> list[NormativeDTO]:

    statement = select(territories_data).where(territories_data.c.territory_id == territory_id)
    territory = (await conn.execute(statement)).one_or_none()
    if territory is None:
        raise EntityNotFoundById(territory_id, "territory")

    for normative in normatives:
        if normative.service_type_id is not None:
            statement = select(service_types_dict).where(
                service_types_dict.c.service_type_id == normative.service_type_id
            )
            service_type = (await conn.execute(statement)).one_or_none()
            if service_type is None:
                raise EntityNotFoundById(normative.service_type_id, "service type")

            statement = select(service_types_normatives_data).where(
                service_types_normatives_data.c.service_type_id == normative.service_type_id,
                service_types_normatives_data.c.territory_id == territory_id,
                service_types_normatives_data.c.year == normative.year,
            )
            requested_normative = (await conn.execute(statement)).one_or_none()
            if requested_normative is None:
                raise EntityNotFoundByParams("normative", normative.service_type_id, territory_id, normative.year)

        if normative.urban_function_id is not None:
            statement = select(urban_functions_dict).where(
                urban_functions_dict.c.urban_function_id == normative.urban_function_id
            )
            urban_function = (await conn.execute(statement)).one_or_none()
            if urban_function is None:
                raise EntityNotFoundById(normative.urban_function_id, "urban function")

            statement = select(service_types_normatives_data).where(
                service_types_normatives_data.c.urban_function_id == normative.urban_function_id,
                service_types_normatives_data.c.territory_id == territory_id,
                service_types_normatives_data.c.year == normative.year,
            )
            requested_normative = (await conn.execute(statement)).one_or_none()
            if requested_normative is None:
                raise EntityNotFoundByParams("normative", normative.urban_function_id, territory_id, normative.year)

    normative_ids = []
    for normative in normatives:
        if normative.service_type_id is not None:
            where_clause = and_(
                service_types_normatives_data.c.service_type_id == normative.service_type_id,
                service_types_normatives_data.c.territory_id == territory_id,
                service_types_normatives_data.c.year == normative.year,
            )
        else:
            where_clause = and_(
                service_types_normatives_data.c.urban_function_id == normative.urban_function_id,
                service_types_normatives_data.c.territory_id == territory_id,
                service_types_normatives_data.c.year == normative.year,
            )

        statement = (
            update(service_types_normatives_data)
            .where(where_clause)
            .values(
                service_type_id=normative.service_type_id,
                urban_function_id=normative.urban_function_id,
                territory_id=territory_id,
                is_regulated=normative.is_regulated,
                radius_availability_meters=normative.radius_availability_meters,
                time_availability_minutes=normative.time_availability_minutes,
                services_per_1000_normative=normative.services_per_1000_normative,
                services_capacity_per_1000_normative=normative.services_capacity_per_1000_normative,
                year=normative.year,
                source=normative.source,
                updated_at=datetime.now(timezone.utc),
            )
            .returning(service_types_normatives_data.c.normative_id)
        )
        normative_ids.append((await conn.execute(statement)).scalar_one())

    await conn.commit()

    return await get_normatives_by_ids_from_db(conn, normative_ids)


async def patch_normatives_by_territory_id_in_db(
    conn: AsyncConnection, territory_id: int, normatives: list[NormativePatch]
) -> list[NormativeDTO]:
    statement = select(territories_data).where(territories_data.c.territory_id == territory_id)
    territory = (await conn.execute(statement)).one_or_none()
    if territory is None:
        raise EntityNotFoundById(territory_id, "territory")

    for normative in normatives:
        if normative.service_type_id is not None:
            statement = select(service_types_dict).where(
                service_types_dict.c.service_type_id == normative.service_type_id
            )
            service_type = (await conn.execute(statement)).one_or_none()
            if service_type is None:
                raise EntityNotFoundById(normative.service_type_id, "service type")

            statement = select(service_types_normatives_data).where(
                service_types_normatives_data.c.service_type_id == normative.service_type_id,
                service_types_normatives_data.c.territory_id == territory_id,
                service_types_normatives_data.c.year == normative.year,
            )
            requested_normative = (await conn.execute(statement)).one_or_none()
            if requested_normative is None:
                raise EntityNotFoundByParams("normative", normative.service_type_id, territory_id, normative.year)

        if normative.urban_function_id is not None:
            statement = select(urban_functions_dict).where(
                urban_functions_dict.c.urban_function_id == normative.urban_function_id
            )
            urban_function = (await conn.execute(statement)).one_or_none()
            if urban_function is None:
                raise EntityNotFoundById(normative.urban_function_id, "urban function")

            statement = select(service_types_normatives_data).where(
                service_types_normatives_data.c.urban_function_id == normative.urban_function_id,
                service_types_normatives_data.c.territory_id == territory_id,
                service_types_normatives_data.c.year == normative.year,
            )
            requested_normative = (await conn.execute(statement)).one_or_none()
            if requested_normative is None:
                raise EntityNotFoundByParams("normative", normative.urban_function_id, territory_id, normative.year)

    normative_ids = []
    for normative in normatives:
        if normative.service_type_id is not None:
            where_clause = and_(
                service_types_normatives_data.c.service_type_id == normative.service_type_id,
                service_types_normatives_data.c.territory_id == territory_id,
                service_types_normatives_data.c.year == normative.year,
            )
        else:
            where_clause = and_(
                service_types_normatives_data.c.urban_function_id == normative.urban_function_id,
                service_types_normatives_data.c.territory_id == territory_id,
                service_types_normatives_data.c.year == normative.year,
            )

        statement = (
            update(service_types_normatives_data)
            .where(where_clause)
            .values(updated_at=datetime.now(timezone.utc))
            .returning(service_types_normatives_data.c.normative_id)
        )

        values_to_update = {}
        for k, v in normative.model_dump().items():
            if v is not None:
                values_to_update.update({k: v})
        statement = statement.values(**values_to_update)

        normative_ids.append((await conn.execute(statement)).scalar_one())

    await conn.commit()

    return await get_normatives_by_ids_from_db(conn, normative_ids)


async def delete_normatives_by_territory_id_in_db(
    conn: AsyncConnection, territory_id: int, normatives: list[NormativeDelete]
) -> dict:
    statement = select(territories_data).where(territories_data.c.territory_id == territory_id)
    territory = (await conn.execute(statement)).one_or_none()
    if territory is None:
        raise EntityNotFoundById(territory_id, "territory")

    for normative in normatives:
        if normative.service_type_id is not None:
            statement = select(service_types_dict).where(
                service_types_dict.c.service_type_id == normative.service_type_id
            )
            service_type = (await conn.execute(statement)).one_or_none()
            if service_type is None:
                raise EntityNotFoundById(normative.service_type_id, "service type")

            statement = select(service_types_normatives_data).where(
                service_types_normatives_data.c.service_type_id == normative.service_type_id,
                service_types_normatives_data.c.territory_id == territory_id,
                service_types_normatives_data.c.year == normative.year,
            )
            requested_normative = (await conn.execute(statement)).one_or_none()
            if requested_normative is None:
                raise EntityNotFoundByParams("normative", normative.service_type_id, territory_id, normative.year)

        if normative.urban_function_id is not None:
            statement = select(urban_functions_dict).where(
                urban_functions_dict.c.urban_function_id == normative.urban_function_id
            )
            urban_function = (await conn.execute(statement)).one_or_none()
            if urban_function is None:
                raise EntityNotFoundById(normative.urban_function_id, "urban function")

            statement = select(service_types_normatives_data).where(
                service_types_normatives_data.c.urban_function_id == normative.urban_function_id,
                service_types_normatives_data.c.territory_id == territory_id,
                service_types_normatives_data.c.year == normative.year,
            )
            requested_normative = (await conn.execute(statement)).one_or_none()
            if requested_normative is None:
                raise EntityNotFoundByParams("normative", normative.urban_function_id, territory_id, normative.year)

    for normative in normatives:
        if normative.service_type_id is not None:
            where_clause = and_(
                service_types_normatives_data.c.service_type_id == normative.service_type_id,
                service_types_normatives_data.c.territory_id == territory_id,
                service_types_normatives_data.c.year == normative.year,
            )
        else:
            where_clause = and_(
                service_types_normatives_data.c.urban_function_id == normative.urban_function_id,
                service_types_normatives_data.c.territory_id == territory_id,
                service_types_normatives_data.c.year == normative.year,
            )
        statement = select(service_types_normatives_data).where(where_clause)
        requested_normative = (await conn.execute(statement)).mappings().one_or_none()
        if requested_normative is None:
            raise EntityNotFoundByParams(
                "normative",
                territory_id,
                normative.service_type_id if normative.service_type_id is not None else normative.urban_function_id,
            )

        statement = delete(service_types_normatives_data).where(where_clause)
        await conn.execute(statement)

    await conn.commit()

    return {"result": "ok"}


async def get_normatives_values_by_parent_id_from_db(
    conn: AsyncConnection, parent_id: Optional[int], service_type_id: Optional[int], urban_function_id: Optional[int]
) -> list[TerritoryWithNormativesDTO]:
    """Get list of normatives with values for territory by parent id and service type|urban function id."""

    if parent_id is not None:
        statement = select(territories_data).where(territories_data.c.territory_id == parent_id)
        territory = (await conn.execute(statement)).one_or_none()
        if territory is None:
            raise EntityNotFoundById(parent_id, "territory")

    if service_type_id is not None:
        statement = select(service_types_dict).where(service_types_dict.c.service_type_id == service_type_id)
        service_type = (await conn.execute(statement)).one_or_none()
        if service_type is None:
            raise EntityNotFoundById(service_type_id, "service_type")

    if urban_function_id is not None:
        statement = select(urban_functions_dict).where(urban_functions_dict.c.urban_function_id == urban_function_id)
        urban_function = (await conn.execute(statement)).one_or_none()
        if urban_function is None:
            raise EntityNotFoundById(urban_function_id, "urban function")

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
        normatives = await get_normatives_by_territory_id_from_db(conn, child_territory.territory_id)
        results.append(TerritoryWithNormativesDTO(**child_territory, normatives=normatives))

    return results
