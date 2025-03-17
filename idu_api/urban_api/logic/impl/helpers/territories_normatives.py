"""Territories normatives internal logic is defined here."""

from collections import defaultdict
from collections.abc import Callable, Sequence

from geoalchemy2.functions import ST_AsEWKB
from sqlalchemy import RowMapping, and_, delete, func, insert, literal, select, update
from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.common.db.entities import (
    service_types_dict,
    service_types_normatives_data,
    territories_data,
    urban_functions_dict,
)
from idu_api.urban_api.dto import NormativeDTO, TerritoryWithNormativesDTO
from idu_api.urban_api.exceptions.logic.common import (
    EntitiesNotFoundByIds,
    EntityAlreadyExists,
    EntityNotFoundById,
    EntityNotFoundByParams,
    TooManyObjectsError,
)
from idu_api.urban_api.logic.impl.helpers.utils import (
    OBJECTS_NUMBER_LIMIT,
    check_existence,
    extract_values_from_model,
)
from idu_api.urban_api.schemas import NormativeDelete, NormativePatch, NormativePost

func: Callable

####################################################################################
#                           Main business-logic                                    #
####################################################################################


async def get_normatives_by_territory_id_from_db(
    conn: "AsyncConnection",
    territory_id: int,
    year: int | None,
    last_only: bool,
    include_child_territories: bool,
    cities_only: bool,
) -> list[NormativeDTO]:
    """Get list of normatives for a given territory (and, optionally, its descendants)."""

    territories = await _get_territories(conn, territory_id, include_child_territories)
    nodes: dict[int, TerritoryNode] = {}
    for territory in territories["all"]:
        nodes[territory.territory_id] = TerritoryNode(territory)

    for node in nodes.values():
        parent_id = node.territory.parent_id
        if parent_id is not None and parent_id in nodes:
            node.parent = nodes[parent_id]
            nodes[parent_id].children.append(node)

    all_territory_ids = list(nodes.keys())
    normative_rows = await _get_normatives(conn, all_territory_ids, year, last_only)

    for norm in normative_rows:
        tid = norm.territory_id
        if tid in nodes:
            if last_only:
                key = (norm.service_type_id, norm.urban_function_id)
                nodes[tid].normatives[key] = norm
            else:
                key = (norm.service_type_id, norm.urban_function_id, norm.year)
                nodes[tid].normatives_by_year[key] = norm

    unique_keys: set[tuple[int | None, int | None]] = _get_unique_normative_keys(normative_rows)
    years = {norm.year for norm in normative_rows} if not last_only else {None}

    result: list[NormativeDTO] = []
    for territory in territories["requested"]:
        node = nodes[territory.territory_id]
        for key in unique_keys:
            for y in years:
                effective = node.get_effective_normative(key, y)
                if effective is not None:
                    result.append(effective)

    if cities_only:
        city_ids = {t.territory_id for t in territories["requested"] if t.is_city}
        result = [n for n in result if n.territory_id in city_ids]

    return result


async def get_normatives_by_ids_from_db(conn: AsyncConnection, ids: list[int]) -> list[NormativeDTO]:
    """Get a list of normative objects by list of identifiers."""

    if len(ids) > OBJECTS_NUMBER_LIMIT:
        raise TooManyObjectsError(len(ids), OBJECTS_NUMBER_LIMIT)

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
            territories_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
        )
        .select_from(
            service_types_normatives_data.join(
                territories_data,
                territories_data.c.territory_id == service_types_normatives_data.c.territory_id,
            )
            .outerjoin(
                service_types_dict,
                service_types_dict.c.service_type_id == service_types_normatives_data.c.service_type_id,
            )
            .outerjoin(
                urban_functions_dict,
                urban_functions_dict.c.urban_function_id == service_types_normatives_data.c.urban_function_id,
            )
        )
        .where(service_types_normatives_data.c.normative_id.in_(ids))
    )

    result = (await conn.execute(statement)).mappings().all()
    if len(ids) > len(result):
        raise EntitiesNotFoundByIds("normative")

    return [NormativeDTO(**normative) for normative in result]


async def add_normatives_to_territory_to_db(
    conn: AsyncConnection, territory_id: int, normatives: list[NormativePost]
) -> list[NormativeDTO]:
    """Create normative objects."""

    if not await check_existence(conn, territories_data, conditions={"territory_id": territory_id}):
        raise EntityNotFoundById(territory_id, "territory")

    service_type_ids = {normative.service_type_id for normative in normatives if normative.service_type_id is not None}
    urban_function_ids = {
        normative.urban_function_id for normative in normatives if normative.urban_function_id is not None
    }

    statement = select(service_types_dict.c.service_type_id).where(
        service_types_dict.c.service_type_id.in_(service_type_ids)
    )
    service_types = (await conn.execute(statement)).scalars().all()
    if len(service_type_ids) > len(service_types):
        raise EntitiesNotFoundByIds("service type")

    statement = select(urban_functions_dict.c.urban_function_id).where(
        urban_functions_dict.c.urban_function_id.in_(urban_function_ids)
    )
    urban_functions = (await conn.execute(statement)).scalars().all()
    if len(urban_function_ids) > len(urban_functions):
        raise EntitiesNotFoundByIds("urban function")

    for normative in normatives:
        conditions = {
            "territory_id": territory_id,
            "year": normative.year,
        }
        if normative.service_type_id is not None:
            conditions["service_type_id"] = normative.service_type_id
        elif normative.urban_function_id is not None:
            conditions["urban_function_id"] = normative.urban_function_id
        else:
            raise EntityNotFoundByParams("normative", normative.service_type_id, normative.urban_function_id)

        if await check_existence(conn, service_types_normatives_data, conditions=conditions):
            raise EntityAlreadyExists(
                "normative", normative.service_type_id, normative.urban_function_id, territory_id, normative.year
            )

    statement = (
        insert(service_types_normatives_data)
        .values([{"territory_id": territory_id, **normative.model_dump()} for normative in normatives])
        .returning(service_types_normatives_data.c.normative_id)
    )
    normative_ids = (await conn.execute(statement)).scalars().all()

    await conn.commit()

    return await get_normatives_by_ids_from_db(conn, normative_ids)


async def put_normatives_by_territory_id_in_db(
    conn: AsyncConnection, territory_id: int, normatives: list[NormativePost]
) -> list[NormativeDTO]:
    """Update a normative objects by territory id - all its attributes."""

    if not await check_existence(conn, territories_data, conditions={"territory_id": territory_id}):
        raise EntityNotFoundById(territory_id, "territory")

    service_type_ids = {normative.service_type_id for normative in normatives if normative.service_type_id is not None}
    urban_function_ids = {
        normative.urban_function_id for normative in normatives if normative.urban_function_id is not None
    }

    statement = select(service_types_dict.c.service_type_id).where(
        service_types_dict.c.service_type_id.in_(service_type_ids)
    )
    service_types = (await conn.execute(statement)).scalars().all()
    if len(service_type_ids) > len(service_types):
        raise EntitiesNotFoundByIds("service type")

    statement = select(urban_functions_dict.c.urban_function_id).where(
        service_types_dict.c.urban_function_id.in_(urban_function_ids)
    )
    urban_functions = (await conn.execute(statement)).scalars().all()
    if len(urban_function_ids) > len(urban_functions):
        raise EntitiesNotFoundByIds("urban function")

    normative_ids = []
    for normative in normatives:
        conditions = {
            "territory_id": territory_id,
            "year": normative.year,
        }
        if normative.service_type_id is not None:
            conditions["service_type_id"] = normative.service_type_id
        elif normative.urban_function_id is not None:
            conditions["urban_function_id"] = normative.urban_function_id
        else:
            raise EntityNotFoundByParams("normative", normative.service_type_id, normative.urban_function_id)

        if await check_existence(conn, service_types_normatives_data, conditions=conditions):
            where_clause = and_(
                (
                    service_types_normatives_data.c.service_type_id == normative.service_type_id
                    if normative.service_type_id is not None
                    else True
                ),
                (
                    service_types_normatives_data.c.urban_function_id == normative.urban_function_id
                    if normative.urban_function_id is not None
                    else True
                ),
                service_types_normatives_data.c.territory_id == territory_id,
                service_types_normatives_data.c.year == normative.year,
            )

            values = extract_values_from_model(normative, to_update=True)

            statement = (
                update(service_types_normatives_data)
                .where(where_clause)
                .values(**values)
                .returning(service_types_normatives_data.c.normative_id)
            )
        else:
            statement = (
                insert(service_types_normatives_data)
                .values(**normative.model_dump(), territory_id=territory_id)
                .returning(service_types_normatives_data.c.normative_id)
            )

        normative_ids.append((await conn.execute(statement)).scalar_one())

    await conn.commit()

    return await get_normatives_by_ids_from_db(conn, normative_ids)


async def patch_normatives_by_territory_id_in_db(
    conn: AsyncConnection, territory_id: int, normatives: list[NormativePatch]
) -> list[NormativeDTO]:
    """Update a normative objects by territory id - only given attributes."""

    if not await check_existence(conn, territories_data, conditions={"territory_id": territory_id}):
        raise EntityNotFoundById(territory_id, "territory")

    service_type_ids = {normative.service_type_id for normative in normatives if normative.service_type_id is not None}
    urban_function_ids = {
        normative.urban_function_id for normative in normatives if normative.urban_function_id is not None
    }

    statement = select(service_types_dict.c.service_type_id).where(
        service_types_dict.c.service_type_id.in_(service_type_ids)
    )
    service_types = (await conn.execute(statement)).scalars().all()
    if len(service_type_ids) > len(service_types):
        raise EntitiesNotFoundByIds("service type")

    statement = select(urban_functions_dict.c.urban_function_id).where(
        service_types_dict.c.urban_function_id.in_(urban_function_ids)
    )
    urban_functions = (await conn.execute(statement)).scalars().all()
    if len(urban_function_ids) > len(urban_functions):
        raise EntitiesNotFoundByIds("urban function")

    normative_ids = []
    for normative in normatives:
        conditions = {
            "territory_id": territory_id,
            "year": normative.year,
        }
        if normative.service_type_id is not None:
            conditions["service_type_id"] = normative.service_type_id
        elif normative.urban_function_id is not None:
            conditions["urban_function_id"] = normative.urban_function_id
        else:
            raise EntityNotFoundByParams("normative", normative.service_type_id, normative.urban_function_id)

        if not await check_existence(conn, service_types_normatives_data, conditions=conditions):
            raise EntityNotFoundByParams(
                "normative", normative.service_type_id, normative.urban_function_id, territory_id, normative.year
            )

        where_clause = and_(
            (
                service_types_normatives_data.c.service_type_id == normative.service_type_id
                if normative.service_type_id is not None
                else True
            ),
            (
                service_types_normatives_data.c.urban_function_id == normative.urban_function_id
                if normative.urban_function_id is not None
                else True
            ),
            service_types_normatives_data.c.territory_id == territory_id,
            service_types_normatives_data.c.year == normative.year,
        )

        values = extract_values_from_model(normative, exclude_unset=True, to_update=True)

        statement = (
            update(service_types_normatives_data)
            .where(where_clause)
            .values(**values)
            .returning(service_types_normatives_data.c.normative_id)
        )

        normative_ids.append((await conn.execute(statement)).scalar_one())

    await conn.commit()

    return await get_normatives_by_ids_from_db(conn, normative_ids)


async def delete_normatives_by_territory_id_in_db(
    conn: AsyncConnection, territory_id: int, normatives: list[NormativeDelete]
) -> dict:
    """Delete normative objects by territory id."""

    if not await check_existence(conn, territories_data, conditions={"territory_id": territory_id}):
        raise EntityNotFoundById(territory_id, "territory")

    service_type_ids = {normative.service_type_id for normative in normatives if normative.service_type_id is not None}
    urban_function_ids = {
        normative.urban_function_id for normative in normatives if normative.urban_function_id is not None
    }

    statement = select(service_types_dict.c.service_type_id).where(
        service_types_dict.c.service_type_id.in_(service_type_ids)
    )
    service_types = (await conn.execute(statement)).scalars().all()
    if len(service_type_ids) > len(service_types):
        raise EntitiesNotFoundByIds("service type")

    statement = select(urban_functions_dict.c.urban_function_id).where(
        service_types_dict.c.urban_function_id.in_(urban_function_ids)
    )
    urban_functions = (await conn.execute(statement)).scalars().all()
    if len(urban_function_ids) > len(urban_functions):
        raise EntitiesNotFoundByIds("urban function")

    normative_ids = []
    for normative in normatives:
        conditions = {
            "territory_id": territory_id,
            "year": normative.year,
        }
        if normative.service_type_id is not None:
            conditions["service_type_id"] = normative.service_type_id
        elif normative.urban_function_id is not None:
            conditions["urban_function_id"] = normative.urban_function_id
        else:
            raise EntityNotFoundByParams("normative", normative.service_type_id, normative.urban_function_id)

        statement = select(service_types_normatives_data.c.normative_id).where(
            service_types_normatives_data.c.urban_function_id == normative.urban_function_id,
            service_types_normatives_data.c.territory_id == territory_id,
            service_types_normatives_data.c.year == normative.year,
        )
        normative_id = (await conn.execute(statement)).scalar_one_or_none()
        if normative_id is None:
            raise EntityNotFoundByParams(
                "normative", normative.service_type_id, normative.urban_function_id, territory_id, normative.year
            )

        normative_ids.append(normative_id)

    statement = delete(service_types_normatives_data).where(
        service_types_normatives_data.c.normative_id.in_(normative_ids)
    )
    await conn.execute(statement)

    await conn.commit()

    return {"status": "ok"}


async def get_normatives_values_by_parent_id_from_db(
    conn: AsyncConnection,
    parent_id: int | None,
    year: int | None,
    last_only: bool,
) -> list[TerritoryWithNormativesDTO]:
    """Get list of normatives with values for territory by parent id and year."""

    if parent_id is not None:
        if not await check_existence(conn, territories_data, conditions={"territory_id": parent_id}):
            raise EntityNotFoundById(parent_id, "territory")

    statement = select(
        territories_data.c.territory_id,
        territories_data.c.name,
        territories_data.c.parent_id,
        ST_AsEWKB(territories_data.c.geometry).label("geometry"),
        ST_AsEWKB(territories_data.c.centre_point).label("centre_point"),
    ).where(territories_data.c.parent_id == parent_id)

    child_territories = list((await conn.execute(statement)).mappings().all())
    ancestors = list(await _get_ancestors(conn, child_territories[0].territory_id))
    nodes: dict[int, TerritoryNode] = {}
    for territory in child_territories + ancestors:
        nodes[territory.territory_id] = TerritoryNode(territory)

    for node in nodes.values():
        parent_id = node.territory.parent_id
        if parent_id is not None and parent_id in nodes:
            node.parent = nodes[parent_id]
            nodes[parent_id].children.append(node)

    all_territory_ids = list(nodes.keys())
    normative_rows = await _get_normatives(conn, all_territory_ids, year, last_only)

    for norm in normative_rows:
        tid = norm.territory_id
        if tid in nodes:
            if last_only:
                key = (norm.service_type_id, norm.urban_function_id)
                nodes[tid].normatives[key] = norm
            else:
                key = (norm.service_type_id, norm.urban_function_id, norm.year)
                nodes[tid].normatives_by_year[key] = norm

    unique_keys: set[tuple[int | None, int | None]] = _get_unique_normative_keys(normative_rows)
    years = {norm.year for norm in normative_rows} if not last_only else {None}

    territories_normatives: dict[int, list[NormativeDTO]] = defaultdict(list)
    for territory in child_territories:
        node = nodes[territory.territory_id]
        for key in unique_keys:
            for y in years:
                effective = node.get_effective_normative(key, y)
                if effective is not None:
                    territories_normatives[territory.territory_id].append(effective)

    result: list[TerritoryWithNormativesDTO] = []
    for territory in child_territories:
        result.append(
            TerritoryWithNormativesDTO(
                **{k: v for k, v in territory.items() if k != "parent_id"},
                normatives=territories_normatives[territory.territory_id],
            )
        )

    return result


####################################################################################
#                            Helper functions                                      #
####################################################################################


async def _get_territories(
    conn: AsyncConnection,
    territory_id: int,
    include_child_territories: bool,
) -> dict[str, list[RowMapping]]:
    """Get all relevant territories (ancestors and optionally descendants)."""

    statement = select(
        territories_data.c.territory_id,
        territories_data.c.name,
        territories_data.c.parent_id,
        territories_data.c.is_city,
        territories_data.c.level,
    ).where(territories_data.c.territory_id == territory_id)
    territory = (await conn.execute(statement)).mappings().one_or_none()
    if territory is None:
        raise EntityNotFoundById(territory_id, "territory")

    ancestors = list(await _get_ancestors(conn, territory_id))

    descendants = [territory]
    if include_child_territories:
        descendants += list(await _get_descendants(conn, territory_id))

    return {
        "ancestors": ancestors,
        "requested": descendants,
        "all": ancestors + descendants,
    }


async def _get_ancestors(conn: AsyncConnection, territory_id: int) -> Sequence[RowMapping]:
    """Get all ancestors of the given territory."""

    cte_statement = (
        select(
            territories_data.c.territory_id,
            territories_data.c.name,
            territories_data.c.parent_id,
            territories_data.c.is_city,
            territories_data.c.level,
        )
        .where(territories_data.c.territory_id == territory_id)
        .cte(name="territories_recursive", recursive=True)
    )
    recursive_part = select(
        territories_data.c.territory_id,
        territories_data.c.name,
        territories_data.c.parent_id,
        territories_data.c.is_city,
        territories_data.c.level,
    ).join(cte_statement, territories_data.c.territory_id == cte_statement.c.parent_id)
    cte_statement = cte_statement.union_all(recursive_part)

    statement = (
        select(cte_statement).where(cte_statement.c.territory_id != territory_id).order_by(cte_statement.c.level.desc())
    )
    result = (await conn.execute(statement)).mappings().all()

    return result


async def _get_descendants(conn: AsyncConnection, territory_id: int) -> Sequence[RowMapping]:
    """Get all descendants of the given territory."""

    cte_statement = (
        select(
            territories_data.c.territory_id,
            territories_data.c.name,
            territories_data.c.parent_id,
            territories_data.c.is_city,
            territories_data.c.level,
        )
        .where(territories_data.c.territory_id == territory_id)
        .cte(name="territories_recursive", recursive=True)
    )
    recursive_part = select(
        territories_data.c.territory_id,
        territories_data.c.name,
        territories_data.c.parent_id,
        territories_data.c.is_city,
        territories_data.c.level,
    ).join(cte_statement, territories_data.c.parent_id == cte_statement.c.territory_id)
    cte_statement = cte_statement.union_all(recursive_part)

    statement = (
        select(cte_statement).where(cte_statement.c.territory_id != territory_id).order_by(cte_statement.c.level.desc())
    )
    result = (await conn.execute(statement)).mappings().all()

    return result


def _get_territory_ancestors(territories: dict[str, list[RowMapping]]) -> dict[int, list[RowMapping]]:
    """Build a dictionary of direct ancestors for each territory."""

    territory_ancestors = {}
    all_territories = {t.territory_id: t for t in territories["all"]}

    for territory in territories["requested"]:
        current_id = territory.territory_id
        ancestors = []

        while True:
            parent_id = all_territories[current_id].parent_id
            if parent_id is None:
                break

            parent = all_territories.get(parent_id, None)
            if parent is None or parent.parent_id is None:
                break

            ancestors.append(parent)
            current_id = parent_id

        territory_ancestors[territory.territory_id] = ancestors

    return territory_ancestors


async def _get_normatives(
    conn: AsyncConnection, territory_ids: list[int], year: int | None, last_only: bool
) -> list[RowMapping]:
    """Get all normatives for the given territory IDs and year."""

    select_from = service_types_normatives_data.outerjoin(
        service_types_dict,
        service_types_dict.c.service_type_id == service_types_normatives_data.c.service_type_id,
    ).outerjoin(
        urban_functions_dict,
        urban_functions_dict.c.urban_function_id == service_types_normatives_data.c.urban_function_id,
    )

    if last_only:
        subquery = (
            select(
                service_types_normatives_data.c.service_type_id,
                service_types_normatives_data.c.urban_function_id,
                service_types_normatives_data.c.territory_id,
                func.max(service_types_normatives_data.c.year).label("max_year"),
            )
            .where(service_types_normatives_data.c.territory_id.in_(territory_ids))
            .group_by(
                service_types_normatives_data.c.service_type_id,
                service_types_normatives_data.c.urban_function_id,
                service_types_normatives_data.c.territory_id,
            )
            .subquery()
        )
        select_from = select_from.join(
            subquery,
            (
                (service_types_normatives_data.c.service_type_id == subquery.c.service_type_id)
                | (service_types_normatives_data.c.urban_function_id == subquery.c.urban_function_id)
            )
            & (service_types_normatives_data.c.territory_id == subquery.c.territory_id)
            & (service_types_normatives_data.c.year == subquery.c.max_year),
        )

    statement = (
        select(
            service_types_normatives_data.c.territory_id,
            service_types_normatives_data.c.service_type_id,
            service_types_dict.c.name.label("service_type_name"),
            service_types_normatives_data.c.urban_function_id,
            urban_functions_dict.c.name.label("urban_function_name"),
            service_types_normatives_data.c.is_regulated,
            service_types_normatives_data.c.radius_availability_meters,
            service_types_normatives_data.c.time_availability_minutes,
            service_types_normatives_data.c.services_per_1000_normative,
            service_types_normatives_data.c.services_capacity_per_1000_normative,
            service_types_normatives_data.c.year,
            service_types_normatives_data.c.source,
            service_types_normatives_data.c.created_at,
            service_types_normatives_data.c.updated_at,
        )
        .select_from(select_from)
        .where(
            service_types_normatives_data.c.territory_id.in_(territory_ids),
            service_types_normatives_data.c.year == year if year is not None else True,
        )
    )
    result = (await conn.execute(statement)).mappings().all()

    return result


def _get_unique_normative_keys(normatives: Sequence[RowMapping]) -> set[tuple[int | None, int | None]]:
    """Get all unique combinations of service_type_id and urban_function_id for normatives."""

    keys = set()
    for normative in normatives:
        key = (normative.service_type_id, normative.urban_function_id)
        keys.add(key)

    return keys


def _set_normative(
    territory: RowMapping,
    ancestors: dict[int, list[RowMapping]] | list[RowMapping],
    top_ancestor: RowMapping,
    normatives: list[RowMapping],
    service_type_id: int | None,
    urban_function_id: int | None,
    year: int | None = None,
) -> NormativeDTO | None:
    """Get normative by unique pair of service_type_id and urban_function_id for given territory
    with setting normative type (self, parent of global)."""

    self_normative = next(
        (
            n
            for n in normatives
            if n.territory_id == territory.territory_id
            and (n.service_type_id == service_type_id or n.urban_function_id == urban_function_id)
            and (year is None or n.year == year)
        ),
        None,
    )
    if self_normative is not None:
        return NormativeDTO(
            **{k: v for k, v in self_normative.items() if k != "territory_id"},
            territory_id=territory.territory_id,
            territory_name=territory.name,
            normative_type="self",
        )

    if isinstance(ancestors, dict):
        ancestors = ancestors[territory.territory_id]
    for ancestor in ancestors:
        parent_normative = next(
            (
                n
                for n in normatives
                if n.territory_id == ancestor.territory_id
                and (n.service_type_id == service_type_id or n.urban_function_id == urban_function_id)
                and (year is None or n.year == year)
            ),
            None,
        )
        if parent_normative is not None:
            return NormativeDTO(
                **{k: v for k, v in parent_normative.items() if k != "territory_id"},
                territory_id=territory.territory_id,
                territory_name=territory.name,
                normative_type="parent",
            )
        break
    else:
        if top_ancestor is not None:
            global_normative = next(
                (
                    n
                    for n in normatives
                    if n.territory_id == top_ancestor.territory_id
                    and (n.service_type_id == service_type_id or n.urban_function_id == urban_function_id)
                    and (year is None or n.year == year)
                ),
                None,
            )
            if global_normative is not None:
                return NormativeDTO(
                    **{k: v for k, v in global_normative.items() if k != "territory_id"},
                    territory_id=territory.territory_id,
                    territory_name=territory.name,
                    normative_type="global",
                )

    return None


class TerritoryNode:

    def __init__(self, territory):
        # territory is a RowMapping object with the fields: territory_id, name, parent_id, is_city, level, etc.
        self.territory = territory
        self.parent: TerritoryNode | None = None
        self.children: list[TerritoryNode] = []
        # Normatives obtained from the database for this territory.
        # The key depends on the mode: if last_only, then (service_type_id, urban_function_id),
        # otherwise – (service_type_id, urban_function_id, year)
        self.normatives: dict[tuple[int | None, int | None], dict] = {}
        self.normatives_by_year: dict[tuple[int | None, int | None, int], dict] = {}

    def get_top_ancestor(self) -> "TerritoryNode":
        """Returns the top ancestor (level = 1)."""
        node = self
        while node.parent:
            node = node.parent
        return node

    def get_effective_normative(
        self, normative_key: tuple[int | None, int | None], year: int | None = None
    ) -> NormativeDTO | None:
        """
        Defines the normative for a given territory by key.
        If the year is specified (last_only=False), the key with the year is used.
        Logic:
          - If there is a standard for this territory, it is returned with the "self" type.
          - Otherwise, if the direct parent has a standard, the "parent" type.
          - Otherwise, if the global (top) ancestor has a standard, the "global" type.
        """
        if year is None:
            # last_only mode: key without year
            if normative_key in self.normatives:
                norm = self.normatives[normative_key]
                return NormativeDTO(
                    **{k: v for k, v in norm.items() if k != "territory_id"},
                    territory_id=self.territory.territory_id,
                    territory_name=self.territory.name,
                    normative_type="self",
                )
            # Checking the parent's normative
            if self.parent and normative_key in self.parent.normatives:
                norm = self.parent.normatives[normative_key]
                return NormativeDTO(
                    **{k: v for k, v in norm.items() if k != "territory_id"},
                    territory_id=self.territory.territory_id,
                    territory_name=self.territory.name,
                    normative_type="parent",
                )
            # If not, we try the global normative from the top ancestor
            top = self.get_top_ancestor()
            if top and normative_key in top.normatives:
                norm = top.normatives[normative_key]
                return NormativeDTO(
                    **{k: v for k, v in norm.items() if k != "territory_id"},
                    territory_id=self.territory.territory_id,
                    territory_name=self.territory.name,
                    normative_type="global",
                )
        else:
            # mode with year: key with year
            key_with_year = (normative_key[0], normative_key[1], year)
            if key_with_year in self.normatives_by_year:
                norm = self.normatives_by_year[key_with_year]
                return NormativeDTO(
                    **{k: v for k, v in norm.items() if k != "territory_id"},
                    territory_id=self.territory.territory_id,
                    territory_name=self.territory.name,
                    normative_type="self",
                )
            if self.parent:
                key_with_year = (normative_key[0], normative_key[1], year)
                if key_with_year in self.parent.normatives_by_year:
                    norm = self.parent.normatives_by_year[key_with_year]
                    return NormativeDTO(
                        **{k: v for k, v in norm.items() if k != "territory_id"},
                        territory_id=self.territory.territory_id,
                        territory_name=self.territory.name,
                        normative_type="parent",
                    )
            top = self.get_top_ancestor()
            if top and key_with_year in top.normatives_by_year:
                norm = top.normatives_by_year[key_with_year]
                return NormativeDTO(
                    **{k: v for k, v in norm.items() if k != "territory_id"},
                    territory_id=self.territory.territory_id,
                    territory_name=self.territory.name,
                    normative_type="global",
                )
        return None
