"""Territories functional zones internal logic is defined here."""

from typing import Callable

from geoalchemy2.functions import ST_AsGeoJSON, ST_GeomFromText
from sqlalchemy import cast, delete, insert, select, text, update
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.common.db.entities import functional_zone_types_dict, functional_zones_data, territories_data
from idu_api.urban_api.dto import FunctionalZoneDataDTO
from idu_api.urban_api.exceptions.logic.common import EntityNotFoundById
from idu_api.urban_api.schemas import FunctionalZoneDataPatch, FunctionalZoneDataPost, FunctionalZoneDataPut

func: Callable


async def check_territory_existence(conn: AsyncConnection, territory_id: int) -> bool:
    """Territory existence checker function."""

    statement = select(territories_data).where(territories_data.c.territory_id == territory_id)
    territory = (await conn.execute(statement)).mappings().one_or_none()

    if territory is None:
        return False

    return True


async def check_functional_zone_existence(conn: AsyncConnection, functional_zone_id: int) -> bool:
    """Functional zone (and relevant functional zone type) existence checker function."""

    statement = select(functional_zones_data).where(functional_zones_data.c.functional_zone_id == functional_zone_id)
    functional_zone = (await conn.execute(statement)).mappings().one_or_none()

    if functional_zone is None:
        return False

    return True


async def check_functional_zone_type_existence(conn: AsyncConnection, functional_zone_type_id: int) -> bool:
    """Functional zone type existence checker function."""

    statement = select(functional_zone_types_dict).where(
        functional_zone_types_dict.c.functional_zone_type_id == functional_zone_type_id
    )
    functional_zone_type = (await conn.execute(statement)).mappings().one_or_none()

    if functional_zone_type is None:
        return False

    return True


async def get_functional_zone_by_id(conn: AsyncConnection, functional_zone_id: int) -> FunctionalZoneDataDTO:
    """Get functional zone by id."""

    statement = select(
        functional_zones_data.c.functional_zone_id,
        functional_zones_data.c.territory_id,
        functional_zones_data.c.functional_zone_type_id,
        cast(ST_AsGeoJSON(functional_zones_data.c.geometry), JSONB).label("geometry"),
    ).where(functional_zones_data.c.functional_zone_id == functional_zone_id)

    result = (await conn.execute(statement)).mappings().one_or_none()

    if result is None:
        raise EntityNotFoundById(functional_zone_id, "functional zone")

    return FunctionalZoneDataDTO(**result)


async def get_functional_zones_by_territory_id_from_db(
    conn: AsyncConnection, territory_id: int, functional_zone_type_id: int | None, include_child_territories: bool
) -> list[FunctionalZoneDataDTO]:
    """Get functional zones with geometry by territory id."""

    territory_exists = await check_territory_existence(conn, territory_id)
    if not territory_exists:
        raise EntityNotFoundById(territory_id, "territory")

    statement = select(
        functional_zones_data.c.functional_zone_id,
        functional_zones_data.c.territory_id,
        functional_zones_data.c.functional_zone_type_id,
        cast(ST_AsGeoJSON(functional_zones_data.c.geometry), JSONB).label("geometry"),
    )

    if functional_zone_type_id is not None:
        statement = statement.where(functional_zones_data.c.functional_zone_type_id == functional_zone_type_id)

    if include_child_territories:
        territories_cte = (
            select(territories_data.c.territory_id)
            .where(territories_data.c.territory_id == territory_id)
            .cte(recursive=True)
        )

        territories_cte = territories_cte.union_all(
            select(territories_data.c.territory_id).where(
                territories_data.c.parent_id == territories_cte.c.territory_id
            )
        )

        statement = statement.where(functional_zones_data.c.territory_id.in_(select(territories_cte))).distinct()
    else:
        statement = statement.where(functional_zones_data.c.territory_id == territory_id)

    result = (await conn.execute(statement)).mappings().all()

    return [FunctionalZoneDataDTO(**zone) for zone in result]


async def add_functional_zone_for_territory_to_db(
    conn: AsyncConnection, territory_id: int, functional_zone: FunctionalZoneDataPost
) -> FunctionalZoneDataDTO:
    """Add functional zone for territory."""

    territory_exists = await check_territory_existence(conn, territory_id)
    if not territory_exists:
        raise EntityNotFoundById(territory_id, "territory")

    type_exists = await check_functional_zone_type_existence(conn, functional_zone.functional_zone_type_id)
    if not type_exists:
        raise EntityNotFoundById(functional_zone.functional_zone_type_id, "functional zone type")

    statement = (
        insert(functional_zones_data)
        .values(
            territory_id=territory_id,
            functional_zone_type_id=functional_zone.functional_zone_type_id,
            geometry=ST_GeomFromText(str(functional_zone.geometry.as_shapely_geometry()), text("4326")),
        )
        .returning(functional_zones_data.c.functional_zone_id)
    )

    functional_zone_id = (await conn.execute(statement)).scalar_one()
    result = await get_functional_zone_by_id(conn, functional_zone_id)

    await conn.commit()

    return result


async def add_functional_zones_for_territory_to_db(
    conn: AsyncConnection, territory_id: int, functional_zones: list[FunctionalZoneDataPost]
) -> list[FunctionalZoneDataDTO]:
    """Add a bunch of functional zones for territory."""

    functional_zones_dto: list[FunctionalZoneDataDTO] = []

    for functional_zone in functional_zones:
        territory_exists = await check_territory_existence(conn, territory_id)
        if not territory_exists:
            raise EntityNotFoundById(territory_id, "territory")

        type_exists = await check_functional_zone_type_existence(conn, functional_zone.functional_zone_type_id)
        if not type_exists:
            raise EntityNotFoundById(functional_zone.functional_zone_type_id, "functional zone type")

    for functional_zone in functional_zones:
        statement = (
            insert(functional_zones_data)
            .values(
                territory_id=territory_id,
                functional_zone_type_id=functional_zone.functional_zone_type_id,
                geometry=ST_GeomFromText(str(functional_zone.geometry.as_shapely_geometry()), text("4326")),
            )
            .returning(functional_zones_data.c.functional_zone_id)
        )

        functional_zone_id = (await conn.execute(statement)).scalar_one()
        functional_zone_dto = await get_functional_zone_by_id(conn, functional_zone_id)

        functional_zones_dto.append(functional_zone_dto)

    await conn.commit()

    return functional_zones_dto


async def put_functional_zone_for_territory_to_db(
    conn: AsyncConnection, territory_id: int, functional_zone_id: int, functional_zone: FunctionalZoneDataPut
) -> FunctionalZoneDataDTO:
    """Put functional zone for territory."""

    territory_exists = await check_territory_existence(conn, territory_id)
    if not territory_exists:
        raise EntityNotFoundById(territory_id, "territory")

    zone_exists = await check_functional_zone_existence(conn, functional_zone_id)
    if not zone_exists:
        raise EntityNotFoundById(functional_zone_id, "functional zone")

    type_exists = await check_functional_zone_type_existence(conn, functional_zone.functional_zone_type_id)
    if not type_exists:
        raise EntityNotFoundById(functional_zone.functional_zone_type_id, "functional zone type")

    statement = (
        update(functional_zones_data)
        .where(functional_zones_data.c.functional_zone_id == functional_zone_id)
        .values(
            territory_id=territory_id,
            functional_zone_type_id=functional_zone.functional_zone_type_id,
            geometry=ST_GeomFromText(str(functional_zone.geometry.as_shapely_geometry()), text("4326")),
        )
        .returning(functional_zones_data.c.functional_zone_id)
    )

    result_id = (await conn.execute(statement)).scalar_one()

    await conn.commit()

    return await get_functional_zone_by_id(conn, result_id)


async def patch_functional_zone_for_territory_to_db(
    conn: AsyncConnection, territory_id: int, functional_zone_id: int, functional_zone: FunctionalZoneDataPatch
) -> FunctionalZoneDataDTO:
    """Patch functional zone for territory."""

    territory_exists = await check_territory_existence(conn, territory_id)
    if not territory_exists:
        raise EntityNotFoundById(territory_id, "territory")

    zone_exists = await check_functional_zone_existence(conn, functional_zone_id)
    if not zone_exists:
        raise EntityNotFoundById(functional_zone_id, "functional zone")

    if functional_zone.functional_zone_type_id is not None:
        type_exists = await check_functional_zone_type_existence(conn, functional_zone.functional_zone_type_id)
        if not type_exists:
            raise EntityNotFoundById(functional_zone.functional_zone_type_id, "functional zone type")

    statement = (
        update(functional_zones_data)
        .where(functional_zones_data.c.functional_zone_id == functional_zone_id)
        .returning(functional_zones_data.c.functional_zone_id)
    )

    values_to_update = {}
    for k, v in functional_zone.model_dump(exclude={"geometry"}, exclude_unset=True).items():
        values_to_update.update({k: v})

    if functional_zone.geometry is not None:
        values_to_update.update(
            {"geometry": ST_GeomFromText(str(functional_zone.geometry.as_shapely_geometry()), text("4326"))}
        )

    statement = statement.values(**values_to_update)
    result_id = (await conn.execute(statement)).scalar_one()

    await conn.commit()

    return await get_functional_zone_by_id(conn, result_id)


async def delete_specific_functional_zone_for_territory_from_db(
    conn: AsyncConnection, territory_id: int, functional_zone_id: int
) -> dict:
    """Delete specific functional zone for territory."""

    territory_exists = await check_territory_existence(conn, territory_id)
    if not territory_exists:
        raise EntityNotFoundById(territory_id, "territory")

    zone_exists = await check_functional_zone_existence(conn, functional_zone_id)
    if not zone_exists:
        raise EntityNotFoundById(functional_zone_id, "functional zone")

    statement = delete(functional_zones_data).where(functional_zones_data.c.functional_zone_id == functional_zone_id)
    await conn.execute(statement)
    await conn.commit()

    return {"result": "ok"}


async def delete_all_functional_zones_for_territory_from_db(conn: AsyncConnection, territory_id: int) -> dict:
    """Delete all functional zones for territory."""

    territory_exists = await check_territory_existence(conn, territory_id)
    if not territory_exists:
        raise EntityNotFoundById(territory_id, "territory")

    statement = delete(functional_zones_data).where(functional_zones_data.c.territory_id == territory_id)
    await conn.execute(statement)
    await conn.commit()

    return {"result": "ok"}
