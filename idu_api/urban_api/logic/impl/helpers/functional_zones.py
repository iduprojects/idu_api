"""Functional zones internal logic is defined here."""

from collections.abc import Callable

from geoalchemy2.functions import ST_AsEWKB, ST_GeomFromWKB
from shapely.geometry import LineString, MultiPolygon, Point, Polygon
from sqlalchemy import case, delete, func, insert, select, text, update
from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.common.db.entities import (
    functional_zone_types_dict,
    functional_zones_data,
    profiles_reclamation_data,
    territories_data,
)
from idu_api.urban_api.dto import (
    FunctionalZoneDTO,
    FunctionalZoneTypeDTO,
    ProfilesReclamationDataDTO,
    ProfilesReclamationDataMatrixDTO,
)
from idu_api.urban_api.exceptions.logic.common import (
    EntitiesNotFoundByIds,
    EntityAlreadyExists,
    EntityNotFoundById,
    EntityNotFoundByParams,
    TooManyObjectsError,
)
from idu_api.urban_api.logic.impl.helpers.utils import (
    OBJECTS_NUMBER_LIMIT,
    SRID,
    check_existence,
    extract_values_from_model,
)
from idu_api.urban_api.schemas import (
    FunctionalZonePatch,
    FunctionalZonePost,
    FunctionalZonePut,
    FunctionalZoneTypePost,
    ProfilesReclamationData,
    ProfilesReclamationDataPost,
    ProfilesReclamationDataPut,
)

func: Callable
Geom = Point | Polygon | MultiPolygon | LineString


async def get_functional_zone_types_from_db(conn: AsyncConnection) -> list[FunctionalZoneTypeDTO]:
    """Get all functional zone type objects."""

    statement = select(functional_zone_types_dict).order_by(functional_zone_types_dict.c.functional_zone_type_id)

    return [FunctionalZoneTypeDTO(**zone_type) for zone_type in (await conn.execute(statement)).mappings().all()]


async def add_functional_zone_type_to_db(
    conn: AsyncConnection,
    functional_zone_type: FunctionalZoneTypePost,
) -> FunctionalZoneTypeDTO:
    """Create a new functional zone type object."""

    if await check_existence(conn, functional_zone_types_dict, conditions={"name": functional_zone_type.name}):
        raise EntityAlreadyExists("functional zone type", functional_zone_type.name)

    statement = (
        insert(functional_zone_types_dict)
        .values(**functional_zone_type.model_dump())
        .returning(functional_zone_types_dict)
    )
    result = (await conn.execute(statement)).mappings().one()

    await conn.commit()

    return FunctionalZoneTypeDTO(**result)


async def get_all_sources_from_db(conn: AsyncConnection) -> list[int]:
    """Get a list of all profiles reclamation sources."""

    statement = (
        select(profiles_reclamation_data.c.source_profile_id)
        .order_by(profiles_reclamation_data.c.source_profile_id)
        .distinct()
    )

    result = (await conn.execute(statement)).scalars().all()

    return result


async def get_profiles_reclamation_data_matrix_from_db(
    conn: AsyncConnection, labels: list[int], territory_id: int | None
) -> ProfilesReclamationDataMatrixDTO:
    """Get a matrix of profiles reclamation data for specific labels and territory."""

    size = len(labels)
    labels = sorted(labels)
    indexes_map: dict[int, int] = {labels[i]: i for i in range(size)}

    statement = select(profiles_reclamation_data.c.source_profile_id).where(
        profiles_reclamation_data.c.source_profile_id.in_(labels)
    )
    sources = (await conn.execute(statement)).scalars().all()
    if size > len(sources):
        raise EntitiesNotFoundByIds("source profile")

    priority_order = case(
        (
            (
                profiles_reclamation_data.c.territory_id == territory_id
                if territory_id is not None
                else profiles_reclamation_data.c.territory_id.is_(None)
            ),
            0,
        ),
        else_=1,
    ).desc()

    base_statement = (
        select(profiles_reclamation_data)
        .where(
            profiles_reclamation_data.c.source_profile_id.in_(labels),
            profiles_reclamation_data.c.target_profile_id.in_(labels),
        )
        .order_by(profiles_reclamation_data.c.source_profile_id, priority_order)
    )

    if territory_id is not None:
        if not await check_existence(conn, territories_data, conditions={"territory_id": territory_id}):
            raise EntityNotFoundById(territory_id, "territory")
        statement = base_statement.where(
            (profiles_reclamation_data.c.territory_id == territory_id)
            | (profiles_reclamation_data.c.territory_id.is_(None)),
        )
    else:
        statement = base_statement.where(profiles_reclamation_data.c.territory_id.is_(None))

    reclamations = (await conn.execute(statement)).mappings().all()

    def generate_zero_matrix(length: int) -> list[list[float]]:
        return [[0.0] * length for _ in range(length)]

    technical_price = generate_zero_matrix(size)
    technical_time = generate_zero_matrix(size)
    biological_price = generate_zero_matrix(size)
    biological_time = generate_zero_matrix(size)

    for reclamation in reclamations:
        i, j = indexes_map[reclamation.source_profile_id], indexes_map[reclamation.target_profile_id]
        technical_price[i][j] = reclamation.technical_price
        technical_time[i][j] = reclamation.technical_time
        biological_price[i][j] = reclamation.biological_price
        biological_time[i][j] = reclamation.biological_time

    return ProfilesReclamationDataMatrixDTO(
        labels=labels,
        technical_price=technical_price,
        technical_time=technical_time,
        biological_price=biological_price,
        biological_time=biological_time,
    )


async def get_profiles_reclamation_data_by_id_from_db(
    conn: AsyncConnection,
    profile_reclamation_id: int,
) -> ProfilesReclamationData:
    """Get a profiles reclamation data by identifier."""

    statement = (
        select(profiles_reclamation_data, territories_data.c.name.label("territory_name"))
        .select_from(
            profiles_reclamation_data.outerjoin(
                territories_data,
                territories_data.c.territory_id == profiles_reclamation_data.c.territory_id,
            )
        )
        .where(profiles_reclamation_data.c.profile_reclamation_id == profile_reclamation_id)
        .order_by(profiles_reclamation_data.c.profile_reclamation_id)
    )
    result = (await conn.execute(statement)).mappings().one()

    return ProfilesReclamationDataDTO(**result)


async def add_profiles_reclamation_data_to_db(
    conn: AsyncConnection, profiles_reclamation: ProfilesReclamationDataPost
) -> ProfilesReclamationDataDTO:
    """Add a new profiles reclamation data."""

    if await check_existence(
        conn,
        profiles_reclamation_data,
        conditions={
            "territory_id": profiles_reclamation.territory_id,
            "target_profile_id": profiles_reclamation.target_profile_id,
            "source_profile_id": profiles_reclamation.source_profile_id,
        },
    ):
        raise EntityAlreadyExists(
            "profiles reclamation data",
            profiles_reclamation.source_profile_id,
            profiles_reclamation.target_profile_id,
            profiles_reclamation.territory_id,
        )

    if not await check_existence(
        conn,
        functional_zone_types_dict,
        conditions={"functional_zone_type_id": profiles_reclamation.source_profile_id},
    ):
        raise EntityNotFoundById(profiles_reclamation.source_profile_id, "source profile")

    if not await check_existence(
        conn,
        functional_zone_types_dict,
        conditions={"functional_zone_type_id": profiles_reclamation.target_profile_id},
    ):
        raise EntityNotFoundById(profiles_reclamation.source_profile_id, "target profile")

    if profiles_reclamation.territory_id is not None and not await check_existence(
        conn,
        territories_data,
        conditions={"territory_id": profiles_reclamation.territory_id},
    ):
        raise EntityNotFoundById(profiles_reclamation.territory_id, "territory")

    statement = (
        insert(profiles_reclamation_data)
        .values(**profiles_reclamation.model_dump())
        .returning(profiles_reclamation_data.c.profile_reclamation_id)
    )
    profile_reclamation_id = (await conn.execute(statement)).scalar_one()

    await conn.commit()

    return await get_profiles_reclamation_data_by_id_from_db(conn, profile_reclamation_id)


async def put_profiles_reclamation_data_to_db(
    conn: AsyncConnection, profiles_reclamation: ProfilesReclamationDataPut
) -> ProfilesReclamationDataDTO:
    """Update profiles reclamation data if exists else create new profiles reclamation data."""

    if await check_existence(
        conn,
        profiles_reclamation_data,
        conditions={
            "territory_id": profiles_reclamation.territory_id,
            "target_profile_id": profiles_reclamation.target_profile_id,
            "source_profile_id": profiles_reclamation.source_profile_id,
        },
    ):
        statement = (
            update(profiles_reclamation_data)
            .where(
                profiles_reclamation_data.c.source_profile_id == profiles_reclamation.source_profile_id,
                profiles_reclamation_data.c.target_profile_id == profiles_reclamation.target_profile_id,
                (
                    profiles_reclamation_data.c.territory_id == profiles_reclamation.territory_id
                    if profiles_reclamation.territory_id is not None
                    else profiles_reclamation_data.c.territory_id.is_(None)
                ),
            )
            .values(**profiles_reclamation.model_dump())
            .returning(profiles_reclamation_data.c.profile_reclamation_id)
        )
    else:

        if not await check_existence(
            conn,
            functional_zone_types_dict,
            conditions={"functional_zone_type_id": profiles_reclamation.source_profile_id},
        ):
            raise EntityNotFoundById(profiles_reclamation.source_profile_id, "source profile")

        if not await check_existence(
            conn,
            functional_zone_types_dict,
            conditions={"functional_zone_type_id": profiles_reclamation.target_profile_id},
        ):
            raise EntityNotFoundById(profiles_reclamation.source_profile_id, "target profile")

        if profiles_reclamation.territory_id is not None and not await check_existence(
            conn, territories_data, conditions={"territory_id": profiles_reclamation.territory_id}
        ):
            raise EntityNotFoundById(profiles_reclamation.territory_id, "territory")

        statement = (
            insert(profiles_reclamation_data)
            .values(**profiles_reclamation.model_dump())
            .returning(profiles_reclamation_data.c.profile_reclamation_id)
        )

    profile_reclamation_id = (await conn.execute(statement)).scalar_one()

    await conn.commit()

    return await get_profiles_reclamation_data_by_id_from_db(conn, profile_reclamation_id)


async def delete_profiles_reclamation_data_from_db(
    conn: AsyncConnection, source_id: int, target_id: int, territory_id: int | None
) -> dict[str, str]:
    """Delete profiles reclamation data by source and target profile identifier and territory identifier."""

    if not await check_existence(
        conn,
        profiles_reclamation_data,
        conditions={
            "territory_id": territory_id,
            "target_profile_id": target_id,
            "source_profile_id": source_id,
        },
    ):
        raise EntityNotFoundByParams("profiles reclamation data", source_id, target_id, territory_id)

    statement = delete(profiles_reclamation_data).where(
        profiles_reclamation_data.c.source_profile_id == source_id,
        profiles_reclamation_data.c.target_profile_id == target_id,
        (
            profiles_reclamation_data.c.territory_id == territory_id
            if territory_id is not None
            else profiles_reclamation_data.c.territory_id.is_(None)
        ),
    )

    await conn.execute(statement)
    await conn.commit()

    return {"status": "ok"}


async def get_functional_zone_by_ids(conn: AsyncConnection, ids: list[int]) -> list[FunctionalZoneDTO]:
    """Get list of functional zones by identifiers."""

    if len(ids) > OBJECTS_NUMBER_LIMIT:
        raise TooManyObjectsError(len(ids), OBJECTS_NUMBER_LIMIT)

    statement = (
        select(
            functional_zones_data.c.functional_zone_id,
            functional_zones_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
            functional_zones_data.c.functional_zone_type_id,
            functional_zone_types_dict.c.name.label("functional_zone_type_name"),
            functional_zone_types_dict.c.zone_nickname.label("functional_zone_type_nickname"),
            functional_zone_types_dict.c.description.label("functional_zone_type_description"),
            functional_zones_data.c.name,
            ST_AsEWKB(functional_zones_data.c.geometry).label("geometry"),
            functional_zones_data.c.year,
            functional_zones_data.c.source,
            functional_zones_data.c.properties,
            functional_zones_data.c.created_at,
            functional_zones_data.c.updated_at,
        )
        .select_from(
            functional_zones_data.join(
                territories_data,
                territories_data.c.territory_id == functional_zones_data.c.territory_id,
            ).join(
                functional_zone_types_dict,
                functional_zone_types_dict.c.functional_zone_type_id == functional_zones_data.c.functional_zone_type_id,
            )
        )
        .where(functional_zones_data.c.functional_zone_id.in_(ids))
    )

    result = (await conn.execute(statement)).mappings().all()
    if len(ids) == 1 and not result:
        raise EntityNotFoundById(ids[0], "functional zone")
    if not result:
        raise EntitiesNotFoundByIds("functional zone")

    return [FunctionalZoneDTO(**zone) for zone in result]


async def get_functional_zone_by_id(conn: AsyncConnection, functional_zone_id: int) -> FunctionalZoneDTO:
    """Get functional zone by identifier."""

    return (await get_functional_zone_by_ids(conn, [functional_zone_id]))[0]


async def add_functional_zone_to_db(conn: AsyncConnection, functional_zone: FunctionalZonePost) -> FunctionalZoneDTO:
    """Add functional zone."""

    if not await check_existence(conn, territories_data, conditions={"territory_id": functional_zone.territory_id}):
        raise EntityNotFoundById(functional_zone.territory_id, "territory")

    if not await check_existence(
        conn,
        functional_zone_types_dict,
        conditions={"functional_zone_type_id": functional_zone.functional_zone_type_id},
    ):
        raise EntityNotFoundById(functional_zone.functional_zone_type_id, "functional zone type")

    values = extract_values_from_model(functional_zone)

    statement = insert(functional_zones_data).values(**values).returning(functional_zones_data.c.functional_zone_id)
    functional_zone_id = (await conn.execute(statement)).scalar_one()

    await conn.commit()

    return await get_functional_zone_by_id(conn, functional_zone_id)


async def put_functional_zone_to_db(
    conn: AsyncConnection, functional_zone_id: int, functional_zone: FunctionalZonePut
) -> FunctionalZoneDTO:
    """Update functional zone by all its attributes."""

    if not await check_existence(conn, functional_zones_data, conditions={"functional_zone_id": functional_zone_id}):
        raise EntityNotFoundById(functional_zone_id, "functional zone")

    if not await check_existence(conn, territories_data, conditions={"territory_id": functional_zone.territory_id}):
        raise EntityNotFoundById(functional_zone.territory_id, "territory")

    if not await check_existence(
        conn,
        functional_zone_types_dict,
        conditions={"functional_zone_type_id": functional_zone.functional_zone_type_id},
    ):
        raise EntityNotFoundById(functional_zone.functional_zone_type_id, "functional zone type")

    values = extract_values_from_model(functional_zone, to_update=True)

    statement = (
        update(functional_zones_data)
        .where(functional_zones_data.c.functional_zone_id == functional_zone_id)
        .values(**values)
    )

    await conn.execute(statement)
    await conn.commit()

    return await get_functional_zone_by_id(conn, functional_zone_id)


async def patch_functional_zone_to_db(
    conn: AsyncConnection, functional_zone_id: int, functional_zone: FunctionalZonePatch
) -> FunctionalZoneDTO:
    """Update functional zone by only given attributes."""

    if not await check_existence(conn, functional_zones_data, conditions={"functional_zone_id": functional_zone_id}):
        raise EntityNotFoundById(functional_zone_id, "functional zone")

    if functional_zone.territory_id is not None:
        if not await check_existence(conn, territories_data):
            raise EntityNotFoundById(functional_zone.territory_id, "territory")

    if functional_zone.functional_zone_type_id is not None:
        if not await check_existence(conn, functional_zone_types_dict):
            raise EntityNotFoundById(functional_zone.functional_zone_type_id, "functional zone type")

    values = extract_values_from_model(functional_zone, exclude_unset=True, to_update=True)

    statement = (
        update(functional_zones_data)
        .where(functional_zones_data.c.functional_zone_id == functional_zone_id)
        .values(**values)
    )

    await conn.execute(statement)
    await conn.commit()

    return await get_functional_zone_by_id(conn, functional_zone_id)


async def delete_functional_zone_from_db(conn: AsyncConnection, functional_zone_id: int) -> dict:
    """Delete functional zone by identifier."""

    if not await check_existence(conn, functional_zones_data, conditions={"functional_zone_id": functional_zone_id}):
        raise EntityNotFoundById(functional_zone_id, "functional zone")

    statement = delete(functional_zones_data).where(functional_zones_data.c.functional_zone_id == functional_zone_id)
    await conn.execute(statement)
    await conn.commit()

    return {"status": "ok"}


async def get_functional_zones_around_from_db(
    conn: AsyncConnection,
    geometry: Geom,
    year: int,
    source: str,
    functional_zone_type_id: int | None,
) -> list[FunctionalZoneDTO]:
    """Get functional zones which are in the given geometry."""

    given_geometry = select(ST_GeomFromWKB(geometry.wkb, text(str(SRID)))).scalar_subquery()
    statement = (
        select(functional_zones_data.c.functional_zone_id)
        .where(
            functional_zones_data.c.year == year,
            functional_zones_data.c.source == source,
            func.ST_Intersects(functional_zones_data.c.geometry, given_geometry),
        )
        .distinct()
    )
    if functional_zone_type_id is not None:
        statement = statement.where(functional_zones_data.c.functional_zone_type_id == functional_zone_type_id)

    ids = (await conn.execute(statement)).scalars().all()

    if len(ids) == 0:
        return []

    return await get_functional_zone_by_ids(conn, ids)
