"""Functional zones internal logic is defined here."""

from datetime import datetime, timezone

from geoalchemy2.functions import ST_AsGeoJSON, ST_GeomFromText
from sqlalchemy import cast, delete, insert, select, text, update
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.common.db.entities import (
    functional_zone_types_dict,
    functional_zones_data,
    profiles_reclamation_data,
    territories_data,
)
from idu_api.urban_api.dto import (
    FunctionalZoneDataDTO,
    FunctionalZoneTypeDTO,
    ProfilesReclamationDataDTO,
    ProfilesReclamationDataMatrixDTO,
)
from idu_api.urban_api.exceptions.logic.common import EntityAlreadyExists, EntityNotFoundById
from idu_api.urban_api.logic.impl.helpers.territory_objects import check_territory_existence
from idu_api.urban_api.schemas import (
    FunctionalZoneDataPatch,
    FunctionalZoneDataPost,
    FunctionalZoneDataPut,
    FunctionalZoneTypePost,
    ProfilesReclamationDataPost,
    ProfilesReclamationDataPut,
)


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


async def get_functional_zone_types_from_db(conn: AsyncConnection) -> list[FunctionalZoneTypeDTO]:
    """Get all functional zone type objects."""

    statement = select(functional_zone_types_dict).order_by(functional_zone_types_dict.c.functional_zone_type_id)

    return [FunctionalZoneTypeDTO(**zone_type) for zone_type in (await conn.execute(statement)).mappings().all()]


async def add_functional_zone_type_to_db(
    conn: AsyncConnection,
    functional_zone_type: FunctionalZoneTypePost,
) -> FunctionalZoneTypeDTO:
    """Create a new functional zone type object."""

    statement = select(functional_zone_types_dict).where(functional_zone_types_dict.c.name == functional_zone_type.name)
    result = (await conn.execute(statement)).scalar_one_or_none()
    if result is not None:
        raise EntityAlreadyExists("functional zone type", functional_zone_type.name)

    statement = (
        insert(functional_zone_types_dict)
        .values(
            name=functional_zone_type.name,
            zone_nickname=functional_zone_type.zone_nickname,
            description=functional_zone_type.description,
        )
        .returning(functional_zone_types_dict)
    )
    result = (await conn.execute(statement)).mappings().one()

    await conn.commit()

    return FunctionalZoneTypeDTO(**result)


async def get_profiles_reclamation_data_from_db(conn: AsyncConnection) -> list[ProfilesReclamationDataDTO]:
    """Get a list of profiles reclamation data."""

    statement = (
        select(profiles_reclamation_data, territories_data.c.name.label("territory_name"))
        .select_from(
            profiles_reclamation_data.outerjoin(
                territories_data,
                territories_data.c.territory_id == profiles_reclamation_data.c.territory_id,
            )
        )
        .order_by(profiles_reclamation_data.c.profile_reclamation_id)
    )

    profiles_reclamations = (await conn.execute(statement)).mappings().all()

    return [ProfilesReclamationDataDTO(**profile_reclamation) for profile_reclamation in profiles_reclamations]


async def get_all_sources_from_db(conn: AsyncConnection) -> list[int]:
    """Get a list of all profiles reclamation sources."""

    statement = (
        select(profiles_reclamation_data.c.source_profile_id)
        .group_by(profiles_reclamation_data.c.source_profile_id)
        .order_by(profiles_reclamation_data.c.profile_reclamation_id)
    )

    source_ids = (await conn.execute(statement)).scalars().all()

    return [int(source_id) for source_id in source_ids]


async def get_profiles_reclamation_data_matrix_from_db(
    conn: AsyncConnection, labels: list[int]
) -> ProfilesReclamationDataMatrixDTO:
    """Get a matrix of profiles reclamation data for specific labels."""

    def generate_zero_matrix(length: int) -> list[list[float]]:
        return [[0.0] * length for _ in range(length)]

    size = len(labels)
    technical_price = generate_zero_matrix(size)
    technical_time = generate_zero_matrix(size)
    biological_price = generate_zero_matrix(size)
    biological_time = generate_zero_matrix(size)

    labels = sorted(labels)
    indexes_map: dict[int, int] = {labels[i]: i for i in range(size)}

    for label in labels:
        reclamations = await get_profiles_reclamation_data_by_source_id_from_db(conn, label)
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


async def get_profiles_reclamation_data_matrix_by_territory_id_from_db(
    conn: AsyncConnection, territory_id: int | None
) -> ProfilesReclamationDataMatrixDTO:
    """Get a matrix of profiles reclamation data for given territory."""

    statement = (
        select(profiles_reclamation_data)
        .where(
            profiles_reclamation_data.c.territory_id == territory_id
            if territory_id is not None
            else profiles_reclamation_data.c.territory_id.is_(None)
        )
        .order_by(profiles_reclamation_data.c.profile_reclamation_id)
    )
    profiles_reclamations = (await conn.execute(statement)).mappings().all()

    labels = sorted(
        list(
            set(
                [row["source_profile_id"] for row in profiles_reclamations]
                + [row["target_profile_id"] for row in profiles_reclamations]
            )
        )
    )

    matrix_size = len(labels)
    technical_price = [[0.0] * matrix_size for _ in range(matrix_size)]
    technical_time = [[0.0] * matrix_size for _ in range(matrix_size)]
    biological_price = [[0.0] * matrix_size for _ in range(matrix_size)]
    biological_time = [[0.0] * matrix_size for _ in range(matrix_size)]

    label_to_index = {label: i for i, label in enumerate(labels)}
    for row in profiles_reclamations:
        src_idx = label_to_index[row["source_profile_id"]]
        tgt_idx = label_to_index[row["target_profile_id"]]
        technical_price[src_idx][tgt_idx] = float(row["technical_price"])
        technical_time[src_idx][tgt_idx] = float(row["technical_time"])
        biological_price[src_idx][tgt_idx] = float(row["biological_price"])
        biological_time[src_idx][tgt_idx] = float(row["biological_time"])

    return ProfilesReclamationDataMatrixDTO(
        labels=labels,
        technical_price=technical_price,
        technical_time=technical_time,
        biological_price=biological_price,
        biological_time=biological_time,
    )


async def add_profiles_reclamation_data_to_db(
    conn: AsyncConnection, profiles_reclamation: ProfilesReclamationDataPost
) -> ProfilesReclamationDataDTO:
    """Add a new profiles reclamation data."""

    statement = select(profiles_reclamation_data).where(
        profiles_reclamation_data.c.source_profile_id == profiles_reclamation.source_profile_id,
        profiles_reclamation_data.c.target_profile_id == profiles_reclamation.target_profile_id,
        (
            profiles_reclamation_data.c.territory_id == profiles_reclamation.territory_id
            if profiles_reclamation.territory_id is not None
            else profiles_reclamation_data.c.territory_id.is_(None)
        ),
    )
    result = (await conn.execute(statement)).scalar_one_or_none()
    if result is not None:
        raise EntityAlreadyExists("profile reclamation")

    statement = (
        insert(profiles_reclamation_data)
        .values(
            source_profile_id=profiles_reclamation.source_profile_id,
            target_profile_id=profiles_reclamation.target_profile_id,
            territory_id=profiles_reclamation.territory_id,
            technical_price=profiles_reclamation.technical_price,
            technical_time=profiles_reclamation.technical_time,
            biological_price=profiles_reclamation.biological_price,
            biological_time=profiles_reclamation.biological_time,
        )
        .returning(profiles_reclamation_data.c.profile_reclamation_id)
    )
    profile_reclamation_id = (await conn.execute(statement)).scalar_one()

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

    await conn.commit()

    return ProfilesReclamationDataDTO(**result)


async def put_profiles_reclamation_data_to_db(
    conn: AsyncConnection, profile_reclamation_id: int, profiles_reclamation: ProfilesReclamationDataPut
) -> ProfilesReclamationDataDTO:
    """Put profiles reclamation data."""

    statement = select(profiles_reclamation_data).where(
        profiles_reclamation_data.c.profile_reclamation_id == profile_reclamation_id
    )
    result = (await conn.execute(statement)).scalar_one_or_none()
    if result is None:
        raise EntityNotFoundById(profile_reclamation_id, "profile reclamation")

    statement = select(profiles_reclamation_data).where(
        profiles_reclamation_data.c.source_profile_id == profiles_reclamation.source_profile_id,
        profiles_reclamation_data.c.target_profile_id == profiles_reclamation.target_profile_id,
        (
            profiles_reclamation_data.c.territory_id == profiles_reclamation.territory_id
            if profiles_reclamation.territory_id is not None
            else profiles_reclamation_data.c.territory_id.is_(None)
        ),
    )
    result = (await conn.execute(statement)).scalar_one_or_none()
    if result is not None:
        raise EntityAlreadyExists("profile reclamation")

    statement = (
        update(profiles_reclamation_data)
        .where(profiles_reclamation_data.c.profile_reclamation_id == profile_reclamation_id)
        .values(
            source_profile_id=profiles_reclamation.source_profile_id,
            target_profile_id=profiles_reclamation.target_profile_id,
            territory_id=profiles_reclamation.territory_id,
            technical_price=profiles_reclamation.technical_price,
            technical_time=profiles_reclamation.technical_time,
            biological_price=profiles_reclamation.biological_price,
            biological_time=profiles_reclamation.biological_time,
        )
        .returning(profiles_reclamation_data.c.profile_reclamation_id)
    )
    profile_reclamation_id = (await conn.execute(statement)).scalar_one()

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

    await conn.commit()

    return ProfilesReclamationDataDTO(**result)


async def get_profiles_reclamation_data_by_source_id_from_db(
    conn: AsyncConnection, source_id: int
) -> list[ProfilesReclamationDataDTO]:
    """Get a list of profiles reclamation data with specified source profile id."""

    statement = (
        select(profiles_reclamation_data, territories_data.c.name.label("territory_name"))
        .select_from(
            profiles_reclamation_data.outerjoin(
                territories_data,
                territories_data.c.territory_id == profiles_reclamation_data.c.territory_id,
            )
        )
        .where(profiles_reclamation_data.c.source_profile_id == source_id)
        .order_by(profiles_reclamation_data.c.source_profile_id, profiles_reclamation_data.c.target_profile_id)
    )

    profiles_reclamations = (await conn.execute(statement)).mappings().all()

    return [ProfilesReclamationDataDTO(**profile_reclamation) for profile_reclamation in profiles_reclamations]


async def get_functional_zone_by_id(conn: AsyncConnection, functional_zone_id: int) -> FunctionalZoneDataDTO:
    """Get functional zone by identifier."""

    statement = (
        select(
            functional_zones_data.c.functional_zone_id,
            functional_zones_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
            functional_zones_data.c.functional_zone_type_id,
            functional_zone_types_dict.c.name.label("functional_zone_type_name"),
            functional_zone_types_dict.c.zone_nickname.label("functional_zone_type_nickname"),
            functional_zones_data.c.name,
            cast(ST_AsGeoJSON(functional_zones_data.c.geometry), JSONB).label("geometry"),
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
        .where(functional_zones_data.c.functional_zone_id == functional_zone_id)
    )

    result = (await conn.execute(statement)).mappings().one_or_none()
    if result is None:
        raise EntityNotFoundById(functional_zone_id, "functional zone")

    return FunctionalZoneDataDTO(**result)


async def add_functional_zone_to_db(
    conn: AsyncConnection, functional_zone: FunctionalZoneDataPost
) -> FunctionalZoneDataDTO:
    """Add functional zone."""

    territory_exists = await check_territory_existence(conn, functional_zone.territory_id)
    if not territory_exists:
        raise EntityNotFoundById(functional_zone.territory_id, "territory")

    type_exists = await check_functional_zone_type_existence(conn, functional_zone.functional_zone_type_id)
    if not type_exists:
        raise EntityNotFoundById(functional_zone.functional_zone_type_id, "functional zone type")

    statement = (
        insert(functional_zones_data)
        .values(
            territory_id=functional_zone.territory_id,
            name=functional_zone.name,
            functional_zone_type_id=functional_zone.functional_zone_type_id,
            geometry=ST_GeomFromText(str(functional_zone.geometry.as_shapely_geometry()), text("4326")),
            year=functional_zone.year,
            source=functional_zone.source,
            properties=functional_zone.properties,
        )
        .returning(functional_zones_data.c.functional_zone_id)
    )
    functional_zone_id = (await conn.execute(statement)).scalar_one()

    await conn.commit()

    return await get_functional_zone_by_id(conn, functional_zone_id)


async def put_functional_zone_to_db(
    conn: AsyncConnection, functional_zone_id: int, functional_zone: FunctionalZoneDataPut
) -> FunctionalZoneDataDTO:
    """Update functional zone by all its attributes."""

    territory_exists = await check_territory_existence(conn, functional_zone.territory_id)
    if not territory_exists:
        raise EntityNotFoundById(functional_zone.territory_id, "territory")

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
            territory_id=functional_zone.territory_id,
            functional_zone_type_id=functional_zone.functional_zone_type_id,
            name=functional_zone.name,
            geometry=ST_GeomFromText(str(functional_zone.geometry.as_shapely_geometry()), text("4326")),
            year=functional_zone.year,
            source=functional_zone.source,
            properties=functional_zone.properties,
            updated_at=datetime.now(timezone.utc),
        )
        .returning(functional_zones_data.c.functional_zone_id)
    )

    result_id = (await conn.execute(statement)).scalar_one()

    await conn.commit()

    return await get_functional_zone_by_id(conn, result_id)


async def patch_functional_zone_to_db(
    conn: AsyncConnection, functional_zone_id: int, functional_zone: FunctionalZoneDataPatch
) -> FunctionalZoneDataDTO:
    """Update functional zone by only given attributes."""

    if functional_zone.territory_id is not None:
        territory_exists = await check_territory_existence(conn, functional_zone.territory_id)
        if not territory_exists:
            raise EntityNotFoundById(functional_zone.territory_id, "territory")

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

    statement = statement.values(updated_at=datetime.now(timezone.utc), **values_to_update)
    result_id = (await conn.execute(statement)).scalar_one()

    await conn.commit()

    return await get_functional_zone_by_id(conn, result_id)


async def delete_functional_zone_from_db(conn: AsyncConnection, functional_zone_id: int) -> dict:
    """Delete functional zone by identifier."""

    zone_exists = await check_functional_zone_existence(conn, functional_zone_id)
    if not zone_exists:
        raise EntityNotFoundById(functional_zone_id, "functional zone")

    statement = delete(functional_zones_data).where(functional_zones_data.c.functional_zone_id == functional_zone_id)
    await conn.execute(statement)
    await conn.commit()

    return {"result": "ok"}
