"""Functional zones internal logic is defined here."""

from sqlalchemy import and_, insert, select, update
from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.common.db.entities import functional_zone_types_dict, profiles_reclamation_data, territories_data
from idu_api.urban_api.dto import FunctionalZoneTypeDTO, ProfilesReclamationDataDTO, ProfilesReclamationDataMatrixDTO
from idu_api.urban_api.exceptions.logic.common import EntityAlreadyExists, EntityNotFoundById
from idu_api.urban_api.schemas import FunctionalZoneTypePost, ProfilesReclamationDataPost, ProfilesReclamationDataPut


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
            if territory_id is not None else profiles_reclamation_data.c.territory_id.is_(None)
        )
        .order_by(profiles_reclamation_data.c.profile_reclamation_id)
    )
    profiles_reclamations = (await conn.execute(statement)).mappings().all()

    labels = sorted(list(set([row["source_profile_id"] for row in profiles_reclamations] +
                             [row["target_profile_id"] for row in profiles_reclamations])))

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
        and_(
            profiles_reclamation_data.c.source_profile_id == profiles_reclamation.source_profile_id,
            profiles_reclamation_data.c.target_profile_id == profiles_reclamation.target_profile_id,
            profiles_reclamation_data.c.territory_id == profiles_reclamation.territory_id
            if profiles_reclamation.territory_id is not None else profiles_reclamation_data.c.territory_id.is_(None),
        )
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

    statement = (
        select(profiles_reclamation_data)
        .where(profiles_reclamation_data.c.profile_reclamation_id == profile_reclamation_id)
    )
    result = (await conn.execute(statement)).scalar_one_or_none()
    if result is None:
        raise EntityNotFoundById(profile_reclamation_id, "profile reclamation")

    statement = select(profiles_reclamation_data).where(
        and_(
            profiles_reclamation_data.c.source_profile_id == profiles_reclamation.source_profile_id,
            profiles_reclamation_data.c.target_profile_id == profiles_reclamation.target_profile_id,
            profiles_reclamation_data.c.territory_id == profiles_reclamation.territory_id
            if profiles_reclamation.territory_id is not None else profiles_reclamation_data.c.territory_id.is_(None),
        )
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
