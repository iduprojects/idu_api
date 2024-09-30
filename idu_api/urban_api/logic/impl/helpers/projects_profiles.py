"""Projects profiles internal logic is defined here."""

from sqlalchemy import insert, select
from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.common.db.entities import target_profiles_dict
from idu_api.urban_api.dto import TargetProfileDTO
from idu_api.urban_api.exceptions.logic.common import EntityAlreadyExists
from idu_api.urban_api.schemas.profiles import TargetProfilesPost


async def get_target_profiles_from_db(conn: AsyncConnection) -> list[TargetProfileDTO]:
    """Get all target profile objects."""

    statement = select(target_profiles_dict).order_by(target_profiles_dict.c.target_profile_id)

    return [TargetProfileDTO(*unit) for unit in await conn.execute(statement)]


async def add_target_profile_to_db(
    conn: AsyncConnection,
    target_profile: TargetProfilesPost,
) -> TargetProfileDTO:
    """Create a new target profile object."""

    statement = select(target_profiles_dict).where(target_profiles_dict.c.name == target_profile.name)
    result = (await conn.execute(statement)).scalar_one_or_none()
    if result is not None:
        raise EntityAlreadyExists("target profile", target_profile.name)

    statement = insert(target_profiles_dict).values(name=target_profile.name).returning(target_profiles_dict)
    result = (await conn.execute(statement)).mappings().one()

    await conn.commit()

    return TargetProfileDTO(**result)
