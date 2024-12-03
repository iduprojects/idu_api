"""Projects functional zones internal logic is defined here."""

from datetime import datetime, timezone

from geoalchemy2.functions import ST_AsGeoJSON, ST_GeomFromText, ST_Intersection, ST_Intersects, ST_Union
from sqlalchemy import cast, delete, insert, select, text, update
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.common.db.entities import (
    functional_zone_types_dict,
    functional_zones_data,
    profiles_data,
    projects_data,
    scenarios_data,
    territories_data,
)
from idu_api.urban_api.dto import FunctionalZoneDataDTO, FunctionalZoneSourceDTO, ProjectProfileDTO
from idu_api.urban_api.exceptions.logic.common import EntitiesNotFoundByIds, EntityNotFoundById
from idu_api.urban_api.exceptions.logic.users import AccessDeniedError
from idu_api.urban_api.logic.impl.helpers.functional_zones import check_functional_zone_type_existence
from idu_api.urban_api.schemas import (
    ProjectProfilePatch,
    ProjectProfilePost,
    ProjectProfilePut,
)


async def check_functional_zone_existence(conn: AsyncConnection, profile_id: int) -> bool:
    """functional_zone existence checker function."""

    statement = select(profiles_data).where(profiles_data.c.profile_id == profile_id)
    functional_zone = (await conn.execute(statement)).mappings().one_or_none()

    if functional_zone is None:
        return False

    return True


async def get_functional_zones_sources_by_scenario_id_from_db(
    conn: AsyncConnection,
    scenario_id: int,
    user_id: str,
) -> list[FunctionalZoneSourceDTO]:
    """Get list of pairs year + source for functional zones for given scenario."""

    statement = select(scenarios_data.c.project_id).where(scenarios_data.c.scenario_id == scenario_id)
    project_id = (await conn.execute(statement)).scalar_one_or_none()
    if project_id is None:
        raise EntityNotFoundById(scenario_id, "scenario")

    statement = select(projects_data).where(projects_data.c.project_id == project_id)
    project = (await conn.execute(statement)).mappings().one_or_none()
    if project.user_id != user_id:
        raise AccessDeniedError(project_id, "project")

    statement = (
        select(profiles_data.c.year, profiles_data.c.source)
        .where(profiles_data.c.scenario_id == scenario_id)
        .distinct()
    )
    result = (await conn.execute(statement)).mappings().all()

    return [FunctionalZoneSourceDTO(**res) for res in result]


async def get_functional_zones_by_scenario_id_from_db(
    conn: AsyncConnection,
    scenario_id: int,
    year: int,
    source: str,
    functional_zone_type_id: int | None,
    user_id: str,
) -> list[ProjectProfileDTO]:
    """Get list of functional zone objects by scenario identifier."""

    statement = select(scenarios_data.c.project_id).where(scenarios_data.c.scenario_id == scenario_id)
    project_id = (await conn.execute(statement)).scalar_one_or_none()
    if project_id is None:
        raise EntityNotFoundById(scenario_id, "scenario")

    statement = select(projects_data).where(projects_data.c.project_id == project_id)
    project = (await conn.execute(statement)).mappings().one_or_none()
    if project.user_id != user_id:
        raise AccessDeniedError(project_id, "project")

    statement = (
        select(
            profiles_data.c.profile_id,
            profiles_data.c.scenario_id,
            scenarios_data.c.name.label("scenario_name"),
            profiles_data.c.functional_zone_type_id,
            functional_zone_types_dict.c.name.label("functional_zone_type_name"),
            functional_zone_types_dict.c.zone_nickname.label("functional_zone_type_nickname"),
            profiles_data.c.name,
            cast(ST_AsGeoJSON(profiles_data.c.geometry), JSONB).label("geometry"),
            profiles_data.c.year,
            profiles_data.c.source,
            profiles_data.c.properties,
            profiles_data.c.created_at,
            profiles_data.c.updated_at,
        )
        .select_from(
            profiles_data.join(
                scenarios_data,
                scenarios_data.c.scenario_id == profiles_data.c.scenario_id,
            ).join(
                functional_zone_types_dict,
                functional_zone_types_dict.c.functional_zone_type_id == profiles_data.c.functional_zone_type_id,
            )
        )
        .where(
            profiles_data.c.scenario_id == scenario_id,
            profiles_data.c.year == year,
            profiles_data.c.source == source,
        )
    )

    if functional_zone_type_id is not None:
        statement = statement.where(profiles_data.c.functional_zone_type_id == functional_zone_type_id)

    result = (await conn.execute(statement)).mappings().all()

    return [ProjectProfileDTO(**profile) for profile in result]


async def get_context_functional_zones_sources_by_scenario_id_from_db(
    conn: AsyncConnection,
    scenario_id: int,
    user_id: str,
) -> list[FunctionalZoneSourceDTO]:
    """Get list of pairs year + source for functional zones for 'context' of the project territory."""

    statement = select(scenarios_data.c.project_id).where(scenarios_data.c.scenario_id == scenario_id)
    project_id = (await conn.execute(statement)).scalar_one_or_none()
    if project_id is None:
        raise EntityNotFoundById(scenario_id, "scenario")

    statement = select(projects_data).where(projects_data.c.project_id == project_id)
    project = (await conn.execute(statement)).mappings().one_or_none()
    if project.user_id != user_id:
        raise AccessDeniedError(project_id, "project")

    context_territories = select(
        territories_data.c.territory_id,
        territories_data.c.geometry,
    ).where(territories_data.c.territory_id.in_(project.properties["context"]))
    unified_geometry = select(ST_Union(context_territories.c.geometry)).scalar_subquery()
    all_descendants = (
        select(
            territories_data.c.territory_id,
            territories_data.c.parent_id,
        )
        .where(territories_data.c.territory_id.in_(select(context_territories.c.territory_id)))
        .cte(name="all_descendants", recursive=True)
    )
    all_descendants = all_descendants.union_all(
        select(
            territories_data.c.territory_id,
            territories_data.c.parent_id,
        ).select_from(
            territories_data.join(
                all_descendants,
                territories_data.c.parent_id == all_descendants.c.territory_id,
            )
        )
    )
    all_ancestors = (
        select(
            territories_data.c.territory_id,
            territories_data.c.parent_id,
        )
        .where(territories_data.c.territory_id.in_(select(context_territories.c.territory_id)))
        .cte(name="all_ancestors", recursive=True)
    )
    all_ancestors = all_ancestors.union_all(
        select(
            territories_data.c.territory_id,
            territories_data.c.parent_id,
        ).select_from(
            territories_data.join(
                all_ancestors,
                territories_data.c.territory_id == all_ancestors.c.parent_id,
            )
        )
    )
    all_related_territories = (
        select(all_descendants.c.territory_id).union(select(all_ancestors.c.territory_id)).subquery()
    )

    statement = (
        select(functional_zones_data.c.year, functional_zones_data.c.source)
        .where(
            functional_zones_data.c.territory_id.in_(select(all_related_territories.c.territory_id)),
            ST_Intersects(functional_zones_data.c.geometry, unified_geometry),
        )
        .distinct()
    )
    result = (await conn.execute(statement)).mappings().all()

    return [FunctionalZoneSourceDTO(**res) for res in result]


async def get_context_functional_zones_by_scenario_id_from_db(
    conn: AsyncConnection,
    scenario_id: int,
    year: int,
    source: str,
    functional_zone_type_id: int | None,
    user_id: str,
) -> list[FunctionalZoneDataDTO]:
    """Get list of functional zone objects for 'context' of the project territory."""

    statement = select(scenarios_data.c.project_id).where(scenarios_data.c.scenario_id == scenario_id)
    project_id = (await conn.execute(statement)).scalar_one_or_none()
    if project_id is None:
        raise EntityNotFoundById(scenario_id, "scenario")

    statement = select(projects_data).where(projects_data.c.project_id == project_id)
    project = (await conn.execute(statement)).mappings().one_or_none()
    if project.user_id != user_id:
        raise AccessDeniedError(project_id, "project")

    context_territories = select(
        territories_data.c.territory_id,
        territories_data.c.geometry,
    ).where(territories_data.c.territory_id.in_(project.properties["context"]))
    unified_geometry = select(ST_Union(context_territories.c.geometry)).scalar_subquery()
    all_descendants = (
        select(
            territories_data.c.territory_id,
            territories_data.c.parent_id,
        )
        .where(territories_data.c.territory_id.in_(select(context_territories.c.territory_id)))
        .cte(name="all_descendants", recursive=True)
    )
    all_descendants = all_descendants.union_all(
        select(
            territories_data.c.territory_id,
            territories_data.c.parent_id,
        ).select_from(
            territories_data.join(
                all_descendants,
                territories_data.c.parent_id == all_descendants.c.territory_id,
            )
        )
    )
    all_ancestors = (
        select(
            territories_data.c.territory_id,
            territories_data.c.parent_id,
        )
        .where(territories_data.c.territory_id.in_(select(context_territories.c.territory_id)))
        .cte(name="all_ancestors", recursive=True)
    )
    all_ancestors = all_ancestors.union_all(
        select(
            territories_data.c.territory_id,
            territories_data.c.parent_id,
        ).select_from(
            territories_data.join(
                all_ancestors,
                territories_data.c.territory_id == all_ancestors.c.parent_id,
            )
        )
    )
    all_related_territories = (
        select(all_descendants.c.territory_id).union(select(all_ancestors.c.territory_id)).subquery()
    )

    statement = (
        select(
            functional_zones_data.c.functional_zone_id,
            functional_zones_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
            functional_zones_data.c.functional_zone_type_id,
            functional_zone_types_dict.c.name.label("functional_zone_type_name"),
            functional_zone_types_dict.c.zone_nickname.label("functional_zone_type_nickname"),
            functional_zones_data.c.name,
            cast(ST_AsGeoJSON(ST_Intersection(functional_zones_data.c.geometry, unified_geometry)), JSONB).label(
                "geometry"
            ),
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
        .where(
            functional_zones_data.c.year == year,
            functional_zones_data.c.source == source,
            functional_zones_data.c.territory_id.in_(select(all_related_territories.c.territory_id)),
            ST_Intersects(functional_zones_data.c.geometry, unified_geometry),
        )
    )

    if functional_zone_type_id is not None:
        statement = statement.where(functional_zones_data.c.functional_zone_type_id == functional_zone_type_id)

    result = (await conn.execute(statement)).mappings().all()

    return [FunctionalZoneDataDTO(**zone) for zone in result]


async def get_functional_zone_by_id(conn: AsyncConnection, profile_id: int) -> ProjectProfileDTO:
    """Get functional zone by identifier."""

    statement = (
        select(
            profiles_data.c.profile_id,
            profiles_data.c.scenario_id,
            scenarios_data.c.name.label("scenario_name"),
            profiles_data.c.functional_zone_type_id,
            functional_zone_types_dict.c.name.label("functional_zone_type_name"),
            functional_zone_types_dict.c.zone_nickname.label("functional_zone_type_nickname"),
            profiles_data.c.name,
            cast(ST_AsGeoJSON(profiles_data.c.geometry), JSONB).label("geometry"),
            profiles_data.c.year,
            profiles_data.c.source,
            profiles_data.c.properties,
            profiles_data.c.created_at,
            profiles_data.c.updated_at,
        )
        .select_from(
            profiles_data.join(
                scenarios_data,
                scenarios_data.c.scenario_id == profiles_data.c.scenario_id,
            ).join(
                functional_zone_types_dict,
                functional_zone_types_dict.c.functional_zone_type_id == profiles_data.c.functional_zone_type_id,
            )
        )
        .where(profiles_data.c.profile_id == profile_id)
    )

    result = (await conn.execute(statement)).mappings().one_or_none()
    if result is None:
        raise EntityNotFoundById(profile_id, "scenario functional zone")

    return ProjectProfileDTO(**result)


async def add_scenario_functional_zones_to_db(
    conn: AsyncConnection, functional_zones: list[ProjectProfilePost], scenario_id: int, user_id: str
) -> list[ProjectProfileDTO]:
    """Add list of scenario functional zones objects."""

    statement = select(scenarios_data.c.project_id).where(scenarios_data.c.scenario_id == scenario_id)
    project_id = (await conn.execute(statement)).scalar_one_or_none()
    if project_id is None:
        raise EntityNotFoundById(scenario_id, "scenario")

    statement = select(projects_data).where(projects_data.c.project_id == project_id)
    project = (await conn.execute(statement)).mappings().one_or_none()
    if project.user_id != user_id:
        raise AccessDeniedError(project_id, "project")

    statement = delete(profiles_data).where(profiles_data.c.scenario_id == scenario_id)
    await conn.execute(statement)

    functional_zone_type_ids = set(functional_zone.functional_zone_type_id for functional_zone in functional_zones)
    statement = select(functional_zone_types_dict.c.functional_zone_type_id).where(
        functional_zone_types_dict.c.functional_zone_type_id.in_(functional_zone_type_ids)
    )
    functional_zone_types = (await conn.execute(statement)).scalars().all()
    if len(list(functional_zone_types)) < len(functional_zone_type_ids):
        raise EntitiesNotFoundByIds("functional zone type")

    insert_values = [
        {
            "scenario_id": scenario_id,
            "name": functional_zone.name,
            "functional_zone_type_id": functional_zone.functional_zone_type_id,
            "geometry": ST_GeomFromText(str(functional_zone.geometry.as_shapely_geometry()), text("4326")),
            "year": functional_zone.year,
            "source": functional_zone.source,
            "properties": functional_zone.properties,
        }
        for functional_zone in functional_zones
    ]

    statement = insert(profiles_data).values(insert_values).returning(profiles_data.c.profile_id)
    profile_ids = (await conn.execute(statement)).scalars().all()

    await conn.commit()

    statement = (
        select(
            profiles_data.c.profile_id,
            profiles_data.c.scenario_id,
            scenarios_data.c.name.label("scenario_name"),
            profiles_data.c.functional_zone_type_id,
            functional_zone_types_dict.c.name.label("functional_zone_type_name"),
            functional_zone_types_dict.c.zone_nickname.label("functional_zone_type_nickname"),
            profiles_data.c.name,
            cast(ST_AsGeoJSON(profiles_data.c.geometry), JSONB).label("geometry"),
            profiles_data.c.year,
            profiles_data.c.source,
            profiles_data.c.properties,
            profiles_data.c.created_at,
            profiles_data.c.updated_at,
        )
        .select_from(
            profiles_data.join(
                scenarios_data,
                scenarios_data.c.scenario_id == profiles_data.c.scenario_id,
            ).join(
                functional_zone_types_dict,
                functional_zone_types_dict.c.functional_zone_type_id == profiles_data.c.functional_zone_type_id,
            )
        )
        .where(profiles_data.c.profile_id.in_(profile_ids))
    )
    result = (await conn.execute(statement)).mappings().all()

    return [ProjectProfileDTO(**profile) for profile in result]


async def put_scenario_functional_zone_to_db(
    conn: AsyncConnection,
    functional_zone: ProjectProfilePut,
    scenario_id: int,
    functional_zone_id: int,
    user_id: str,
) -> ProjectProfileDTO:
    """Update scenario functional zone by all its attributes."""

    statement = select(scenarios_data.c.project_id).where(scenarios_data.c.scenario_id == scenario_id)
    project_id = (await conn.execute(statement)).scalar_one_or_none()
    if project_id is None:
        raise EntityNotFoundById(scenario_id, "scenario")

    statement = select(projects_data).where(projects_data.c.project_id == project_id)
    project = (await conn.execute(statement)).mappings().one_or_none()
    if project.user_id != user_id:
        raise AccessDeniedError(project_id, "project")

    functional_zone_exists = await check_functional_zone_existence(conn, functional_zone_id)
    if not functional_zone_exists:
        raise EntityNotFoundById(functional_zone_id, "scenario functional zone")

    type_exists = await check_functional_zone_type_existence(conn, functional_zone.functional_zone_type_id)
    if not type_exists:
        raise EntityNotFoundById(functional_zone.functional_zone_type_id, "functional zone type")

    statement = (
        update(profiles_data)
        .where(profiles_data.c.profile_id == functional_zone_id)
        .values(
            functional_zone_type_id=functional_zone.functional_zone_type_id,
            name=functional_zone.name,
            geometry=ST_GeomFromText(str(functional_zone.geometry.as_shapely_geometry()), text("4326")),
            year=functional_zone.year,
            source=functional_zone.source,
            properties=functional_zone.properties,
            updated_at=datetime.now(timezone.utc),
        )
        .returning(profiles_data.c.profile_id)
    )

    result_id = (await conn.execute(statement)).scalar_one()

    await conn.commit()

    return await get_functional_zone_by_id(conn, result_id)


async def patch_scenario_functional_zone_to_db(
    conn: AsyncConnection,
    functional_zone: ProjectProfilePatch,
    scenario_id: int,
    functional_zone_id: int,
    user_id: str,
) -> ProjectProfileDTO:
    """Update scenario functional zone by only given attributes."""

    statement = select(scenarios_data.c.project_id).where(scenarios_data.c.scenario_id == scenario_id)
    project_id = (await conn.execute(statement)).scalar_one_or_none()
    if project_id is None:
        raise EntityNotFoundById(scenario_id, "scenario")

    statement = select(projects_data).where(projects_data.c.project_id == project_id)
    project = (await conn.execute(statement)).mappings().one_or_none()
    if project.user_id != user_id:
        raise AccessDeniedError(project_id, "project")

    functional_zone_exists = await check_functional_zone_existence(conn, functional_zone_id)
    if not functional_zone_exists:
        raise EntityNotFoundById(functional_zone_id, "scenario functional zone")

    type_exists = await check_functional_zone_type_existence(conn, functional_zone.functional_zone_type_id)
    if not type_exists:
        raise EntityNotFoundById(functional_zone.functional_zone_type_id, "functional zone type")

    statement = (
        update(profiles_data)
        .where(profiles_data.c.profile_id == functional_zone_id)
        .returning(profiles_data.c.profile_id)
    )

    values_to_update = {}
    for k, v in functional_zone.model_dump(exclude_unset=True).items():
        if k == "geometry":
            values_to_update.update(
                {"geometry": ST_GeomFromText(str(functional_zone.geometry.as_shapely_geometry()), text("4326"))}
            )
        else:
            values_to_update.update({k: v})

    statement = statement.values(updated_at=datetime.now(timezone.utc), **values_to_update)
    result_id = (await conn.execute(statement)).scalar_one()

    await conn.commit()

    return await get_functional_zone_by_id(conn, result_id)


async def delete_functional_zone_by_scenario_id_from_db(conn: AsyncConnection, scenario_id: int, user_id: str) -> dict:
    """Delete functional zones by scenario identifier."""

    statement = select(scenarios_data.c.project_id).where(scenarios_data.c.scenario_id == scenario_id)
    project_id = (await conn.execute(statement)).scalar_one_or_none()
    if project_id is None:
        raise EntityNotFoundById(scenario_id, "scenario")

    statement = select(projects_data).where(projects_data.c.project_id == project_id)
    project = (await conn.execute(statement)).mappings().one_or_none()
    if project.user_id != user_id:
        raise AccessDeniedError(project_id, "project")

    statement = delete(profiles_data).where(profiles_data.c.scenario_id == scenario_id)
    await conn.execute(statement)
    await conn.commit()

    return {"result": "ok"}
