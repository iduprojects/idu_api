"""Projects internal logic is defined here."""

from datetime import datetime, timezone

from geoalchemy2.functions import ST_AsGeoJSON, ST_GeomFromText
from sqlalchemy import cast, delete, insert, or_, select, text, update
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.common.db.entities import (
    object_geometries_data,
    projects_data,
    projects_object_geometries_data,
    projects_physical_objects_data,
    projects_services_data,
    projects_territory_data,
    projects_urban_objects_data,
    scenarios_data,
    urban_objects_data,
)
from idu_api.urban_api.dto import ProjectDTO, ProjectTerritoryDTO
from idu_api.urban_api.exceptions.logic.common import AccessDeniedError, EntityNotFoundById
from idu_api.urban_api.logic.impl.helpers.territories_physical_objects import (
    get_physical_objects_with_geometry_by_territory_id_from_db,
)
from idu_api.urban_api.logic.impl.helpers.territory_objects import get_common_territory_for_geometry
from idu_api.urban_api.logic.impl.helpers.territory_services import get_services_by_territory_id_from_db
from idu_api.urban_api.schemas import ProjectPatch, ProjectPost, ProjectPut


async def get_project_by_id_from_db(conn: AsyncConnection, project_id: int, user_id: str) -> ProjectDTO:
    """Get project object by id."""

    statement = select(projects_data).where(projects_data.c.project_id == project_id)
    result = (await conn.execute(statement)).mappings().one_or_none()

    if result is None:
        raise EntityNotFoundById(project_id, "projects_data")
    elif result.user_id != user_id and result.public is False:
        raise AccessDeniedError(project_id, "projects_data")

    return ProjectDTO(**result)


async def add_project_to_db(conn: AsyncConnection, project: ProjectPost, user_id: str) -> ProjectDTO:
    """Create project object and base scenario."""

    parent_territory = await get_common_territory_for_geometry(
        conn, project.project_territory_info.geometry.as_shapely_geometry()
    )

    statement_for_territory = (
        insert(projects_territory_data)
        .values(
            parent_territory_id=parent_territory.territory_id if parent_territory else None,
            geometry=ST_GeomFromText(str(project.project_territory_info.geometry.as_shapely_geometry()), text("4326")),
            centre_point=ST_GeomFromText(
                str(project.project_territory_info.centre_point.as_shapely_geometry()), text("4326")
            ),
            properties=project.project_territory_info.properties,
        )
        .returning(projects_territory_data.c.project_territory_id)
    )
    territory_id = (await conn.execute(statement_for_territory)).scalar_one()

    statement_for_project = (
        insert(projects_data)
        .values(
            user_id=user_id,
            name=project.name,
            project_territory_id=territory_id,
            description=project.description,
            public=project.public,
            image_url=project.image_url,
        )
        .returning(projects_data.c.project_id)
    )
    project_id = (await conn.execute(statement_for_project)).scalar_one()

    statement_for_scenario = (
        insert(scenarios_data)
        .values(project_id=project_id, name=f"base scenario for project with id={project_id}", properties={})
        .returning(scenarios_data.c.scenario_id)
    )
    scenario_id = (await conn.execute(statement_for_scenario)).scalar_one()

    await conn.commit()

    return await get_project_by_id_from_db(conn, project_id, user_id)


async def get_all_available_projects_from_db(conn: AsyncConnection, user_id: int) -> list[ProjectDTO]:
    """Get all public and user's projects."""

    statement = (
        select(projects_data)
        .where(or_(projects_data.c.user_id == user_id, projects_data.c.public.is_(True)))
        .order_by(projects_data.c.project_id)
    )
    results = (await conn.execute(statement)).mappings().all()

    return [ProjectDTO(**result) for result in results]


async def get_user_projects_from_db(conn: AsyncConnection, user_id: str) -> list[ProjectDTO]:
    """Get all user's projects."""

    statement = select(projects_data).where(projects_data.c.user_id == user_id).order_by(projects_data.c.project_id)
    results = (await conn.execute(statement)).mappings().all()

    return [ProjectDTO(**result) for result in results]


async def get_project_territory_by_id_from_db(
    conn: AsyncConnection, project_id: int, user_id: str
) -> ProjectTerritoryDTO:
    """Get project object by id."""

    statement_for_project = select(projects_data).where(projects_data.c.project_id == project_id)
    result_for_project = (await conn.execute(statement_for_project)).mappings().one_or_none()
    if result_for_project is None:
        raise EntityNotFoundById(project_id, "projects_data")
    elif result_for_project.user_id != user_id and result_for_project.public is False:
        raise AccessDeniedError(project_id, "projects_data")

    statement = select(
        projects_territory_data.c.project_territory_id,
        projects_territory_data.c.parent_territory_id,
        cast(ST_AsGeoJSON(projects_territory_data.c.geometry), JSONB).label("geometry"),
        cast(ST_AsGeoJSON(projects_territory_data.c.centre_point), JSONB).label("centre_point"),
        projects_territory_data.c.properties,
    ).where(projects_territory_data.c.project_territory_id == result_for_project.project_territory_id)

    result = (await conn.execute(statement)).mappings().one_or_none()
    if result is None:
        raise EntityNotFoundById(result_for_project.project_territory_id, "projects_territory_data")

    return ProjectTerritoryDTO(**result)


async def delete_project_from_db(conn: AsyncConnection, project_id: int, user_id: str) -> dict:
    """Delete project object."""

    statement = select(projects_data).where(projects_data.c.project_id == project_id)
    result = (await conn.execute(statement)).one_or_none()
    if result is None:
        raise EntityNotFoundById(project_id, "projects_data")
    elif result.user_id != user_id:
        raise AccessDeniedError(project_id, "projects_data")

    statement_for_project = delete(projects_data).where(projects_data.c.project_id == project_id)

    statement_for_territory = delete(projects_territory_data).where(
        projects_territory_data.c.project_territory_id == result.project_territory_id
    )

    await conn.execute(statement_for_project)
    await conn.execute(statement_for_territory)

    await conn.commit()

    return {"status": "ok"}


async def put_project_to_db(conn: AsyncConnection, project: ProjectPut, project_id: int, user_id: str) -> ProjectDTO:
    """Put project object."""

    statement = select(projects_data).where(projects_data.c.project_id == project_id)
    requested_project = (await conn.execute(statement)).one_or_none()
    if requested_project is None:
        raise EntityNotFoundById(project_id, "projects_data")
    elif requested_project.user_id != user_id:
        raise AccessDeniedError(project_id, "projects_data")

    statement_for_territory = (
        update(projects_territory_data)
        .where(projects_territory_data.c.project_territory_id == requested_project.project_territory_id)
        .values(
            parent_territory_id=project.project_territory_info.parent_territory_id,
            geometry=ST_GeomFromText(str(project.project_territory_info.geometry.as_shapely_geometry()), text("4326")),
            centre_point=ST_GeomFromText(
                str(project.project_territory_info.centre_point.as_shapely_geometry()), text("4326")
            ),
            properties=project.project_territory_info.properties,
        )
    )

    await conn.execute(statement_for_territory)

    statement = (
        update(projects_data)
        .where(projects_data.c.project_id == project_id)
        .values(
            user_id=user_id,
            name=project.name,
            description=project.description,
            public=project.public,
            image_url=project.image_url,
            updated_at=datetime.now(timezone.utc),
        )
        .returning(projects_data.c.project_id)
    )
    project_id = (await conn.execute(statement)).scalar_one_or_none()

    await conn.commit()

    return await get_project_by_id_from_db(conn, project_id, user_id)


async def patch_project_to_db(
    conn: AsyncConnection, project: ProjectPatch, project_id: int, user_id: str
) -> ProjectDTO:
    """Patch project object."""

    statement = select(projects_data).where(projects_data.c.project_id == project_id)
    requested_project = (await conn.execute(statement)).one_or_none()
    if requested_project is None:
        raise EntityNotFoundById(project_id, "projects_data")
    elif requested_project.user_id != user_id:
        raise AccessDeniedError(project_id, "projects_data")

    new_values_for_project = {}
    new_values_for_territory = {}

    for k, v in project.model_dump(exclude={"project_territory_info"}, exclude_unset=True).items():
        new_values_for_project.update({k: v})

    if project.project_territory_info is not None:
        for k, v in project.project_territory_info.model_dump(exclude_unset=True).items():
            if k == "geometry" and v is not None:
                new_values_for_territory["geometry"] = ST_GeomFromText(
                    str(project.project_territory_info.geometry.as_shapely_geometry()), text("4326")
                )
            elif k == "centre_point" and v is not None:
                new_values_for_territory["centre_point"] = ST_GeomFromText(
                    str(project.project_territory_info.centre_point.as_shapely_geometry()), text("4326")
                )
            else:
                new_values_for_territory[k] = v

    if new_values_for_project:
        statement_for_project = (
            update(projects_data)
            .where(projects_data.c.project_id == project_id)
            .values(updated_at=datetime.now(timezone.utc), **new_values_for_project)
            .returning(projects_data)
        )
        await conn.execute(statement_for_project)

    if new_values_for_territory:
        statement_for_territory = (
            update(projects_territory_data)
            .where(projects_territory_data.c.project_territory_id == requested_project.project_territory_id)
            .values(**new_values_for_territory)
        )
        await conn.execute(statement_for_territory)

    await conn.commit()

    return await get_project_by_id_from_db(conn, project_id, user_id)
