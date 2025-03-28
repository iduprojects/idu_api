"""Projects internal logic is defined here."""

import io
import os
import zipfile
from collections.abc import Callable
from datetime import date, datetime, timezone
from typing import Literal

import aiohttp
import structlog
from geoalchemy2 import Geography, Geometry
from geoalchemy2.functions import (
    ST_Area,
    ST_AsEWKB,
    ST_Buffer,
    ST_Centroid,
    ST_GeometryType,
    ST_GeomFromWKB,
    ST_Intersection,
    ST_Intersects,
    ST_IsEmpty,
    ST_Within,
)
from PIL import Image
from sqlalchemy import cast, delete, func, insert, literal, select, text, update
from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.common.db.entities import (
    functional_zones_data,
    object_geometries_data,
    physical_object_types_dict,
    physical_objects_data,
    projects_data,
    projects_functional_zones,
    projects_object_geometries_data,
    projects_physical_objects_data,
    projects_services_data,
    projects_territory_data,
    projects_urban_objects_data,
    scenarios_data,
    territories_data,
    urban_objects_data,
)
from idu_api.urban_api.config import UrbanAPIConfig
from idu_api.urban_api.dto import PageDTO, ProjectDTO, ProjectTerritoryDTO, ProjectWithTerritoryDTO, UserDTO
from idu_api.urban_api.exceptions.logic.common import EntityNotFoundById, EntityNotFoundByParams
from idu_api.urban_api.exceptions.logic.users import AccessDeniedError
from idu_api.urban_api.exceptions.utils.pillow import InvalidImageError
from idu_api.urban_api.logic.impl.helpers.utils import SRID, check_existence, extract_values_from_model
from idu_api.urban_api.schemas import (
    ProjectPatch,
    ProjectPost,
    ProjectPut,
)
from idu_api.urban_api.utils.minio_client import AsyncMinioClient
from idu_api.urban_api.utils.pagination import paginate_dto

func: Callable


async def check_project(conn: AsyncConnection, project_id: int, user: UserDTO | None, to_edit: bool = False) -> None:
    """Check project existence and user access."""

    statement = select(projects_data).where(projects_data.c.project_id == project_id)
    result = (await conn.execute(statement)).mappings().one_or_none()
    if result is None:
        raise EntityNotFoundById(project_id, "project")
    if user is None:
        if not result.public:
            raise AccessDeniedError(project_id, "project")
    elif result.user_id != user.id and (not result.public or to_edit) and not user.is_superuser:
        raise AccessDeniedError(project_id, "project")


async def get_project_by_id_from_db(conn: AsyncConnection, project_id: int, user: UserDTO | None) -> ProjectDTO:
    """Get project object by identifier."""

    statement = (
        select(
            projects_data,
            territories_data.c.name.label("territory_name"),
            scenarios_data.c.scenario_id,
            scenarios_data.c.name.label("scenario_name"),
        )
        .select_from(
            projects_data.join(
                territories_data,
                territories_data.c.territory_id == projects_data.c.territory_id,
            ).join(scenarios_data, scenarios_data.c.project_id == projects_data.c.project_id)
        )
        .where(projects_data.c.project_id == project_id, scenarios_data.c.is_based.is_(True))
    )
    result = (await conn.execute(statement)).mappings().one_or_none()

    if result is None:
        raise EntityNotFoundById(project_id, "project")
    if user is None:
        if not result.public:
            raise AccessDeniedError(project_id, "project")
    elif result.user_id != user.id and not result.public and not user.is_superuser:
        raise AccessDeniedError(project_id, "project")

    return ProjectDTO(**result)


async def get_project_territory_by_id_from_db(
    conn: AsyncConnection, project_id: int, user: UserDTO | None
) -> ProjectTerritoryDTO:
    """Get project territory object by project identifier."""

    await check_project(conn, project_id, user)

    statement = (
        select(
            projects_territory_data.c.project_territory_id,
            projects_data.c.project_id,
            projects_data.c.name.label("project_name"),
            projects_data.c.user_id.label("project_user_id"),
            territories_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
            ST_AsEWKB(projects_territory_data.c.geometry).label("geometry"),
            ST_AsEWKB(projects_territory_data.c.centre_point).label("centre_point"),
            projects_territory_data.c.properties,
            scenarios_data.c.scenario_id,
            scenarios_data.c.name.label("scenario_name"),
        )
        .select_from(
            projects_territory_data.join(
                projects_data, projects_data.c.project_id == projects_territory_data.c.project_id
            )
            .join(territories_data, territories_data.c.territory_id == projects_data.c.territory_id)
            .join(
                scenarios_data,
                scenarios_data.c.project_id == projects_data.c.project_id,
            )
        )
        .where(
            projects_territory_data.c.project_id == project_id,
            scenarios_data.c.is_based.is_(True),
        )
    )

    result = (await conn.execute(statement)).mappings().one_or_none()
    if result is None:
        raise EntityNotFoundById(project_id, "project territory")

    return ProjectTerritoryDTO(**result)


async def get_all_projects_from_db(conn: AsyncConnection) -> list[ProjectDTO]:
    """Get all available projects."""

    statement = (
        select(
            projects_data,
            territories_data.c.name.label("territory_name"),
            scenarios_data.c.scenario_id,
            scenarios_data.c.name.label("scenario_name"),
        )
        .select_from(
            projects_data.join(
                territories_data,
                territories_data.c.territory_id == projects_data.c.territory_id,
            ).join(scenarios_data, scenarios_data.c.project_id == projects_data.c.project_id)
        )
        .where(scenarios_data.c.is_based.is_(True), projects_data.c.is_regional.is_(False))
        .order_by(projects_data.c.project_id)
    )

    result = (await conn.execute(statement)).mappings().all()
    return [ProjectDTO(**item) for item in result]


async def get_projects_from_db(
    conn: AsyncConnection,
    user: UserDTO | None,
    only_own: bool,
    is_regional: bool,
    project_type: Literal["common", "city"] | None,
    territory_id: int | None,
    name: str | None,
    created_at: date | None,
    order_by: Literal["created_at", "updated_at"] | None,
    ordering: Literal["asc", "desc"] | None,
    paginate: bool = False,
) -> list[ProjectDTO] | PageDTO[ProjectDTO]:
    """Get all public and user's projects."""

    statement = (
        select(
            projects_data,
            territories_data.c.name.label("territory_name"),
            scenarios_data.c.scenario_id,
            scenarios_data.c.name.label("scenario_name"),
        )
        .select_from(
            projects_data.join(
                territories_data,
                territories_data.c.territory_id == projects_data.c.territory_id,
            ).join(scenarios_data, scenarios_data.c.project_id == projects_data.c.project_id)
        )
        .where(scenarios_data.c.is_based.is_(True), projects_data.c.is_regional.is_(is_regional))
    )

    if only_own:
        statement = statement.where(projects_data.c.user_id == user.id)
    elif user is not None:
        statement = statement.where(
            (projects_data.c.user_id == user.id) | (projects_data.c.public.is_(True) if not user.is_superuser else True)
        )
    else:
        statement = statement.where(projects_data.c.public.is_(True))

    if project_type == "common":
        statement = statement.where(projects_data.c.is_city.is_(False))
    elif project_type == "city":
        statement = statement.where(projects_data.c.is_city.is_(True))

    if territory_id is not None:
        statement = statement.where(projects_data.c.territory_id == territory_id)
    if name is not None:
        statement = statement.where(projects_data.c.name.ilike(f"%{name}%"))
    if created_at is not None:
        statement = statement.where(func.date(projects_data.c.created_at) >= created_at)

    if order_by is not None:
        order = projects_data.c.created_at if order_by == "created_at" else projects_data.c.updated_at
        if ordering == "desc":
            order = order.desc()
        statement = statement.order_by(order)
    else:
        if ordering == "desc":
            statement = statement.order_by(projects_data.c.project_id.desc())
        else:
            statement = statement.order_by(projects_data.c.project_id)

    if paginate:
        return await paginate_dto(conn, statement, transformer=lambda x: [ProjectDTO(**item) for item in x])

    result = (await conn.execute(statement)).mappings().all()

    return [ProjectDTO(**project) for project in result]


async def get_projects_territories_from_db(
    conn: AsyncConnection,
    user: UserDTO | None,
    only_own: bool,
    project_type: Literal["common", "city"] | None,
    territory_id: int | None,
) -> list[ProjectWithTerritoryDTO]:
    """Get all public and user's project territories."""

    statement = (
        select(
            projects_data,
            territories_data.c.name.label("territory_name"),
            ST_AsEWKB(projects_territory_data.c.geometry).label("geometry"),
            ST_AsEWKB(projects_territory_data.c.centre_point).label("centre_point"),
            scenarios_data.c.scenario_id,
            scenarios_data.c.name.label("scenario_name"),
        )
        .select_from(
            projects_data.join(territories_data, territories_data.c.territory_id == projects_data.c.territory_id)
            .join(scenarios_data, scenarios_data.c.project_id == projects_data.c.project_id)
            .join(projects_territory_data, projects_territory_data.c.project_id == projects_data.c.project_id)
        )
        .where(scenarios_data.c.is_based.is_(True), projects_data.c.is_regional.is_(False))
    )

    if only_own:
        statement = statement.where(projects_data.c.user_id == user.id)
    elif user is not None:
        statement = statement.where(
            (projects_data.c.user_id == user.id) | (projects_data.c.public.is_(True) if not user.is_superuser else True)
        )
    else:
        statement = statement.where(projects_data.c.public.is_(True))

    if project_type == "common":
        statement = statement.where(projects_data.c.is_city.is_(False))
    elif project_type == "city":
        statement = statement.where(projects_data.c.is_city.is_(True))

    if territory_id is not None:
        statement = statement.where(projects_data.c.territory_id == territory_id)

    result = (await conn.execute(statement)).mappings().all()

    return [ProjectWithTerritoryDTO(**project) for project in result]


async def get_preview_projects_images_from_minio(
    conn: AsyncConnection,
    minio_client: AsyncMinioClient,
    user: UserDTO | None,
    only_own: bool,
    is_regional: bool,
    project_type: Literal["common", "city"] | None,
    territory_id: int | None,
    name: str | None,
    created_at: date | None,
    order_by: Literal["created_at", "updated_at"] | None,
    ordering: Literal["asc", "desc"] | None,
    page: int,
    page_size: int,
    logger: structlog.stdlib.BoundLogger,
) -> io.BytesIO:
    """Get preview images for all public and user's projects with parallel MinIO requests."""

    statement = (
        select(projects_data.c.project_id)
        .where(projects_data.c.is_regional.is_(is_regional))
        .offset(page_size * (page - 1))
        .limit(page_size)
    )

    if only_own:
        statement = statement.where(projects_data.c.user_id == user.id)
    elif user is not None:
        statement = statement.where(
            (projects_data.c.user_id == user.id) | (projects_data.c.public.is_(True) if not user.is_superuser else True)
        )
    else:
        statement = statement.where(projects_data.c.public.is_(True))

    if project_type == "common":
        statement = statement.where(projects_data.c.is_city.is_(False))
    elif project_type == "city":
        statement = statement.where(projects_data.c.is_city.is_(True))

    if territory_id is not None:
        statement = statement.where(projects_data.c.territory_id == territory_id)
    if name is not None:
        statement = statement.where(projects_data.c.name.ilike(f"%{name}%"))
    if created_at is not None:
        statement = statement.where(func.date(projects_data.c.created_at) >= created_at)

    if order_by is not None:
        order = projects_data.c.created_at if order_by == "created_at" else projects_data.c.updated_at
        if ordering == "desc":
            order = order.desc()
        statement = statement.order_by(order)
    else:
        if ordering == "desc":
            statement = statement.order_by(projects_data.c.project_id.desc())
        else:
            statement = statement.order_by(projects_data.c.project_id)

    project_ids = (await conn.execute(statement)).scalars().all()

    results = await minio_client.get_files([f"projects/{project_id}/preview.jpg" for project_id in project_ids], logger)
    images = {project_id: image for project_id, image in zip(project_ids, results) if image is not None}

    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w") as zip_file:
        for project_id, image_stream in images.items():
            if image_stream:
                zip_file.writestr(f"preview_{project_id}.jpg", image_stream.read())
    zip_buffer.seek(0)

    return zip_buffer


async def get_preview_projects_images_url_from_minio(
    conn: AsyncConnection,
    minio_client: AsyncMinioClient,
    user: UserDTO | None,
    only_own: bool,
    is_regional: bool,
    project_type: Literal["common", "city"] | None,
    territory_id: int | None,
    name: str | None,
    created_at: date | None,
    order_by: Literal["created_at", "updated_at"] | None,
    ordering: Literal["asc", "desc"] | None,
    page: int,
    page_size: int,
    logger: structlog.stdlib.BoundLogger,
) -> list[dict[str, int | str]]:
    """Get preview images url for all public and user's projects."""

    statement = (
        select(projects_data.c.project_id)
        .where(projects_data.c.is_regional.is_(is_regional))
        .offset(page_size * (page - 1))
        .limit(page_size)
    )

    if only_own:
        statement = statement.where(projects_data.c.user_id == user.id)
    elif user is not None:
        statement = statement.where(
            (projects_data.c.user_id == user.id) | (projects_data.c.public.is_(True) if not user.is_superuser else True)
        )
    else:
        statement = statement.where(projects_data.c.public.is_(True))

    if project_type == "common":
        statement = statement.where(projects_data.c.is_city.is_(False))
    elif project_type == "city":
        statement = statement.where(projects_data.c.is_city.is_(True))

    if territory_id is not None:
        statement = statement.where(projects_data.c.territory_id == territory_id)
    if name is not None:
        statement = statement.where(projects_data.c.name.ilike(f"%{name}%"))
    if created_at is not None:
        statement = statement.where(func.date(projects_data.c.created_at) >= created_at)

    if order_by is not None:
        order = projects_data.c.created_at if order_by == "created_at" else projects_data.c.updated_at
        if ordering == "desc":
            order = order.desc()
        statement = statement.order_by(order)
    else:
        if ordering == "desc":
            statement = statement.order_by(projects_data.c.project_id.desc())
        else:
            statement = statement.order_by(projects_data.c.project_id)

    project_ids = (await conn.execute(statement)).scalars().all()

    results = await minio_client.generate_presigned_urls(
        [f"projects/{project_id}/preview.jpg" for project_id in project_ids], logger
    )

    return [{"project_id": project_id, "url": url} for project_id, url in zip(project_ids, results) if url is not None]


async def get_user_projects_from_db(
    conn: AsyncConnection, user: UserDTO, is_regional: bool, territory_id: int | None
) -> PageDTO[ProjectDTO]:
    """Get all user's projects."""

    statement = (
        select(
            projects_data,
            territories_data.c.name.label("territory_name"),
            scenarios_data.c.scenario_id,
            scenarios_data.c.name.label("scenario_name"),
        )
        .select_from(
            projects_data.join(
                territories_data,
                territories_data.c.territory_id == projects_data.c.territory_id,
            ).join(scenarios_data, scenarios_data.c.project_id == projects_data.c.project_id)
        )
        .where(
            scenarios_data.c.is_based.is_(True),
            projects_data.c.is_regional.is_(is_regional),
            projects_data.c.user_id == user.id,
        )
        .order_by(projects_data.c.project_id)
    )

    if territory_id is not None:
        statement = statement.where(projects_data.c.territory_id == territory_id)

    return await paginate_dto(conn, statement, transformer=lambda x: [ProjectDTO(**item) for item in x])


async def get_user_preview_projects_images_from_minio(
    conn: AsyncConnection,
    minio_client: AsyncMinioClient,
    user: UserDTO,
    is_regional: bool,
    territory_id: int | None,
    page: int,
    page_size: int,
    logger: structlog.stdlib.BoundLogger,
) -> io.BytesIO:
    """Get preview images for all user's projects with parallel MinIO requests."""

    statement = (
        select(projects_data.c.project_id)
        .where(projects_data.c.user_id == user.id, projects_data.c.is_regional.is_(is_regional))
        .order_by(projects_data.c.project_id)
        .offset(page_size * (page - 1))
        .limit(page_size)
    )
    if territory_id is not None:
        statement = statement.where(projects_data.c.territory_id == territory_id)
    project_ids = (await conn.execute(statement)).scalars().all()

    results = await minio_client.get_files([f"projects/{project_id}/preview.jpg" for project_id in project_ids], logger)
    images = {project_id: image for project_id, image in zip(project_ids, results) if image is not None}

    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w") as zip_file:
        for project_id, image_stream in images.items():
            if image_stream:
                zip_file.writestr(f"preview_{project_id}.jpg", image_stream.read())
    zip_buffer.seek(0)

    return zip_buffer


async def get_user_preview_projects_images_url_from_minio(
    conn: AsyncConnection,
    minio_client: AsyncMinioClient,
    user: UserDTO,
    is_regional: bool,
    territory_id: int | None,
    page: int,
    page_size: int,
    logger: structlog.stdlib.BoundLogger,
) -> list[dict[str, int | str]]:
    """Get preview images url for all user's projects."""

    statement = (
        select(projects_data.c.project_id)
        .where(projects_data.c.user_id == user.id, projects_data.c.is_regional.is_(is_regional))
        .order_by(projects_data.c.project_id)
        .offset(page_size * (page - 1))
        .limit(page_size)
    )
    if territory_id is not None:
        statement = statement.where(projects_data.c.territory_id == territory_id)
    project_ids = (await conn.execute(statement)).scalars().all()

    results = await minio_client.generate_presigned_urls(
        [f"projects/{project_id}/preview.jpg" for project_id in project_ids], logger
    )

    return [{"project_id": project_id, "url": url} for project_id, url in zip(project_ids, results) if url is not None]


async def add_project_to_db(
    conn: AsyncConnection, project: ProjectPost, user: UserDTO, logger: structlog.stdlib.BoundLogger
) -> ProjectDTO:
    """Create project object, its territory and base scenario."""

    given_geometry = select(
        ST_GeomFromWKB(project.territory.geometry.as_shapely_geometry().wkb, text(str(SRID))).label("geometry")
    ).scalar_subquery()

    if not project.is_regional:
        if not await check_existence(conn, territories_data, conditions={"territory_id": project.territory_id}):
            raise EntityNotFoundById(project.territory_id, "territory")
        buffer_meters = 3000
        buffered_project_geometry = select(
            cast(
                ST_Buffer(
                    cast(
                        given_geometry,
                        Geography(srid=SRID),
                    ),
                    buffer_meters,
                ),
                Geometry(srid=SRID),
            ).label("geometry")
        ).scalar_subquery()

        base_cte = (
            select(
                territories_data.c.territory_id,
                territories_data.c.name,
                territories_data.c.is_city,
                territories_data.c.level,
                territories_data.c.geometry,
            )
            .where(territories_data.c.territory_id == project.territory_id)
            .cte(name="territories_cte", recursive=True)
        )
        recursive_query = select(
            territories_data.c.territory_id,
            territories_data.c.name,
            territories_data.c.is_city,
            territories_data.c.level,
            territories_data.c.geometry,
        ).where(territories_data.c.parent_id == base_cte.c.territory_id)
        territories_cte = base_cte.union_all(recursive_query)
        cities_level = (
            select(territories_cte.c.level)
            .where(territories_cte.c.is_city.is_(True))
            .order_by(territories_cte.c.level.desc())
            .limit(1)
            .cte(name="cities_level")
        )
        intersecting_territories = select(territories_cte.c.name).where(
            territories_cte.c.level == (cities_level.c.level - 1),
            ST_Intersects(territories_cte.c.geometry, given_geometry),
        )
        intersecting_district = select(territories_cte.c.name).where(
            territories_cte.c.level == (cities_level.c.level - 2),
            ST_Intersects(territories_cte.c.geometry, given_geometry),
        )
        context_territories = select(territories_cte.c.territory_id).where(
            territories_cte.c.level == (cities_level.c.level - 1),
            ST_Intersects(territories_cte.c.geometry, buffered_project_geometry),
        )
        project.properties.update(
            {
                "territories": list((await conn.execute(intersecting_territories)).scalars().all()),
                "districts": list((await conn.execute(intersecting_district)).scalars().all()),
                "context": list((await conn.execute(context_territories)).scalars().all()),
            }
        )

    statement_for_project = (
        insert(projects_data)
        .values(**project.model_dump(exclude={"territory"}), user_id=user.id)
        .returning(projects_data.c.project_id)
    )
    project_id = (await conn.execute(statement_for_project)).scalar_one()

    project_territory = extract_values_from_model(project.territory)
    await conn.execute(insert(projects_territory_data).values(**project_territory, project_id=project_id))

    # fixme: use the real parent scenario identifier from the user
    #  instead of using the basic regional scenario by default
    parent_scenario_id = None
    if not project.is_regional:
        parent_scenario_id = (
            await conn.execute(
                select(scenarios_data.c.scenario_id)
                .select_from(
                    scenarios_data.join(projects_data, projects_data.c.project_id == scenarios_data.c.project_id)
                )
                .where(
                    projects_data.c.territory_id == project.territory_id,
                    projects_data.c.is_regional.is_(True),
                    scenarios_data.c.is_based.is_(True),
                )
            )
        ).scalar_one_or_none()
        if parent_scenario_id is None:
            raise EntityNotFoundByParams("parent scenario", project.territory_id)

    scenario_id = (
        await conn.execute(
            insert(scenarios_data)
            .values(
                project_id=project_id,
                functional_zone_type_id=None,
                name="Исходный пользовательский сценарий",
                is_based=True,
                parent_id=parent_scenario_id,
            )
            .returning(scenarios_data.c.scenario_id)
        )
    ).scalar_one()

    # TODO: get geometries and urban objects from regional scenario

    # 1. Find all objects for territories from the first step (partially included at given percent)
    # where the geometry is not completely included in the passed
    area_percent = 0.01
    objects_intersecting = (
        select(object_geometries_data.c.object_geometry_id)
        .select_from(
            object_geometries_data.join(
                urban_objects_data,
                urban_objects_data.c.object_geometry_id == object_geometries_data.c.object_geometry_id,
            )
            .join(
                physical_objects_data,
                physical_objects_data.c.physical_object_id == urban_objects_data.c.physical_object_id,
            )
            .join(
                physical_object_types_dict,
                physical_object_types_dict.c.physical_object_type_id == physical_objects_data.c.physical_object_type_id,
            )
        )
        .where(
            ST_Intersects(object_geometries_data.c.geometry, given_geometry),
            ~ST_Within(object_geometries_data.c.geometry, given_geometry),
            ST_Area(ST_Intersection(object_geometries_data.c.geometry, given_geometry))
            >= area_percent * ST_Area(object_geometries_data.c.geometry),
            physical_object_types_dict.c.physical_object_function_id != 1,  # fixme: remove hardcode to skip buildings
        )
        .distinct()
    ).cte("objects_intersecting")

    # 2. Crop and insert geometries from the first step
    insert_geometries = (
        insert(projects_object_geometries_data)
        .from_select(
            [
                projects_object_geometries_data.c.public_object_geometry_id,
                projects_object_geometries_data.c.territory_id,
                projects_object_geometries_data.c.geometry,
                projects_object_geometries_data.c.centre_point,
                projects_object_geometries_data.c.address,
                projects_object_geometries_data.c.osm_id,
            ],
            select(
                object_geometries_data.c.object_geometry_id.label("public_object_geometry_id"),
                object_geometries_data.c.territory_id,
                ST_Intersection(object_geometries_data.c.geometry, given_geometry).label("geometry"),
                ST_Centroid(ST_Intersection(object_geometries_data.c.geometry, given_geometry)).label("centre_point"),
                object_geometries_data.c.address,
                object_geometries_data.c.osm_id,
            ).where(object_geometries_data.c.object_geometry_id.in_(select(objects_intersecting))),
        )
        .returning(
            projects_object_geometries_data.c.object_geometry_id,
            projects_object_geometries_data.c.public_object_geometry_id,
        )
    )
    inserted_geometries = (await conn.execute(insert_geometries)).mappings().all()
    id_mapping = {row.public_object_geometry_id: row.object_geometry_id for row in inserted_geometries}

    # 3. Insert cropped geometries from the second step to `projects_urban_objects_data`
    urban_objects_select = select(
        urban_objects_data.c.urban_object_id.label("public_urban_object_id"),
        urban_objects_data.c.physical_object_id.label("public_physical_object_id"),
        urban_objects_data.c.service_id.label("public_service_id"),
        urban_objects_data.c.object_geometry_id,
    ).where(urban_objects_data.c.object_geometry_id.in_(select(objects_intersecting)))
    urban_objects = (await conn.execute(urban_objects_select)).mappings().all()
    if urban_objects:
        await conn.execute(
            insert(projects_urban_objects_data).values(
                [
                    {
                        "public_urban_object_id": row.public_urban_object_id,
                        "scenario_id": scenario_id,
                    }
                    for row in urban_objects
                ]
            )
        )
        await conn.execute(
            insert(projects_urban_objects_data).values(
                [
                    {
                        "public_physical_object_id": row.public_physical_object_id,
                        "public_service_id": row.public_service_id,
                        "object_geometry_id": id_mapping[row.object_geometry_id],
                        "scenario_id": scenario_id,
                    }
                    for row in urban_objects
                ]
            )
        )

    # 4. Find all functional zones for the project
    # TODO: If we get at least one urban object from regional scenario
    # or we haven't got any functional zones for the project territory from public schema,
    # it's needed to generate new functional zones else get it from public schema
    if not project.is_regional:
        statement = select(
            literal(scenario_id).label("scenario_id"),
            functional_zones_data.c.functional_zone_type_id,
            ST_Intersection(functional_zones_data.c.geometry, given_geometry).label("geometry"),
            functional_zones_data.c.year,
            functional_zones_data.c.source,
            functional_zones_data.c.properties,
        ).where(
            functional_zones_data.c.territory_id.in_(select(territories_data.c.territory_id)),
            ST_Intersects(functional_zones_data.c.geometry, given_geometry),
            ST_GeometryType(ST_Intersection(functional_zones_data.c.geometry, given_geometry)).in_(
                ("ST_Polygon", "ST_MultiPolygon")
            ),
            ~ST_IsEmpty(ST_Intersection(functional_zones_data.c.geometry, given_geometry)),
        )
        zones = (await conn.execute(statement)).mappings().all()
        if zones:
            await conn.execute(
                insert(projects_functional_zones).from_select(
                    [
                        projects_functional_zones.c.scenario_id,
                        projects_functional_zones.c.functional_zone_type_id,
                        projects_functional_zones.c.geometry,
                        projects_functional_zones.c.year,
                        projects_functional_zones.c.source,
                        projects_functional_zones.c.properties,
                    ],
                    statement,
                )
            )

    await conn.commit()

    config = UrbanAPIConfig.from_file_or_default(os.getenv("CONFIG_PATH"))

    if not project.is_regional:
        async with aiohttp.ClientSession() as session:
            params = {"scenario_id": scenario_id, "project_id": project_id, "background": "true"}
            try:
                response = await session.put(
                    f"{config.external.hextech_api}/hextech/indicators_saving/save_all",
                    params=params,
                )
                response.raise_for_status()
            except aiohttp.ClientResponseError as exc:
                await logger.awarning(
                    "failed to save indicators",
                    status=exc.status,
                    message=exc.message,
                    url=exc.request_info.url,
                    params=params,
                )
            except aiohttp.ClientConnectorError as exc:
                await logger.awarning("request failed", reason=str(exc), params=params)
            except Exception:  # pylint: disable=broad-except
                await logger.aexception("unexpected error occurred")

    return await get_project_by_id_from_db(conn, project_id, user)


async def put_project_to_db(conn: AsyncConnection, project: ProjectPut, project_id: int, user: UserDTO) -> ProjectDTO:
    """Update project object by all its attributes."""

    await check_project(conn, project_id, user, to_edit=True)

    statement = (
        update(projects_data)
        .where(projects_data.c.project_id == project_id)
        .values(**extract_values_from_model(project, to_update=True))
    )

    await conn.execute(statement)
    await conn.commit()

    return await get_project_by_id_from_db(conn, project_id, user)


async def patch_project_to_db(
    conn: AsyncConnection, project: ProjectPatch, project_id: int, user: UserDTO
) -> ProjectDTO:
    """Patch project object."""

    await check_project(conn, project_id, user, to_edit=True)

    statement_for_project = (
        update(projects_data)
        .where(projects_data.c.project_id == project_id)
        .values(**project.model_dump(exclude_unset=True), updated_at=datetime.now(timezone.utc))
        .returning(projects_data)
    )

    await conn.execute(statement_for_project)
    await conn.commit()

    return await get_project_by_id_from_db(conn, project_id, user)


async def delete_project_from_db(
    conn: AsyncConnection,
    project_id: int,
    minio_client: AsyncMinioClient,
    user: UserDTO,
    logger: structlog.stdlib.BoundLogger,
) -> dict:
    """Delete project object."""

    await check_project(conn, project_id, user, to_edit=True)

    urban_objects = (
        (
            await conn.execute(
                select(
                    projects_urban_objects_data.c.object_geometry_id,
                    projects_urban_objects_data.c.physical_object_id,
                    projects_urban_objects_data.c.service_id,
                )
                .select_from(
                    projects_urban_objects_data.join(
                        scenarios_data,
                        scenarios_data.c.scenario_id == projects_urban_objects_data.c.scenario_id,
                    )
                )
                .where(scenarios_data.c.project_id == project_id)
            )
        )
        .mappings()
        .all()
    )

    geometry_ids = {obj.object_geometry_id for obj in urban_objects if obj.object_geometry_id is not None}
    physical_ids = {obj.physical_object_id for obj in urban_objects if obj.physical_object_id is not None}
    service_ids = {obj.service_id for obj in urban_objects if obj.service_id is not None}

    if geometry_ids:
        delete_geometry_statement = delete(projects_object_geometries_data).where(
            projects_object_geometries_data.c.object_geometry_id.in_(geometry_ids)
        )
        await conn.execute(delete_geometry_statement)

    if physical_ids:
        delete_physical_statement = delete(projects_physical_objects_data).where(
            projects_physical_objects_data.c.physical_object_id.in_(physical_ids)
        )
        await conn.execute(delete_physical_statement)

    if service_ids:
        delete_service_statement = delete(projects_services_data).where(
            projects_services_data.c.service_id.in_(service_ids)
        )
        await conn.execute(delete_service_statement)

    statement_for_project = delete(projects_data).where(projects_data.c.project_id == project_id)

    await conn.execute(statement_for_project)
    await minio_client.delete_file(f"projects/{project_id}/", logger)

    await conn.commit()

    return {"status": "ok"}


async def upload_project_image_to_minio(
    conn: AsyncConnection,
    minio_client: AsyncMinioClient,
    project_id: int,
    user: UserDTO,
    file: bytes,
    logger: structlog.stdlib.BoundLogger,
) -> dict[str, str]:
    """Upload original and resized project image to minio bucket."""

    await check_project(conn, project_id, user, to_edit=True)

    try:
        image = Image.open(io.BytesIO(file))
    except Exception as exc:
        raise InvalidImageError(project_id) from exc

    # Convert RGBA to RGB for JPEG compatibility
    if image.mode == "RGBA":
        image = image.convert("RGB")

    preview_image = image.copy()

    # Resize main image to max 1600px on the larger side
    width, height = image.size
    max_dimension = 1600

    if width > max_dimension or height > max_dimension:
        ratio = min(max_dimension / width, max_dimension / height)
        new_size = (int(width * ratio), int(height * ratio))
        preview_image = preview_image.resize(new_size, Image.Resampling.LANCZOS)

    # Save and prepare for upload
    image_stream = io.BytesIO()
    image.save(image_stream, format="JPEG")
    image_stream.seek(0)

    preview_stream = io.BytesIO()
    preview_image.save(preview_stream, format="JPEG", quality=85)
    preview_stream.seek(0)

    # Upload to MinIO
    await minio_client.upload_file(image_stream.getvalue(), f"projects/{project_id}/image.jpg", logger)
    await minio_client.upload_file(preview_stream.getvalue(), f"projects/{project_id}/preview.jpg", logger)

    # Generate URLs
    image_url, preview_url = await minio_client.generate_presigned_urls(
        [f"projects/{project_id}/image.jpg", f"projects/{project_id}/preview.jpg"], logger
    )

    return {
        "image_url": image_url,
        "preview_url": preview_url,
    }


async def get_project_image_from_minio(
    conn: AsyncConnection,
    minio_client: AsyncMinioClient,
    project_id: int,
    user: UserDTO | None,
    image_type: Literal["origin", "preview"],
    logger: structlog.stdlib.BoundLogger,
) -> io.BytesIO:
    """Get full image for given project."""

    await check_project(conn, project_id, user)

    object_name = f"projects/{project_id}/image.jpg" if image_type == "origin" else f"projects/{project_id}/preview.jpg"

    return (await minio_client.get_files([object_name], logger))[0]


async def get_project_image_url_from_minio(
    conn: AsyncConnection,
    minio_client: AsyncMinioClient,
    project_id: int,
    user: UserDTO | None,
    image_type: Literal["origin", "preview"],
    logger: structlog.stdlib.BoundLogger,
) -> str:
    """Get full image url for given project."""

    await check_project(conn, project_id, user)

    object_name = f"projects/{project_id}/image.jpg" if image_type == "origin" else f"projects/{project_id}/preview.jpg"

    return (await minio_client.generate_presigned_urls([object_name], logger))[0]
