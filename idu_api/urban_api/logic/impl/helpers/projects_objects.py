"""Projects internal logic is defined here."""

import io
import os
import zipfile
from collections.abc import Callable
from datetime import date, datetime, timezone
from typing import Literal, Any

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
from sqlalchemy import cast, delete, func, insert, literal, select, text, update, ScalarSelect
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


####################################################################################
#                           Main business-logic                                    #
####################################################################################


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

    given_geometry = select(
        ST_GeomFromWKB(project.territory.geometry.as_shapely_geometry().wkb, text(str(SRID))).label("geometry")
    ).scalar_subquery()

    await add_context_territories(conn, project, given_geometry)

    statement = (
        insert(projects_data)
        .values(**project.model_dump(exclude={"territory"}), user_id=user.id)
        .returning(projects_data.c.project_id)
    )
    project_id =  (await conn.execute(statement)).scalar_one()

    project_territory = extract_values_from_model(project.territory)
    await conn.execute(insert(projects_territory_data).values(**project_territory, project_id=project_id))

    scenario_id = await create_project_base_scenario(conn, project_id, project.territory_id)

    id_mapping = await insert_intersecting_geometries(conn, given_geometry)

    await insert_urban_objects(conn, scenario_id, id_mapping)

    await insert_functional_zones(conn, scenario_id, given_geometry)

    await save_indicators(project_id, scenario_id, logger)

    await conn.commit()

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


####################################################################################
#                            Helper functions                                      #
####################################################################################


async def add_context_territories(
    conn: AsyncConnection,
    project: ProjectPost,
    geometry: ScalarSelect[Any],
) -> None:
    """
    Update project properties with additional territorial context:
    - Add a list of intersecting districts (e.g., municipalities),
    - Add the nearest territories (e.g., city neighborhoods),
    - Add context territories within a buffer zone (e.g., nearby areas).
    """

    # Check if the given territory exists in the database; raise an error if it doesn't.
    if not await check_existence(conn, territories_data, conditions={"territory_id": project.territory_id}):
        raise EntityNotFoundById(project.territory_id, "territory")

    # Define a 3000-meter buffer zone around the project's geometry to find nearby (context) territories.
    buffer_meters = 3000
    buffered_project_geometry = select(
        cast(
            ST_Buffer(
                cast(
                    geometry,
                    Geography(srid=SRID), # Convert geometry to geography for accurate distance-based buffering.
                ),
                buffer_meters, # Distance of buffer in meters.
            ),
            Geometry(srid=SRID), # Cast back to geometry type with SRID.
        ).label("geometry")
    ).scalar_subquery()

    # Create a recursive CTE to collect the hierarchy of territories starting from the project's territory.
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

    # Recursive part: include all children territories of the current territory in the hierarchy.
    recursive_query = select(
        territories_data.c.territory_id,
        territories_data.c.name,
        territories_data.c.is_city,
        territories_data.c.level,
        territories_data.c.geometry,
    ).where(territories_data.c.parent_id == base_cte.c.territory_id)

    # Union base with recursive to get full hierarchy of territories under the current one.
    territories_cte = base_cte.union_all(recursive_query)

    # Determine the highest city-level (e.g., city) in the territory hierarchy.
    cities_level = (
        select(territories_cte.c.level)
        .where(territories_cte.c.is_city.is_(True))
        .order_by(territories_cte.c.level.desc())
        .limit(1)
        .cte(name="cities_level")
    )

    # Get names of territories one level below the city that intersect with the project geometry (e.g., neighborhoods).
    intersecting_territories = select(territories_cte.c.name).where(
        territories_cte.c.level == (cities_level.c.level - 1),
        ST_Intersects(territories_cte.c.geometry, geometry),
    )

    # Get names of territories two levels below the city that intersect with the project geometry (e.g., districts).
    intersecting_district = select(territories_cte.c.name).where(
        territories_cte.c.level == (cities_level.c.level - 2),
        ST_Intersects(territories_cte.c.geometry, geometry),
    )

    # Get IDs of territories one level below the city that intersect with the buffered (3 km) geometry (context areas).
    context_territories = select(territories_cte.c.territory_id).where(
        territories_cte.c.level == (cities_level.c.level - 1),
        ST_Intersects(territories_cte.c.geometry, buffered_project_geometry),
        territories_cte.c.is_city.is_(False),
    )

    # Update the project’s properties dictionary with gathered information.
    project.properties.update(
        {
            "territories": list((await conn.execute(intersecting_territories)).scalars().all()),
            "districts": list((await conn.execute(intersecting_district)).scalars().all()),
            "context": list((await conn.execute(context_territories)).scalars().all()),
        }
    )


async def create_project_base_scenario(
    conn: AsyncConnection,
    project_id: int,
    territory_id: int,
    parent_id: int | None = None,
) -> int:
    """
    Create a base scenario for a given project.

    If `parent_id` is not provided, the function will attempt to find a regional base scenario
    associated with the same territory. If no such scenario exists, an error will be raised.

    Args:
        conn (AsyncConnection): Active database connection.
        project_id (int): ID of the current project.
        territory_id (int): ID of the territory the scenario is related to.
        parent_id (int | None): Optional ID of the parent (regional) scenario.

    Returns:
        int: ID of the newly created scenario.

    Raises:
        EntityNotFoundByParams: If no regional base scenario is found when expected.
    """

    # If no parent scenario ID is explicitly provided, try to find the appropriate one.
    if parent_id is None:
        # Look for an existing regional base scenario for the same territory.
        parent_id = (
            await conn.execute(
                select(scenarios_data.c.scenario_id)
                .select_from(
                    scenarios_data.join(projects_data, projects_data.c.project_id == scenarios_data.c.project_id)
                )
                .where(
                    projects_data.c.territory_id == territory_id,
                    projects_data.c.is_regional.is_(True),  # Regional projects only.
                    scenarios_data.c.is_based.is_(True),    # Must be a base scenario.
                )
            )
        ).scalar_one_or_none()

        # If no parent scenario is found, raise an informative error.
        if parent_id is None:
            raise EntityNotFoundByParams("parent regional scenario", territory_id)

    # Insert a new base scenario for the user project with the found or given parent.
    scenario_id = (
        await conn.execute(
            insert(scenarios_data)
            .values(
                project_id=project_id,
                functional_zone_type_id=None,  # Base scenario doesn't have a predefined profile type.
                name="Исходный пользовательский сценарий",  # Default name (in Russian).
                is_based=True,
                parent_id=parent_id,
            )
            .returning(scenarios_data.c.scenario_id)
        )
    ).scalar_one()

    # Return the ID of the newly created scenario.
    return scenario_id


async def insert_intersecting_geometries(conn: AsyncConnection, geometry: ScalarSelect[Any]) -> dict[int, int]:
    """
    Identify geometries that intersect the given geometry, crop them,
    and insert the resulting geometries into the `user_projects` schema.

    The function filters geometries that intersect (but are not fully within) the given geometry
    and have an overlapping area greater than a minimum threshold. It excludes building-type objects.
    After inserting the cropped geometries, it returns a mapping from the public object geometry IDs
    to their corresponding new project-specific geometry IDs.

    Args:
        conn (AsyncConnection): Active asynchronous database connection.
        geometry (ScalarSelect[Any]): Geometry used as a mask to crop intersecting geometries.

    Returns:
        dict[int, int]: A mapping of public object geometry IDs to inserted project object geometry IDs.
    """

    area_percent = 0.01  # Minimum intersection threshold as a fraction of original area.

    # Step 1: Identify object geometries that intersect the given geometry
    # but are not fully within it, and have at least 1% overlapping area.
    # Also exclude objects of type "building".
    objects_intersecting_cte = (
        select(object_geometries_data.c.object_geometry_id)
        .select_from(
            object_geometries_data
            .join(urban_objects_data, urban_objects_data.c.object_geometry_id == object_geometries_data.c.object_geometry_id)
            .join(physical_objects_data, physical_objects_data.c.physical_object_id == urban_objects_data.c.physical_object_id)
            .join(physical_object_types_dict, physical_object_types_dict.c.physical_object_type_id == physical_objects_data.c.physical_object_type_id)
        )
        .where(
            ST_Intersects(object_geometries_data.c.geometry, geometry),
            ~ST_Within(object_geometries_data.c.geometry, geometry),
            ST_Area(ST_Intersection(object_geometries_data.c.geometry, geometry)) >=
                area_percent * ST_Area(object_geometries_data.c.geometry),
            ~physical_object_types_dict.c.name.ilike("%здание%"),  # Exclude buildings
        )
        .distinct()
        .cte("objects_intersecting")
    )

    # Step 2: Crop geometries using the given geometry and insert the cropped
    # versions into the `user_projects.object_geometries_data` table.
    insert_stmt = (
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
                ST_Intersection(object_geometries_data.c.geometry, geometry).label("geometry"),
                ST_Centroid(ST_Intersection(object_geometries_data.c.geometry, geometry)).label("centre_point"),
                object_geometries_data.c.address,
                object_geometries_data.c.osm_id,
            ).where(object_geometries_data.c.object_geometry_id.in_(select(objects_intersecting_cte)))
        )
        .returning(
            projects_object_geometries_data.c.object_geometry_id,
            projects_object_geometries_data.c.public_object_geometry_id,
        )
    )

    # Step 3: Execute the insertion and build a mapping of public IDs to new project-specific IDs.
    result = await conn.execute(insert_stmt)
    return {
        row.public_object_geometry_id: row.object_geometry_id
        for row in result.mappings().all()
    }


async def insert_urban_objects(conn: AsyncConnection, scenario_id: int, id_mapping: dict[int, int]) -> None:
    """
    Insert cropped urban objects into the `user_projects.urban_objects_data` table for a specific scenario.

    This function does the following:
    1. Selects urban objects related to the provided geometry mapping.
    2. Inserts both public and detailed data using `INSERT FROM SELECT` directly,
       applying the provided geometry ID mapping in a SQL-compliant way.

    Args:
        conn (AsyncConnection): Asynchronous database connection.
        scenario_id (int): The scenario to which the urban objects should be linked.
        id_mapping (dict[int, int]): Mapping from original object_geometry_id to newly inserted project-specific geometry_id.
    """

    if not id_mapping:
        return

    # Step 1: Common base query for selecting matched urban objects and joining mapping
    base_query_cte = (
        select(
            urban_objects_data.c.urban_object_id.label("public_urban_object_id"),
            urban_objects_data.c.physical_object_id.label("public_physical_object_id"),
            urban_objects_data.c.service_id.label("public_service_id"),
            projects_object_geometries_data.c.object_geometry_id.label("object_geometry_id"),
        )
        .select_from(
            urban_objects_data.join(
                projects_object_geometries_data,
                urban_objects_data.c.object_geometry_id == projects_object_geometries_data.c.public_object_geometry_id
            )
        )
        .where(urban_objects_data.c.object_geometry_id.in_(id_mapping.keys()))
    ).cte(name="base_query")

    # Step 2: Insert basic public urban object references
    insert_public = insert(projects_urban_objects_data).from_select(
        ["public_urban_object_id", "scenario_id"],
        select(
            base_query_cte.c.public_urban_object_id,
            literal(scenario_id).label("scenario_id"),
        )
    )

    # Step 3: Insert detailed urban object data with mapped geometries
    insert_detailed = insert(projects_urban_objects_data).from_select(
        ["public_physical_object_id", "public_service_id", "object_geometry_id", "scenario_id"],
        select(
            base_query_cte.c.public_physical_object_id,
            base_query_cte.c.public_service_id,
            base_query_cte.c.object_geometry_id,
            literal(scenario_id).label("scenario_id")
        )
    )

    # Execute inserts
    await conn.execute(insert_public)
    await conn.execute(insert_detailed)


async def insert_functional_zones(conn: AsyncConnection, scenario_id: int, geometry: ScalarSelect[Any]) -> None:
    """
    Insert functional zones intersecting the given geometry into the specified scenario.

    This function performs the following steps:
    1. Finds functional zones from the `functional_zones_data` table that intersect with the given geometry.
    2. Computes the intersection geometry using ST_Intersection.
    3. Filters out invalid geometries (empty or non-polygonal).
    4. Inserts the resulting intersected zones into the `projects_functional_zones` table for the specified scenario.

    Args:
        conn (AsyncConnection): Asynchronous database connection.
        scenario_id (int): Target scenario ID to associate the functional zones with.
        geometry (ScalarSelect[Any]): A subquery returning the project's geometry to intersect with.
    """

    # Step 1: Define the SELECT statement to fetch intersecting functional zones
    intersecting_zones_select = (
        select(
            literal(scenario_id).label("scenario_id"),
            functional_zones_data.c.functional_zone_type_id,
            ST_Intersection(functional_zones_data.c.geometry, geometry).label("geometry"),
            functional_zones_data.c.year,
            functional_zones_data.c.source,
            functional_zones_data.c.properties,
        )
        .where(
            # Must intersect with the given geometry
            ST_Intersects(functional_zones_data.c.geometry, geometry),

            # Filter only valid polygonal geometries
            ST_GeometryType(ST_Intersection(functional_zones_data.c.geometry, geometry)).in_(
                ("ST_Polygon", "ST_MultiPolygon")
            ),

            # Ensure geometry is not empty
            ~ST_IsEmpty(ST_Intersection(functional_zones_data.c.geometry, geometry)),
        )
    )

    # Step 2: Insert results into the `user_projects.functional_zones` table using INSERT FROM SELECT
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
            intersecting_zones_select
        )
    )


async def save_indicators(project_id: int, scenario_id: int, logger: structlog.stdlib.BoundLogger) -> None:
    """
    Update all indicators for the specified project via the external Hextech service.

    This function interacts with the Hextech API to save indicators related to a specific project and scenario.
    It handles different exceptions and logs relevant information in case of errors.

    Args:
        project_id (int): The ID of the project for which indicators are being saved.
        scenario_id (int): The ID of the scenario within the project.
        logger (structlog.stdlib.BoundLogger): The logger used to record warning and error logs.
    """

    # Load configuration settings from a file or default environment variables
    config = UrbanAPIConfig.from_file_or_default(os.getenv("CONFIG_PATH"))
    params = {"scenario_id": scenario_id, "project_id": project_id, "background": "true"}

    # Create an asynchronous HTTP client session
    async with aiohttp.ClientSession() as session:
        try:
            response = await session.put(
                f"{config.external.hextech_api}/hextech/indicators_saving/save_all", params=params
            )
            response.raise_for_status()

        # Handle errors related to the response (e.g., 4xx or 5xx errors)
        except aiohttp.ClientResponseError as exc:
            await logger.awarning(
                "Failed to save indicators",
                status=exc.status, message=exc.message, url=exc.request_info.url, params=params
            )

        # Handle connection errors (e.g., network issues)
        except aiohttp.ClientConnectorError as exc:
            await logger.awarning("Request failed due to connection error", reason=str(exc), params=params)

        # Handle any other unexpected exceptions
        except Exception:
            await logger.aexception("Unexpected error occurred while saving indicators")

