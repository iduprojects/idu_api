"""Projects internal logic is defined here."""

import asyncio
import io
import zipfile
from datetime import datetime, timezone

from geoalchemy2.functions import ST_AsGeoJSON, ST_GeomFromText, ST_Intersects, ST_Within
from PIL import Image
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
    territories_data,
)
from idu_api.urban_api.dto import ProjectDTO, ProjectTerritoryDTO
from idu_api.urban_api.exceptions.logic.common import EntityNotFoundById
from idu_api.urban_api.exceptions.logic.users import AccessDeniedError
from idu_api.urban_api.exceptions.utils.pillow import InvalidImageError
from idu_api.urban_api.logic.impl.helpers.territory_objects import get_common_territory_for_geometry
from idu_api.urban_api.logic.impl.helpers.urban_objects import get_urban_object_by_object_geometry_id_from_db
from idu_api.urban_api.schemas import ProjectPatch, ProjectPost, ProjectPut
from idu_api.urban_api.utils.minio_client import AsyncMinioClient


async def get_project_by_id_from_db(conn: AsyncConnection, project_id: int, user_id: str) -> ProjectDTO:
    """Get project object by id."""

    statement = select(projects_data).where(projects_data.c.project_id == project_id)
    result = (await conn.execute(statement)).mappings().one_or_none()

    if result is None:
        raise EntityNotFoundById(project_id, "project")
    if result.user_id != user_id and result.public is False:
        raise AccessDeniedError(project_id, "project")

    return ProjectDTO(**result)


async def add_project_to_db(conn: AsyncConnection, project: ProjectPost, user_id: str) -> ProjectDTO:
    """Create project object and base scenario."""

    parent_territory = await get_common_territory_for_geometry(
        conn, project.project_territory_info.geometry.as_shapely_geometry()
    )

    statement_for_territory = (
        insert(projects_territory_data)
        .values(
            parent_territory_id=parent_territory.territory_id if parent_territory is not None else None,
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
            properties=project.properties,
        )
        .returning(projects_data.c.project_id)
    )
    project_id = (await conn.execute(statement_for_project)).scalar_one()

    statement_for_scenario = (
        insert(scenarios_data)
        .values(project_id=project_id, target_profile_id=None, name=f"base scenario for project with id={project_id}")
        .returning(scenarios_data.c.scenario_id)
    )
    scenario_id = (await conn.execute(statement_for_scenario)).scalar_one()

    if parent_territory is not None:
        # 1. Find all territories that are completely included in the transferred geometry.
        territories_fully_within = select(territories_data.c.territory_id).where(
            ST_Within(
                territories_data.c.geometry,
                ST_GeomFromText(str(project.project_territory_info.geometry.as_shapely_geometry()), text("4326")),
            )
        )

        # 2. Find all territories that are partially included in the transferred geometry
        territories_intersecting = (
            select(territories_data.c.territory_id)
            .where(
                ST_Intersects(
                    territories_data.c.geometry,
                    ST_GeomFromText(str(project.project_territory_info.geometry.as_shapely_geometry()), text("4326")),
                )
            )
            .where(territories_data.c.territory_id.notin_(territories_fully_within))
        )

        fully_within_ids = (await conn.execute(territories_fully_within)).scalars().all()
        intersecting_ids = (await conn.execute(territories_intersecting)).scalars().all()

        # 3. Get all objects for territories from the first group (fully included)
        objects_fully_within = select(object_geometries_data.c.object_geometry_id).where(
            object_geometries_data.c.territory_id.in_(fully_within_ids)
        )

        # 4. Get all objects for territories from the second group (partially included),
        # but where the geometry is completely included in the passed
        objects_intersecting_and_within = (
            select(object_geometries_data.c.object_geometry_id)
            .where(object_geometries_data.c.territory_id.in_(intersecting_ids))
            .where(
                ST_Within(
                    object_geometries_data.c.geometry,
                    ST_GeomFromText(str(project.project_territory_info.geometry.as_shapely_geometry()), text("4326")),
                )
            )
        )
        fully_within_objects_ids = (await conn.execute(objects_fully_within)).scalars().all()
        intersecting_and_within_objects_ids = (await conn.execute(objects_intersecting_and_within)).scalars().all()

        object_geometries_ids = set(fully_within_objects_ids) | set(intersecting_and_within_objects_ids)

        for object_geometry_id in object_geometries_ids:
            urban_objects = await get_urban_object_by_object_geometry_id_from_db(conn, object_geometry_id)
            for urban_object in urban_objects:
                statement_for_physical_object = (
                    insert(projects_physical_objects_data)
                    .values(
                        physical_object_type_id=urban_object.physical_object_type_id,
                        name=urban_object.physical_object_name,
                        properties=urban_object.physical_object_properties,
                        created_at=urban_object.physical_object_created_at,
                        updated_at=urban_object.physical_object_updated_at,
                    )
                    .returning(projects_physical_objects_data.c.physical_object_id)
                )
                physical_object_id = (await conn.execute(statement_for_physical_object)).scalar_one()
                statement_for_object_geometry = (
                    insert(projects_object_geometries_data)
                    .values(
                        territory_id=urban_object.territory_id,
                        geometry=ST_GeomFromText(str(urban_object.geometry), text("4326")),
                        centre_point=ST_GeomFromText(str(urban_object.centre_point), text("4326")),
                        address=urban_object.address,
                        osm_id=urban_object.osm_id,
                        created_at=urban_object.object_geometry_created_at,
                        updated_at=urban_object.object_geometry_updated_at,
                    )
                    .returning(projects_object_geometries_data.c.object_geometry_id)
                )
                new_object_geometry_id = (await conn.execute(statement_for_object_geometry)).scalar_one()
                service_id = None
                if urban_object.service_id is not None:
                    statement_for_service = (
                        insert(projects_services_data)
                        .values(
                            service_type_id=urban_object.service_type_id,
                            name=urban_object.service_name,
                            properties=urban_object.service_properties,
                            capacity_real=urban_object.capacity_real,
                            created_at=urban_object.service_created_at,
                            updated_at=urban_object.service_updated_at,
                        )
                        .returning(projects_services_data.c.service_id)
                    )
                    service_id = (await conn.execute(statement_for_service)).scalar_one()
                statement_for_urban_object = (
                    insert(projects_urban_objects_data)
                    .values(
                        physical_object_id=physical_object_id,
                        object_geometry_id=new_object_geometry_id,
                        service_id=service_id,
                        scenario_id=scenario_id,
                    )
                    .returning(projects_urban_objects_data.c.urban_object_id)
                )
                await conn.execute(statement_for_urban_object)

    await conn.commit()

    return await get_project_by_id_from_db(conn, project_id, user_id)


async def get_all_available_projects_from_db(conn: AsyncConnection, user_id: str | None) -> list[ProjectDTO]:
    """Get all public and user's projects."""

    statement = select(projects_data).order_by(projects_data.c.project_id)

    if user_id is not None:
        statement = statement.where(or_(projects_data.c.user_id == user_id, projects_data.c.public.is_(True)))
    else:
        statement = statement.where(projects_data.c.public.is_(True))

    results = (await conn.execute(statement)).mappings().all()

    return [ProjectDTO(**result) for result in results]


async def get_all_preview_projects_images_from_minio(
    conn: AsyncConnection, minio_client: AsyncMinioClient, user_id: str | None
) -> io.BytesIO:
    """Get preview images for all public and user's projects with parallel MinIO requests."""

    statement = select(projects_data.c.project_id).order_by(projects_data.c.project_id)
    if user_id is not None:
        statement = statement.where(or_(projects_data.c.user_id == user_id, projects_data.c.public.is_(True)))
    else:
        statement = statement.where(projects_data.c.public.is_(True))
    project_ids = (await conn.execute(statement)).scalars().all()

    async def fetch_image(project_id: int) -> tuple[int, io.BytesIO]:
        """Get preview image for given project identifier."""

        image = await minio_client.get_file(f"projects/{project_id}/preview.png")
        return project_id, image

    results = await asyncio.gather(*(fetch_image(project_id) for project_id in project_ids))
    images = {project_id: image for project_id, image in results if image is not None}

    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w") as zip_file:
        for project_id, image_stream in images.items():
            if image_stream:
                zip_file.writestr(f"preview_{project_id}.png", image_stream.read())
    zip_buffer.seek(0)

    return zip_buffer


async def get_user_projects_from_db(conn: AsyncConnection, user_id: str) -> list[ProjectDTO]:
    """Get all user's projects."""

    statement = select(projects_data).where(projects_data.c.user_id == user_id).order_by(projects_data.c.project_id)
    results = (await conn.execute(statement)).mappings().all()

    return [ProjectDTO(**result) for result in results]


async def get_user_preview_projects_images_from_minio(
    conn: AsyncConnection, minio_client: AsyncMinioClient, user_id: str
) -> io.BytesIO:
    """Get preview images for all user's projects with parallel MinIO requests."""

    statement = (
        select(projects_data.c.project_id)
        .where(projects_data.c.user_id == user_id)
        .order_by(projects_data.c.project_id)
    )
    project_ids = (await conn.execute(statement)).scalars().all()

    async def fetch_image(project_id: int) -> tuple[int, io.BytesIO]:
        """Get preview image for given project identifier."""

        image = await minio_client.get_file(f"projects/{project_id}/preview.png")
        return project_id, image

    results = await asyncio.gather(*(fetch_image(project_id) for project_id in project_ids))
    images = {project_id: image for project_id, image in results if image is not None}

    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w") as zip_file:
        for project_id, image_stream in images.items():
            if image_stream:
                zip_file.writestr(f"preview_{project_id}.png", image_stream.read())
    zip_buffer.seek(0)

    return zip_buffer


async def get_project_territory_by_id_from_db(
    conn: AsyncConnection, project_id: int, user_id: str
) -> ProjectTerritoryDTO:
    """Get project object by id."""

    statement_for_project = select(projects_data).where(projects_data.c.project_id == project_id)
    result_for_project = (await conn.execute(statement_for_project)).mappings().one_or_none()
    if result_for_project is None:
        raise EntityNotFoundById(project_id, "project")
    if result_for_project.user_id != user_id and result_for_project.public is False:
        raise AccessDeniedError(project_id, "project")

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


async def delete_project_from_db(
    conn: AsyncConnection, project_id: int, minio_client: AsyncMinioClient, user_id: str
) -> dict:
    """Delete project object."""

    statement = select(projects_data).where(projects_data.c.project_id == project_id)
    result = (await conn.execute(statement)).one_or_none()
    if result is None:
        raise EntityNotFoundById(project_id, "project")
    if result.user_id != user_id:
        raise AccessDeniedError(project_id, "project")

    statement_for_project = delete(projects_data).where(projects_data.c.project_id == project_id)

    statement_for_territory = delete(projects_territory_data).where(
        projects_territory_data.c.project_territory_id == result.project_territory_id
    )

    await conn.execute(statement_for_project)
    await conn.execute(statement_for_territory)

    await conn.commit()

    await minio_client.delete_file(f"projects/{project_id}/")

    return {"status": "ok"}


async def put_project_to_db(conn: AsyncConnection, project: ProjectPut, project_id: int, user_id: str) -> ProjectDTO:
    """Put project object."""

    statement = select(projects_data).where(projects_data.c.project_id == project_id)
    requested_project = (await conn.execute(statement)).one_or_none()
    if requested_project is None:
        raise EntityNotFoundById(project_id, "project")
    if requested_project.user_id != user_id:
        raise AccessDeniedError(project_id, "project")

    statement_for_parent_territory = select(projects_territory_data.c.parent_territory_id).where(
        projects_territory_data.c.project_territory_id == requested_project.project_territory_id
    )
    requested_parent_territory_id = (await conn.execute(statement_for_parent_territory)).scalar_one_or_none()

    statement_for_territory = (
        update(projects_territory_data)
        .where(projects_territory_data.c.project_territory_id == requested_project.project_territory_id)
        .values(
            parent_territory_id=requested_parent_territory_id,
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
            properties=project.properties,
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
        raise EntityNotFoundById(project_id, "project")
    if requested_project.user_id != user_id:
        raise AccessDeniedError(project_id, "project")

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


async def upload_project_image_to_minio(
    conn: AsyncConnection, minio_client: AsyncMinioClient, project_id: int, user_id: str, file: bytes
) -> dict:
    """Create project image preview and upload it (full and preview) to minio bucket."""

    statement = select(projects_data).where(projects_data.c.project_id == project_id)
    result = (await conn.execute(statement)).mappings().one_or_none()

    if result is None:
        raise EntityNotFoundById(project_id, "project")
    if result.user_id != user_id and result.public is False:
        raise AccessDeniedError(project_id, "project")

    try:
        image = Image.open(io.BytesIO(file))
    except Exception as exc:
        raise InvalidImageError(project_id) from exc

    preview_image = image.copy()
    width, height = preview_image.size
    target_width = 100
    target_height = 100

    target_aspect_ratio = target_width / target_height
    current_aspect_ratio = width / height

    if current_aspect_ratio > target_aspect_ratio:
        new_width = int(height * target_aspect_ratio)
        left = (width - new_width) / 2
        top = 0
        right = left + new_width
        bottom = height
    else:
        new_height = int(width / target_aspect_ratio)
        left = 0
        top = (height - new_height) / 2
        right = width
        bottom = top + new_height

    img_cropped = preview_image.crop((left, top, right, bottom))
    preview_image = img_cropped.resize((target_width, target_height))

    if image.mode == "RGBA":
        image = image.convert("RGB")

    image_stream = io.BytesIO()
    image.save(image_stream, format="JPEG")
    image_stream.seek(0)

    preview_stream = io.BytesIO()
    preview_image.save(preview_stream, format="PNG")
    preview_stream.seek(0)

    await minio_client.upload_file(image_stream.getvalue(), f"projects/{project_id}/image.jpg")
    await minio_client.upload_file(preview_stream.getvalue(), f"projects/{project_id}/preview.png")

    return {
        "image_url": await minio_client.get_presigned_url(f"projects/{project_id}/image.jpg"),
        "preview_url": await minio_client.get_presigned_url(f"projects/{project_id}/preview.png"),
    }


async def get_full_project_image_from_minio(
    conn: AsyncConnection, minio_client: AsyncMinioClient, project_id: int, user_id: str
) -> io.BytesIO:
    """Get full image for given project."""

    statement = select(projects_data).where(projects_data.c.project_id == project_id)
    result = (await conn.execute(statement)).mappings().one_or_none()

    if result is None:
        raise EntityNotFoundById(project_id, "project")
    if result.user_id != user_id and result.public is False:
        raise AccessDeniedError(project_id, "project")

    return await minio_client.get_file(f"projects/{project_id}/image.jpg")


async def get_preview_project_image_from_minio(
    conn: AsyncConnection, minio_client: AsyncMinioClient, project_id: int, user_id: str
) -> io.BytesIO:
    """Get preview image for given project."""

    statement = select(projects_data).where(projects_data.c.project_id == project_id)
    result = (await conn.execute(statement)).mappings().one_or_none()

    if result is None:
        raise EntityNotFoundById(project_id, "project")
    if result.user_id != user_id and result.public is False:
        raise AccessDeniedError(project_id, "project")

    return await minio_client.get_file(f"projects/{project_id}/preview.png")
