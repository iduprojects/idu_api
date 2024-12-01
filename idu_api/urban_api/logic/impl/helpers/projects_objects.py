"""Projects internal logic is defined here."""

import asyncio
import io
import zipfile
from datetime import datetime, timezone

from geoalchemy2 import Geography, Geometry
from geoalchemy2.functions import (
    ST_Area,
    ST_AsGeoJSON,
    ST_Buffer,
    ST_Centroid,
    ST_GeomFromText,
    ST_Intersection,
    ST_Intersects,
    ST_Within,
)
from PIL import Image
from sqlalchemy import Integer, cast, delete, insert, literal, or_, select, text, update
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncConnection

from idu_api.common.db.entities import (
    functional_zones_data,
    object_geometries_data,
    physical_object_types_dict,
    physical_objects_data,
    profiles_data,
    projects_data,
    projects_object_geometries_data,
    projects_physical_objects_data,
    projects_services_data,
    projects_territory_data,
    projects_urban_objects_data,
    scenarios_data,
    territories_data,
    urban_objects_data,
)
from idu_api.urban_api.dto import ProjectDTO, ProjectTerritoryDTO
from idu_api.urban_api.exceptions.logic.common import EntityNotFoundById
from idu_api.urban_api.exceptions.logic.users import AccessDeniedError
from idu_api.urban_api.exceptions.utils.pillow import InvalidImageError
from idu_api.urban_api.schemas import (
    ProjectPatch,
    ProjectPost,
    ProjectPut,
)
from idu_api.urban_api.utils.minio_client import AsyncMinioClient


async def get_project_by_id_from_db(conn: AsyncConnection, project_id: int, user_id: str) -> ProjectDTO:
    """Get project object by identifier."""

    statement = (
        select(projects_data, territories_data.c.name.label("territory_name"))
        .select_from(
            projects_data.join(
                territories_data,
                territories_data.c.territory_id == projects_data.c.territory_id,
            )
        )
        .where(projects_data.c.project_id == project_id)
    )
    result = (await conn.execute(statement)).mappings().one_or_none()

    if result is None:
        raise EntityNotFoundById(project_id, "project")
    if result.user_id != user_id and result.public is False:
        raise AccessDeniedError(project_id, "project")

    return ProjectDTO(**result)


async def get_project_territory_by_id_from_db(
    conn: AsyncConnection, project_id: int, user_id: str
) -> ProjectTerritoryDTO:
    """Get project territory object by project identifier."""

    statement_for_project = select(projects_data).where(projects_data.c.project_id == project_id)
    result_for_project = (await conn.execute(statement_for_project)).mappings().one_or_none()
    if result_for_project is None:
        raise EntityNotFoundById(project_id, "project")
    if result_for_project.user_id != user_id and result_for_project.public is False:
        raise AccessDeniedError(project_id, "project")

    statement = (
        select(
            projects_territory_data.c.project_territory_id,
            projects_data.c.project_id,
            projects_data.c.name.label("project_name"),
            projects_data.c.user_id.label("project_user_id"),
            territories_data.c.territory_id,
            territories_data.c.name.label("territory_name"),
            cast(ST_AsGeoJSON(projects_territory_data.c.geometry), JSONB).label("geometry"),
            cast(ST_AsGeoJSON(projects_territory_data.c.centre_point), JSONB).label("centre_point"),
            projects_territory_data.c.properties,
        )
        .select_from(
            projects_territory_data.join(
                projects_data,
                projects_data.c.project_id == projects_territory_data.c.project_id,
            ).join(
                territories_data,
                territories_data.c.territory_id == projects_data.c.territory_id,
            )
        )
        .where(projects_territory_data.c.project_id == project_id)
    )

    result = (await conn.execute(statement)).mappings().one_or_none()
    if result is None:
        raise EntityNotFoundById(project_id, "project territory")

    return ProjectTerritoryDTO(**result)


async def get_all_available_projects_from_db(
    conn: AsyncConnection, user_id: str | None, is_regional: bool
) -> list[ProjectDTO]:
    """Get all public and user's projects."""

    statement = (
        select(projects_data, territories_data.c.name.label("territory_name"))
        .select_from(
            projects_data.join(
                territories_data,
                territories_data.c.territory_id == projects_data.c.territory_id,
            )
        )
        .order_by(projects_data.c.project_id)
    )

    if user_id is not None:
        statement = statement.where(
            or_(projects_data.c.user_id == user_id, projects_data.c.public.is_(True)),
            projects_data.c.is_regional.is_(is_regional),
        )
    else:
        statement = statement.where(projects_data.c.public.is_(True), projects_data.c.is_regional.is_(is_regional))

    results = (await conn.execute(statement)).mappings().all()

    return [ProjectDTO(**result) for result in results]


async def get_all_preview_projects_images_from_minio(
    conn: AsyncConnection, minio_client: AsyncMinioClient, user_id: str | None, is_regional: bool
) -> io.BytesIO:
    """Get preview images for all public and user's projects with parallel MinIO requests."""

    statement = select(projects_data.c.project_id).order_by(projects_data.c.project_id)
    if user_id is not None:
        statement = statement.where(
            or_(projects_data.c.user_id == user_id, projects_data.c.public.is_(True)),
            projects_data.c.is_regional.is_(is_regional),
        )
    else:
        statement = statement.where(projects_data.c.public.is_(True), projects_data.c.is_regional.is_(is_regional))
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


async def get_all_preview_projects_images_url_from_minio(
    conn: AsyncConnection, minio_client: AsyncMinioClient, user_id: str | None, is_regional: bool
) -> list[dict[str, int | str]]:
    """Get preview images url for all public and user's projects."""

    statement = select(projects_data.c.project_id).order_by(projects_data.c.project_id)
    if user_id is not None:
        statement = statement.where(
            or_(projects_data.c.user_id == user_id, projects_data.c.public.is_(True)),
            projects_data.c.is_regional.is_(is_regional),
        )
    else:
        statement = statement.where(projects_data.c.public.is_(True), projects_data.c.is_regional.is_(is_regional))
    project_ids = (await conn.execute(statement)).scalars().all()

    async def fetch_image(project_id: int) -> tuple[int, str]:
        """Get preview image url for given project identifier."""

        url = await minio_client.get_presigned_url(f"projects/{project_id}/preview.png")
        return project_id, url

    results = await asyncio.gather(*(fetch_image(project_id) for project_id in project_ids))

    return [{"project_id": project_id, "url": url} for project_id, url in results if url is not None]


async def get_user_projects_from_db(conn: AsyncConnection, user_id: str, is_regional: bool) -> list[ProjectDTO]:
    """Get all user's projects."""

    statement = (
        select(projects_data, territories_data.c.name.label("territory_name"))
        .select_from(
            projects_data.join(
                territories_data,
                territories_data.c.territory_id == projects_data.c.territory_id,
            )
        )
        .where(projects_data.c.user_id == user_id, projects_data.c.is_regional.is_(is_regional))
        .order_by(projects_data.c.project_id)
    )
    results = (await conn.execute(statement)).mappings().all()

    return [ProjectDTO(**result) for result in results]


async def get_user_preview_projects_images_from_minio(
    conn: AsyncConnection, minio_client: AsyncMinioClient, user_id: str, is_regional: bool
) -> io.BytesIO:
    """Get preview images for all user's projects with parallel MinIO requests."""

    statement = (
        select(projects_data.c.project_id)
        .where(projects_data.c.user_id == user_id, projects_data.c.is_regional.is_(is_regional))
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


async def get_user_preview_projects_images_url_from_minio(
    conn: AsyncConnection, minio_client: AsyncMinioClient, user_id: str, is_regional: bool
) -> list[dict[str, int | str]]:
    """Get preview images url for all user's projects."""

    statement = (
        select(projects_data.c.project_id)
        .where(projects_data.c.user_id == user_id, projects_data.c.is_regional.is_(is_regional))
        .order_by(projects_data.c.project_id)
    )
    project_ids = (await conn.execute(statement)).scalars().all()

    async def fetch_image(project_id: int) -> tuple[int, str]:
        """Get preview image url for given project identifier."""

        url = await minio_client.get_presigned_url(f"projects/{project_id}/preview.png")
        return project_id, url

    results = await asyncio.gather(*(fetch_image(project_id) for project_id in project_ids))

    return [{"project_id": project_id, "url": url} for project_id, url in results if url is not None]


async def add_project_to_db(conn: AsyncConnection, project: ProjectPost, user_id: str) -> ProjectDTO:
    """Create project object, its territory and base scenario."""

    given_geometry = select(ST_GeomFromText(str(project.territory.geometry.as_shapely_geometry()), text("4326"))).cte(
        "given_geometry"
    )

    buffer_meters = 3000
    buffered_project_geometry = select(
        cast(
            ST_Buffer(
                cast(
                    select(given_geometry).scalar_subquery(),
                    Geography(srid=4326),
                ),
                buffer_meters,
            ),
            Geometry(srid=4326),
        ).label("geometry")
    ).alias("buffered_project_geometry")

    territories_cte = (
        select(
            territories_data.c.territory_id,
            territories_data.c.name,
            territories_data.c.parent_id,
            territories_data.c.level,
            territories_data.c.geometry,
            territories_data.c.is_city,
        )
        .where(territories_data.c.territory_id == project.territory_id)
        .cte(recursive=True)
    )
    territories_cte = territories_cte.union_all(
        select(
            territories_data.c.territory_id,
            territories_data.c.name,
            territories_data.c.parent_id,
            territories_data.c.level,
            territories_data.c.geometry,
            territories_data.c.is_city,
        ).join(
            territories_cte,
            territories_data.c.parent_id == territories_cte.c.territory_id,
        )
    )
    cities_level = (
        select(territories_cte.c.level)
        .where(territories_cte.c.is_city.is_(True))
        .order_by(territories_cte.c.level.desc())
        .limit(1)
        .cte(name="cities_level")
    )
    intersecting_territories = select(territories_cte.c.name).where(
        territories_cte.c.level == (cities_level.c.level - 1),
        ST_Intersects(territories_cte.c.geometry, select(given_geometry).scalar_subquery()),
    )
    intersecting_district = select(territories_cte.c.name).where(
        territories_cte.c.level == (cities_level.c.level - 2),
        ST_Intersects(territories_cte.c.geometry, select(given_geometry).scalar_subquery()),
    )
    context_territories = select(territories_cte.c.territory_id).where(
        territories_cte.c.level == (cities_level.c.level - 1),
        ST_Intersects(territories_cte.c.geometry, select(buffered_project_geometry).scalar_subquery()),
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
        .values(
            user_id=user_id,
            territory_id=project.territory_id,
            name=project.name,
            description=project.description,
            public=project.public,
            is_regional=project.is_regional,
            properties=project.properties,
        )
        .returning(projects_data.c.project_id)
    )
    project_id = (await conn.execute(statement_for_project)).scalar_one()

    await conn.execute(
        (
            insert(projects_territory_data).values(
                project_id=project_id,
                geometry=select(given_geometry).scalar_subquery(),
                centre_point=ST_GeomFromText(str(project.territory.centre_point.as_shapely_geometry()), text("4326")),
                properties=project.territory.properties,
            )
        )
    )

    # TODO: use the real parent scenario identifier from the user
    #  instead of using the basic regional scenario by default
    parent_scenario_id = (
        await conn.execute(
            select(scenarios_data.c.scenario_id)
            .select_from(scenarios_data.join(projects_data, projects_data.c.project_id == scenarios_data.c.project_id))
            .where(
                projects_data.c.territory_id == project.territory_id,
                projects_data.c.is_regional.is_(True),
                scenarios_data.c.is_based.is_(True),
            )
            .limit(1)
        )
    ).scalar_one()

    scenario_id = (
        await conn.execute(
            insert(scenarios_data)
            .values(
                project_id=project_id,
                functional_zone_type_id=None,
                name="base scenario for user project",
                is_based=True,
                parent_id=parent_scenario_id,
            )
            .returning(scenarios_data.c.scenario_id)
        )
    ).scalar_one()

    # 1. Find all territories that are only partially included in the transferred geometry
    territories_intersecting_only = (
        select(territories_cte.c.territory_id).where(
            ST_Intersects(territories_cte.c.geometry, select(given_geometry).scalar_subquery()),
            ~ST_Within(territories_cte.c.geometry, select(given_geometry).scalar_subquery()),
        )
    ).cte("territories_intersecting_only")

    # TODO: get geometries and urban objects from regional scenario

    # 2. Find all objects for territories from the first step (partially included at given percent)
    # where the geometry is not completely included in the passed
    area_percent = 0.10
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
            object_geometries_data.c.territory_id.in_(select(territories_intersecting_only)),
            ST_Intersects(object_geometries_data.c.geometry, select(given_geometry).scalar_subquery()),
            ~ST_Within(object_geometries_data.c.geometry, select(given_geometry).scalar_subquery()),
            ST_Area(ST_Intersection(object_geometries_data.c.geometry, select(given_geometry).scalar_subquery()))
            >= area_percent * ST_Area(object_geometries_data.c.geometry),
            physical_object_types_dict.c.physical_object_function_id != 1,  # TODO: remove hardcode to skip buildings
        )
        .distinct()
    ).cte("objects_intersecting")

    # 3. Crop and insert geometries from the second step
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
                ST_Intersection(object_geometries_data.c.geometry, select(given_geometry).scalar_subquery()).label(
                    "geometry"
                ),
                ST_Centroid(
                    ST_Intersection(object_geometries_data.c.geometry, select(given_geometry).scalar_subquery())
                ).label("centre_point"),
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

    # 4. Insert cropped geometries from third step to `projects_urban_objects_data`
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

    # 5. Find all functional zones for the project
    # TODO: If we get at least one urban object from regional scenario
    # or we haven't got any functional zones for the project territory from public schema,
    # it's needed to generate new functional zones else get it from public schema
    statement = select(
        functional_zones_data.c.functional_zone_type_id,
        functional_zones_data.c.geometry,
        functional_zones_data.c.properties,
    ).where(
        functional_zones_data.c.territory_id.in_(select(territories_cte.c.territory_id)),
        ST_Within(functional_zones_data.c.geometry, select(given_geometry).scalar_subquery())
        | ST_Intersects(functional_zones_data.c.geometry, select(given_geometry).scalar_subquery()),
    )
    zones = (await conn.execute(statement)).mappings().all()

    if not zones:
        pass  # TODO: use external service to generate functional zones
    else:
        await conn.execute(
            insert(profiles_data).from_select(
                [
                    profiles_data.c.scenario_id,
                    profiles_data.c.functional_zone_type_id,
                    profiles_data.c.geometry,
                    profiles_data.c.properties,
                ],
                select(
                    cast(literal(scenario_id), Integer).label("scenario_id"),
                    functional_zones_data.c.functional_zone_type_id,
                    ST_Intersection(functional_zones_data.c.geometry, select(given_geometry).scalar_subquery()).label(
                        "geometry"
                    ),
                    functional_zones_data.c.properties,
                ).where(
                    functional_zones_data.c.territory_id.in_(select(territories_cte.c.territory_id)),
                    ST_Intersects(functional_zones_data.c.geometry, select(given_geometry).scalar_subquery()),
                ),
            )
        )

    await conn.commit()

    return await get_project_by_id_from_db(conn, project_id, user_id)


async def put_project_to_db(conn: AsyncConnection, project: ProjectPut, project_id: int, user_id: str) -> ProjectDTO:
    """Update project object by all its attributes."""

    statement = select(projects_data).where(projects_data.c.project_id == project_id)
    requested_project = (await conn.execute(statement)).one_or_none()
    if requested_project is None:
        raise EntityNotFoundById(project_id, "project")
    if requested_project.user_id != user_id:
        raise AccessDeniedError(project_id, "project")

    statement = (
        update(projects_data)
        .where(projects_data.c.project_id == project_id)
        .values(
            name=project.name,
            description=project.description,
            public=project.public,
            properties=project.properties,
            updated_at=datetime.now(timezone.utc),
        )
        .returning(projects_data.c.project_id)
    )
    project_id = (await conn.execute(statement)).scalar_one()

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

    for k, v in project.model_dump(exclude_unset=True).items():
        new_values_for_project.update({k: v})

    statement_for_project = (
        update(projects_data)
        .where(projects_data.c.project_id == project_id)
        .values(updated_at=datetime.now(timezone.utc), **new_values_for_project)
        .returning(projects_data)
    )
    await conn.execute(statement_for_project)

    await conn.commit()

    return await get_project_by_id_from_db(conn, project_id, user_id)


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

    delete_geometry_statement = delete(projects_object_geometries_data).where(
        projects_object_geometries_data.c.object_geometry_id.in_(geometry_ids)
    )
    await conn.execute(delete_geometry_statement)

    delete_physical_statement = delete(projects_physical_objects_data).where(
        projects_physical_objects_data.c.physical_object_id.in_(physical_ids)
    )
    await conn.execute(delete_physical_statement)

    delete_service_statement = delete(projects_services_data).where(
        projects_services_data.c.service_id.in_(service_ids)
    )
    await conn.execute(delete_service_statement)

    statement_for_project = delete(projects_data).where(projects_data.c.project_id == project_id)

    await conn.execute(statement_for_project)
    await minio_client.delete_file(f"projects/{project_id}/")

    await conn.commit()

    return {"status": "ok"}


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


async def get_full_project_image_url_from_minio(
    conn: AsyncConnection, minio_client: AsyncMinioClient, project_id: int, user_id: str
) -> str:
    """Get full image url for given project."""

    statement = select(projects_data).where(projects_data.c.project_id == project_id)
    result = (await conn.execute(statement)).mappings().one_or_none()

    if result is None:
        raise EntityNotFoundById(project_id, "project")
    if result.user_id != user_id and result.public is False:
        raise AccessDeniedError(project_id, "project")

    return await minio_client.get_presigned_url(f"projects/{project_id}/image.jpg")
