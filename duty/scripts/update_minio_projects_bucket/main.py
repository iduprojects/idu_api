"""Duty script to update MinIO project's bucket structure."""
import uuid

import asyncio

import click
import structlog

from idu_api.common.db.connection.manager import PostgresConnectionManager
from idu_api.urban_api.config import DBConfig, UrbanAPIConfig
from idu_api.urban_api.logic.impl.projects import UserProjectServiceImpl
from idu_api.urban_api.minio.client import AsyncMinioClient, get_minio_client_from_config
from idu_api.urban_api.minio.services.projects_storage import (
    get_project_storage_manager_from_config,
    ProjectStorageManager,
)


async def update_project_file_structure(
    service: ProjectStorageManager,
    minio_client: AsyncMinioClient,
    project_id: int,
    logger: structlog.stdlib.BoundLogger,
):
    """
    Create folders for given projects and move existing images:
    - folder `main` for main project's image (existing images automatically become main);
    - folder `gallery` for all project's images (existing images also moved here);

    It also creates metadata.json file to manage main and gallery images.
    """

    async with minio_client.get_session() as session:
        # Check if any image exist
        existing_objects = await minio_client.list_objects(session, logger, prefix=f"projects/{project_id}/")
        if not existing_objects:
            logger.warning("no existing images found", project_id=project_id)
            metadata = {"main_image_id": None}
            await service.save_metadata(session, project_id, metadata, logger)
            return

        # Define image identifier
        image_id = str(uuid.uuid4())

        # Copy existing preview image to new folders and remove old
        old_key = f"projects/{project_id}/preview.jpg"
        new_gallery_key = f"{project_id}/gallery/preview/{image_id}.jpg"
        await minio_client.copy_object(session, old_key, new_gallery_key, logger)
        await minio_client.delete_file(session, old_key, logger)

        # Copy existing original image to new folder and remove old
        old_key = f"projects/{project_id}/image.jpg"
        new_gallery_key = f"{project_id}/gallery/original/{image_id}.jpg"
        await minio_client.copy_object(session, old_key, new_gallery_key, logger)
        await minio_client.delete_file(session, old_key, logger)

        # Create metadata.json file
        metadata = {"main_image_id": image_id, "gallery_images": [image_id]}
        await service.save_metadata(session, project_id, metadata, logger)


async def async_main(
    connection_manager: PostgresConnectionManager,
    minio_client: AsyncMinioClient,
    project_storage_manager: ProjectStorageManager,
    logger: structlog.stdlib.BoundLogger
):
    """Asynchronously update MinIO project's bucket structure."""
    service = UserProjectServiceImpl(connection_manager, logger)
    projects = await service.get_all_projects()

    for project in projects:
        try:
            await update_project_file_structure(project_storage_manager, minio_client, project.project_id, logger)
            logger.info("regenerated project file structure", project_id=project.project_id)
        except Exception as exc:  # pylint: disable=broad-except
            logger.warning("could not regenerate project file structure", project_id=project.project_id, error=repr(exc))


@click.command("regenerate-projects-previews")
@click.option(
    "--config_path",
    envvar="CONFIG_PATH",
    default="../../../urban-api.config.yaml",
    type=click.Path(exists=True, dir_okay=False, path_type=str),
    show_default=True,
    show_envvar=True,
    help="Path to YAML configuration file",
)
def main(config_path: str):
    """Run the regenerate-projects-previews script using the parameters from the console and loading configuration."""
    config = UrbanAPIConfig.load(config_path)
    logger = structlog.getLogger("update-minio-projects-bucket")
    connection_manager = PostgresConnectionManager(
        master=DBConfig(
            host=config.db.master.host,
            port=config.db.master.port,
            database=config.db.master.database,
            user=config.db.master.user,
            password=config.db.master.password,
            pool_size=1,
        ),
        replicas=config.db.replicas or [],
        logger=logger,
        application_name="duty_update_minio_projects_bucket",
    )
    minio_client = get_minio_client_from_config(config)
    project_storage_manager = get_project_storage_manager_from_config(config)

    asyncio.run(async_main(connection_manager, minio_client, project_storage_manager, logger))


if __name__ == "__main__":
    main()  # pylint: disable=no-value-for-parameter
