"""Duty script to regenerate previews for all projects"""

import asyncio

import click
import structlog

from idu_api.common.db.connection.manager import PostgresConnectionManager
from idu_api.urban_api.config import DBConfig, UrbanAPIConfig
from idu_api.urban_api.dto.projects import ProjectDTO
from idu_api.urban_api.logic.impl.projects import UserProjectServiceImpl
from idu_api.urban_api.logic.projects import UserProjectService
from idu_api.urban_api.utils.minio_client import AsyncMinioClient, get_minio_client_from_config


async def regenerate_preview(service: UserProjectService, minio_client: AsyncMinioClient, project: ProjectDTO):
    """Load project image and upload again (with new params for preview image)."""
    image_buf = await service.get_project_image(minio_client, project.project_id, project.user_id, image_type="origin")
    await service.upload_project_image(minio_client, project.project_id, project.user_id, image_buf.read())


async def async_main(
    connection_manager: PostgresConnectionManager, minio_client: AsyncMinioClient, logger: structlog.stdlib.BoundLogger
):
    """Asynchronously regenerate all preview projects images."""
    service = UserProjectServiceImpl(connection_manager, logger)
    projects = await service.get_all_projects()

    for project in projects:
        try:
            await regenerate_preview(service, minio_client, project)
            logger.info("regenerated project preview", project_id=project.project_id)
        except Exception as exc:  # pylint: disable=broad-except
            logger.warning("could not regenerate preview", project_id=project.project_id, error=repr(exc))


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
    logger = structlog.getLogger("regenerate-project-previews")
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
        application_name="duty_regenerate_projects_previews",
    )
    minio_client = get_minio_client_from_config(config)

    asyncio.run(async_main(connection_manager, minio_client, logger))


if __name__ == "__main__":
    main()  # pylint: disable=no-value-for-parameter
