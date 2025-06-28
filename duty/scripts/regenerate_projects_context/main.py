"""Duty script to regenerate context territoties for all projects"""

import asyncio

import click
import structlog
from sqlalchemy.sql import select, text
from geoalchemy2.functions import ST_GeomFromWKB

from idu_api.common.db.connection.manager import PostgresConnectionManager
from idu_api.urban_api.config import DBConfig, UrbanAPIConfig
from idu_api.urban_api.dto import UserDTO
from idu_api.urban_api.dto.projects import ProjectDTO
from idu_api.urban_api.logic.impl.helpers.projects_objects import add_context_territories
from idu_api.urban_api.logic.impl.helpers.utils import SRID
from idu_api.urban_api.logic.impl.projects import UserProjectServiceImpl
from idu_api.urban_api.logic.projects import UserProjectService
from idu_api.urban_api.schemas import ProjectPatch


async def regenerate_context(
    service: UserProjectService,
    connection_manager: PostgresConnectionManager,
    project: ProjectDTO
):
    """Load project properties and update it (with new data)."""
    user = UserDTO(id='admin', is_superuser=True)
    territory = await service.get_project_territory_by_id(project.project_id, user=user)
    async with connection_manager.get_connection() as conn:
        geometry = select(
            ST_GeomFromWKB(territory.geometry.wkb, text(str(SRID))).label("geometry")
        ).scalar_subquery()
        await add_context_territories(conn, project, geometry)
    patch_model = ProjectPatch(properties=project.properties)
    await service.patch_project(patch_model, project_id=project.project_id, user=user)


async def async_main(connection_manager: PostgresConnectionManager, logger: structlog.stdlib.BoundLogger):
    """Asynchronously regenerate all projects context territories."""
    service = UserProjectServiceImpl(connection_manager, logger)
    projects = await service.get_all_projects()

    for project in projects:
        try:
            await regenerate_context(service, connection_manager, project)
            logger.info("regenerated project context", project_id=project.project_id)
        except Exception as exc:  # pylint: disable=broad-except
            logger.warning("could not regenerate context", project_id=project.project_id, error=repr(exc))


@click.command("regenerate-projects-context")
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
    """Run the regenerate-projects-context script using the parameters from the console and loading configuration."""
    config = UrbanAPIConfig.load(config_path)
    logger = structlog.getLogger("regenerate-project-context")
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
        application_name="duty_regenerate_projects_context",
    )

    asyncio.run(async_main(connection_manager, logger))


if __name__ == "__main__":
    main()  # pylint: disable=no-value-for-parameter