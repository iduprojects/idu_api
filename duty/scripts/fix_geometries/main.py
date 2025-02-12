import asyncio

import click
import structlog

from idu_api import UrbanAPIConfig
from idu_api.common.db.connection import PostgresConnectionManager
from idu_api.urban_api.config import DBConfig


async def async_main(connection_manager: PostgresConnectionManager, logger: structlog.stdlib.BoundLogger):
    async with connection_manager.get_connection() as conn:
        pass



@click.command("fix-geometries")
@click.option(
    "--config_path",
    envvar="CONFIG_PATH",
    default="../../../urban-api.config.yaml",
    type=click.Path(exists=True, dir_okay=False, path_type=str),
    show_default=True,
    show_envvar=True,
    help="Path to YAML configuration file",
)
@click.option(
    "--geojson_path",
    envvar="GEOJSON_PATH",
    type=click.Path(exists=True, dir_okay=False, path_type=str),
    show_envvar=True,
    help="Path to GeoJSON file",
)
def main(config_path: str):
    config = UrbanAPIConfig.load(config_path)
    logger = structlog.getLogger("fix-geometries")
    connection_manager = PostgresConnectionManager(
        master=DBConfig(
            host=config.db.master.host,
            port=config.db.master.port,
            database="postgres",
            user=config.db.master.user,
            password=config.db.master.password,
            pool_size=1,
        ),
        replicas=[],
        logger=logger,
        application_name="duty_fix_geometry",
    )

    asyncio.run(async_main(connection_manager, logger))