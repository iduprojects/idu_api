"""Duty script to fix all geometries from geojson by postgis methods."""

import asyncio
import json
import os
from datetime import datetime

import shapely
import click
import structlog
from shapely import geometry as geom

from idu_api.common.db.config import DBConfig
from idu_api.common.db.connection import PostgresConnectionManager
from idu_api.urban_api.config import UrbanAPIConfig
from idu_api.urban_api.logic.impl.system import SystemServiceImpl
from idu_api.urban_api.schemas.geometries import GeoJSONResponse

Geom = geom.Point | geom.MultiPoint | geom.Polygon | geom.MultiPolygon | geom.LineString | geom.MultiLineString | geom.GeometryCollection


async def async_main(
    connection_manager: PostgresConnectionManager,
    logger: structlog.stdlib.BoundLogger,
    geojson_path: str,
    output_file: str | None,
):
    # Reading the source GeoJSON from the file
    with open(geojson_path, "r", encoding="utf-8") as f:
        geojson_data = json.load(f)

    # Parsing the input geojson into the Pydantic model
    try:
        geojson_obj = GeoJSONResponse(**geojson_data)
    except Exception as e:
        await logger.aerror("failed to parse GeoJSON", error=str(e))
        raise

    # Initialize SystemService and process features geometries (to shapely objects)
    system_service = SystemServiceImpl(connection_manager, logger)
    shapely_geoms: list[Geom] = [
        shapely.from_geojson(
            json.dumps(
                {
                    "type": feature["geometry"]["type"],
                    "coordinates": feature["geometry"]["coordinates"]
                }
            )
        ) for feature in geojson_data["features"]
    ]

    # Try to fix geometries by using SystemService.fix_geojson method
    try:
        fixed_geoms = await system_service.fix_geojson(shapely_geoms)
        fixed_geojson = geojson_obj.update_geometries(fixed_geoms)
    except Exception as exc:
        await logger.aerror("failed to fix GeoJSON", error=str(exc))
        raise

    # Save output geojson file
    if output_file is None:
        file_name, file_ext = os.path.splitext(os.path.basename(geojson_path))
        os.makedirs("output", exist_ok=True)
        output_file = os.path.join("output", f"fixed_{file_name}_{datetime.now().strftime('%d%m%y_%H%M%S')}{file_ext}")

    with open(output_file, "w", encoding="utf-8") as f:
        f.write(fixed_geojson.model_dump_json(indent=2))

    await logger.ainfo("fixed geojson saved", output_file=output_file)


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
    required=True,
    show_envvar=True,
    help="Path to GeoJSON file",
)
@click.option(
    "--output",
    envvar="OUTPUT_GEOJSON_PATH",
    default=None,
    type=click.Path(exists=False, dir_okay=False, path_type=str),
    show_envvar=True,
    help="Path to output fixed geojson file",
)
def main(config_path: str, geojson_path: str, output: str | None):
    # Load configuration
    config = UrbanAPIConfig.load(config_path)
    logger = structlog.getLogger("fix-geometries")

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
        application_name="duty_fix_geometry",
    )

    asyncio.run(async_main(connection_manager, logger, geojson_path, output))


if __name__ == "__main__":
    main()
