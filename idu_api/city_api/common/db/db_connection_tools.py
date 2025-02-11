import structlog

from idu_api.city_api.common.config.config import config
from idu_api.common.db.connection import PostgresConnectionManager
from idu_api.urban_api.config import DBConfig

logger: structlog.stdlib.BoundLogger = structlog.get_logger()

try:
    connection_manager = PostgresConnectionManager(
        master=DBConfig(
            host=config.get("DB_HOST"),
            port=int(config.get("DB_PORT")),
            database=config.get("DB_DATABASE"),
            user=config.get("DB_USER"),
            password=config.get("DB_PASSWORD"),
            pool_size=10,
        ),
        replicas=[],
        logger=logger,
        application_name="city_api",
    )
except Exception as exc:  # pylint:disable=broad-except
    logger.exception("error on creating default connection manager")
    connection_manager = ...  # pylint: disable=invalid-name
