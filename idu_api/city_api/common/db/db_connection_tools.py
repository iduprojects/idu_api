from idu_api.city_api.common.config.config import config
from idu_api.common.db.connection import PostgresConnectionManager

connection_manager = PostgresConnectionManager(
    config.get("DB_HOST"),
    int(config.get("DB_PORT")),
    config.get("DB_DATABASE"),
    config.get("DB_USER"),
    config.get("DB_PASSWORD"),
    application_name="city_api",
)
