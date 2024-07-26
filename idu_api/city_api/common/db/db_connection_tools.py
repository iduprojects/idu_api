from idu_api.common.db.connection import PostgresConnectionManager


connection_manager = PostgresConnectionManager(
    "10.32.1.107",
    5432,
    "urban_db",
    "postgres",
    "postgres",
    application_name="city_api"
)
