"""All configurations and fixtures, including database and urban_api_host, are defined here."""

import os
import random
import subprocess
import tempfile
import time
from collections.abc import Iterator
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from dotenv import load_dotenv

from idu_api.urban_api.config import AppConfig, DBConfig, UrbanAPIConfig
from tests.urban_api.helpers import *

load_dotenv(dotenv_path="urban_api/.env")


@pytest.fixture(scope="session")
def database() -> DBConfig:
    """Fixture to get database credentials from environment variables."""

    config = UrbanAPIConfig.load(os.environ["CONFIG_PATH"])

    if "CONFIG_PATH" not in os.environ:
        pytest.skip("Database for integration tests is not configured")

    run_migrations(config.db)
    return config.db


@pytest.fixture(scope="session")
def config(database) -> UrbanAPIConfig:
    """Fixture to generate configuration from environment variables."""

    port = random.randint(10000, 50000)
    config = UrbanAPIConfig.load(os.environ["CONFIG_PATH"])
    config = UrbanAPIConfig(
        app=AppConfig(
            host=config.app.host,
            port=port,
            debug=config.app.debug,
            name=config.app.name,
        ),
        db=database,
        auth=config.auth,
        fileserver=config.fileserver,
        external=config.external,
        logging=config.logging,
    )
    return config


@pytest.fixture(scope="session")
def urban_api_host(config) -> Iterator[str]:  # pylint: disable=redefined-outer-name
    """Fixture to start the urban_api HTTP server on random port with poetry command."""

    host = f"http://localhost:{config.app.port}"
    temp_yaml_config_path = os.path.join(tempfile.gettempdir(), os.urandom(24).hex())
    config.dump(temp_yaml_config_path)
    with subprocess.Popen(
        [
            # fmt: off
            "poetry", "run", "launch_urban_api",
            "--config_path", temp_yaml_config_path,
            # fmt: on
        ]
    ) as process:

        time.sleep(5)
        client = httpx.Client()
        max_attempts = 10

        try:
            for _ in range(max_attempts):
                time.sleep(1)
                if client.get(f"{host}/health_check/ping").is_success:
                    break
            else:
                pytest.fail("Failed to start urban_api server")
            yield host
        finally:
            if os.path.exists(temp_yaml_config_path):
                os.remove(temp_yaml_config_path)
            process.terminate()
            process.wait()


def run_migrations(database: DBConfig):  # pylint: disable=redefined-outer-name
    dsn = f"postgresql+asyncpg://{database.user}:{database.password}@{database.addr}:{database.port}/{database.name}"
    alembic_dir = Path(__file__).resolve().parent.parent.parent / "idu_api" / "common" / "db"

    alembic_cfg = Config(str(alembic_dir / "alembic.ini"))
    alembic_cfg.set_main_option("script_location", str(alembic_dir / "migrator"))

    try:
        alembic_cfg.set_main_option("sqlalchemy.url", dsn)
        command.upgrade(alembic_cfg, "head")
    except Exception:  # pylint: disable=broad-except
        pytest.fail("Error on migration preparation")
