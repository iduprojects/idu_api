# pylint: disable=unused-import
import os
import random
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

import httpx
import pytest
from alembic import command
from alembic.config import Config

from tests.urban_api.projects.helpers.projects import *  # pylint: disable=wildcard-import,unused-wildcard-import


@dataclass
class DBConfig:
    host: str
    port: int
    user: str
    password: str
    database: str


@pytest.fixture(scope="session")
def database() -> DBConfig:
    """Fixture to get database credentials from environment variables."""
    if any(value not in os.environ for value in ("DB_ADDR", "DB_PORT", "DB_USER", "DB_PASS", "DB_NAME")):
        pytest.skip("Database for integration tests is not configured")

    db = DBConfig(
        host=os.environ["DB_ADDR"],
        port=int(
            os.environ["DB_PORT"],
        ),
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASS"],
        database=os.environ["DB_NAME"],
    )

    run_migrations(db)
    return db


@pytest.fixture(scope="session")
def urban_api_host(database) -> Iterator[str]:  # pylint: disable=redefined-outer-name
    """Fixture to start the urban_api HTTP server on random port with poetry command."""

    port = random.randint(10000, 50000)
    host = f"http://localhost:{port}"
    with subprocess.Popen(
        [
            # fmt: off
            "poetry", "run", "launch_urban_api",
            "--port", str(port),
            "--db_addr", database.host,
            "--db_port", str(database.port),
            "--db_user", database.user,
            "--db_pass", database.password,
            "--db_name", database.database,
            # fmt: on
        ]
    ) as process:
        client = httpx.Client()
        attempt = 0

        try:
            for attempt in range(10):
                time.sleep(1)
                if client.get(f"{host}/api/health_check/ping").is_success:
                    break
                if attempt == 10:
                    pytest.fail("Failed to start urban_api server")

            yield host
        finally:
            process.terminate()
            process.wait()


def run_migrations(database: DBConfig):  # pylint: disable=redefined-outer-name
    dsn = (
        f"postgresql+asyncpg://{database.user}:{database.password}@{database.host}:{database.port}/{database.database}"
    )
    alembic_dir = Path(__file__).resolve().parent.parent.parent / "idu_api" / "common" / "db"

    alembic_cfg = Config(str(alembic_dir / "alembic.ini"))
    alembic_cfg.set_main_option("script_location", str(alembic_dir / "migrator"))

    try:
        alembic_cfg.set_main_option("sqlalchemy.url", dsn)
        command.upgrade(alembic_cfg, "head")
    except Exception:  # pylint: disable=broad-except
        pytest.fail("Error on migration preparation")
