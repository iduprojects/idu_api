"""Application configuration class is defined here."""

import os
from dataclasses import dataclass

from loguru import logger

from idu_api.urban_api.version import VERSION as api_version


@dataclass
class UrbanAPIConfig:  # pylint: disable=too-many-instance-attributes
    """
    Configuration class for application.
    """

    host: str = "0.0.0.0"
    port: int = 8000

    db_addr: str = "localhost"
    db_port: int = 5432
    db_name: str = "urban_db"
    db_user: str = "postgres"
    db_pass: str = "postgres"
    debug: bool = False
    db_connect_retry: int = 20
    db_pool_size: int = 15
    authentication_url: str = "http://10.32.1.100:8086/introspect"
    validate: int = 0
    cache_size: int = 100
    cache_ttl: int = 1800
    application_name = f"urban_api ({api_version})"


    @classmethod
    def try_from_env(cls) -> "UrbanAPIConfig":
        """Call default class constructor, and then try to find attributes
        values in environment variables by upper({name}).
        """
        res = cls()
        for param, value in res.__dict__.items():
            if (env := param.upper()) in os.environ:
                logger.trace("Getting {} from envvar: {}", param, os.environ[env])
                setattr(res, param, type(value)(os.environ[env]))
        return res

    def to_envfile(self, filename: str) -> None:
        """Save config values as envfile with given filename."""
        with open(filename, "w", encoding="utf-8") as file:
            for param, value in self.__dict__.items():
                env = param.upper()
                print(f"{env}={value}", file=file)

    def update(self, other: "UrbanAPIConfig") -> None:
        """
        Update current class attributes to the values of a given instance.
        """
        for param, value in other.__dict__.items():
            if param in self.__dict__:
                setattr(self, param, value)
