"""
Application configuration class is defined here.
"""
import os
from dataclasses import dataclass
from loguru import logger

from test_fastapi import __version__ as api_version


@dataclass
class AppSettings:
    """
    Configuration class for application.
    """

    host: str = "0.0.0.0"
    port: int = 8000

    db_addr: str = "notes_db"
    db_port: int = 5432
    db_name: str = "notes_db"
    db_user: str = "postgres"
    db_pass: str = "1111"
    debug: bool = False
    db_connect_retry: int = 20
    db_pool_size: int = 15
    keycloak_server_url: str = "http://localhost:8080/"
    realm: str = "test"
    client_id: str = "urban_api"
    client_secret: str = ""
    _authorization_url: str = f"realms/{realm}/protocol/openid-connect/auth"
    _token_url: str = f"/realms/{realm}/protocol/openid-connect/token"
    application_name = f"urban_api ({api_version})"

    def __post_init__(self):
        self._authorization_url = self._authorization_url.format(realm=self.realm)
        self._token_url = self._token_url.format(realm=self.realm)

    @property
    def database_settings(self) -> dict[str, str | int]:
        """
        Get all settings for connection with database.
        """
        return {
            "host": self.db_addr,
            "port": self.db_port,
            "database": self.db_name,
            "user": self.db_user,
            "password": self.db_pass,
        }

    @property
    def database_uri(self) -> str:
        """
        Get uri for connection with database.
        """
        return ("postgresql+asyncpg://{user}:{password}@{host}:{port}/{database}".format(
            **self.database_settings))

    @property
    def database_uri_sync(self) -> str:
        """
        Get uri for connection with database.
        """
        return "postgresql://{user}:{password}@{host}:{port}/{database}".format(**self.database_settings)

    @classmethod
    def try_from_env(cls) -> "AppSettings":
        """
        Call default class constructor, and then tries to find attributes
        values in environment variables by upper({name}).
        """
        res = cls()
        for param, value in res.__dict__.items():
            if (env := param.upper()) in os.environ:
                logger.trace("Getting {} from envvar: {}", param, os.environ[env])
                setattr(res, param, type(value)(os.environ[env]))
        return res

    def update(self, other: "AppSettings") -> None:
        """
        Update current class attributes to the values of a given instance.
        """
        for param, value in other.__dict__.items():
            if param in self.__dict__:
                setattr(self, param, value)


__all__ = [
    "AppSettings",
]
