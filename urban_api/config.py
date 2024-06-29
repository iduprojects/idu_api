"""Application configuration class is defined here."""

import os
from dataclasses import dataclass

from loguru import logger

from urban_api.version import VERSION as api_version


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

    def to_env(self) -> None:
        """Call default class constructor, and then tries to find attributes
        values in environment variables by upper({name}).
        """
        for param, value in self.__dict__.items():
            env = param.upper()
            os.environ[env] = str(value)

    def update(self, other: "UrbanAPIConfig") -> None:
        """
        Update current class attributes to the values of a given instance.
        """
        for param, value in other.__dict__.items():
            if param in self.__dict__:
                setattr(self, param, value)
