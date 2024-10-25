"""Application configuration class is defined here."""

import os
from collections import OrderedDict
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, TextIO

import yaml

from idu_api.urban_api.version import VERSION as api_version


@dataclass
class AppConfig:
    host: str = "0.0.0.0"
    port: int = 8000
    debug: int = 0
    logger_verbosity: Literal["TRACE", "DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
    name: str = f"urban_api ({api_version})"


@dataclass
class DBConfig:
    addr: str = "localhost"
    port: int = 5432
    name: str = "urban_db"
    user: str = "postgres"
    password: str = "postgres"
    pool_size: int = 15


@dataclass
class AuthConfig:
    url: str = "http://10.32.1.100:8086/introspect"
    validate: int = 0
    cache_size: int = 100
    cache_ttl: int = 1800


@dataclass
class FileServerConfig:
    host: str = "10.32.1.42"
    port: int = 9000
    projects_bucket: str = "projects.images"
    access_key: str = ""
    secret_key: str = ""


@dataclass
class UrbanAPIConfig:
    app: AppConfig
    db: DBConfig
    auth: AuthConfig
    fileserver: FileServerConfig

    def to_order_dict(self) -> OrderedDict:
        """OrderDict transformer."""

        return OrderedDict(
            [
                ("app", self._to_ordered_dict_recursive(self.app)),
                ("db", self._to_ordered_dict_recursive(self.db)),
                ("auth", self._to_ordered_dict_recursive(self.auth)),
                ("fileserver", self._to_ordered_dict_recursive(self.fileserver)),
            ]
        )

    def _to_ordered_dict_recursive(self, obj) -> OrderedDict:
        """Recursive OrderDict transformer."""

        if isinstance(obj, (dict, OrderedDict)):
            return OrderedDict((k, self._to_ordered_dict_recursive(v)) for k, v in obj.items())
        if hasattr(obj, "__dataclass_fields__"):
            return OrderedDict(
                (field, self._to_ordered_dict_recursive(getattr(obj, field))) for field in obj.__dataclass_fields__
            )
        return obj

    def dump(self, file: str | Path | TextIO) -> None:
        """Export current configuration to a file"""

        class OrderedDumper(yaml.SafeDumper):
            def represent_dict_preserve_order(self, data):
                return self.represent_dict(data.items())

        OrderedDumper.add_representer(OrderedDict, OrderedDumper.represent_dict_preserve_order)

        if isinstance(file, (str, Path)):
            with open(str(file), "w", encoding="utf-8") as file_w:
                yaml.dump(self.to_order_dict(), file_w, Dumper=OrderedDumper, default_flow_style=False)
        else:
            yaml.dump(self.to_order_dict(), file, Dumper=OrderedDumper, default_flow_style=False)

    @classmethod
    def example(cls) -> "UrbanAPIConfig":
        """Generate an example of configuration."""

        return cls(app=AppConfig(), db=DBConfig(), auth=AuthConfig(), fileserver=FileServerConfig())

    @classmethod
    def load(cls, file: str | Path | TextIO) -> "UrbanAPIConfig":
        """Import config from the given filename or raise `ValueError` on error."""

        try:
            if isinstance(file, (str, Path)):
                with open(file, "r", encoding="utf-8") as file_r:
                    data = yaml.safe_load(file_r)
            else:
                data = yaml.safe_load(file)

            return cls(
                app=AppConfig(**data.get("app", {})),
                db=DBConfig(**data.get("db", {})),
                auth=AuthConfig(**data.get("auth", {})),
                fileserver=FileServerConfig(**data.get("fileserver", {})),
            )
        except Exception as exc:
            raise ValueError("Could not read app config file") from exc

    @classmethod
    def try_from_env(cls) -> "UrbanAPIConfig":
        """Try to load configuration from the path specified in the environment variable."""

        config_path = os.getenv("CONFIG_PATH")  # Ensure this matches your .env variable name
        if not config_path:
            return cls.example()

        return cls.load(config_path)

    def update(self, other: "UrbanAPIConfig") -> None:
        """Update current config attributes with the values from another UrbanAPIConfig instance."""
        for section in ("app", "db", "auth", "fileserver"):
            current_subconfig = getattr(self, section)
            other_subconfig = getattr(other, section)

            for param, value in other_subconfig.__dict__.items():
                if param in current_subconfig.__dict__:
                    setattr(current_subconfig, param, value)
