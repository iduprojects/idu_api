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
    name: str = "urban_api"

    def __post_init__(self):
        self.name = f"urban_api ({api_version})"


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
    url: str = ""
    validate: int = 0
    cache_size: int = 100
    cache_ttl: int = 1800


@dataclass
class FileServerConfig:
    host: str = ""
    port: int = 9000
    projects_bucket: str = ""
    access_key: str = ""
    secret_key: str = ""
    region_name: str = ""
    connect_timeout: int = 5
    read_timeout: int = 20
    retries: int = 3


@dataclass
class ExternalServicesConfig:
    gen_planner_api: str = "http://10.32.1.102"
    hextech_api: str = "http://10.32.1.48:8100"


@dataclass
class UrbanAPIConfig:
    app: AppConfig
    db: DBConfig
    auth: AuthConfig
    fileserver: FileServerConfig
    external: ExternalServicesConfig

    def to_order_dict(self) -> OrderedDict:
        """OrderDict transformer."""

        def to_ordered_dict_recursive(obj) -> OrderedDict:
            """Recursive OrderDict transformer."""

            if isinstance(obj, (dict, OrderedDict)):
                return OrderedDict((k, to_ordered_dict_recursive(v)) for k, v in obj.items())
            if hasattr(obj, "__dataclass_fields__"):
                return OrderedDict(
                    (field, to_ordered_dict_recursive(getattr(obj, field))) for field in obj.__dataclass_fields__
                )
            return obj

        return OrderedDict(
            [
                ("app", to_ordered_dict_recursive(self.app)),
                ("db", to_ordered_dict_recursive(self.db)),
                ("auth", to_ordered_dict_recursive(self.auth)),
                ("fileserver", to_ordered_dict_recursive(self.fileserver)),
                ("external", to_ordered_dict_recursive(self.external)),
            ]
        )

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

        return cls(
            app=AppConfig(),
            db=DBConfig(),
            auth=AuthConfig(),
            fileserver=FileServerConfig(),
            external=ExternalServicesConfig(),
        )

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
                external=ExternalServicesConfig(**data.get("external", {})),
            )
        except Exception as exc:
            raise ValueError("Could not read app config file") from exc

    @classmethod
    def from_file_or_default(cls, config_path: str = os.getenv("CONFIG_PATH")) -> "UrbanAPIConfig":
        """Try to load configuration from the path specified in the environment variable."""

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
