"""Application configuration class is defined here."""

import os
from collections import OrderedDict
from dataclasses import dataclass, field
from pathlib import Path
from typing import TextIO

import yaml

from idu_api.urban_api.version import VERSION as api_version

from .utils.logging import LoggingLevel


@dataclass
class AppConfig:
    host: str
    port: int
    debug: bool
    name: str

    def __post_init__(self):
        self.name = f"urban_api ({api_version})"


@dataclass
class DBConfig:
    addr: str
    port: int
    name: str
    user: str
    password: str
    pool_size: int


@dataclass
class AuthConfig:
    url: str
    validate: bool
    cache_size: int
    cache_ttl: int


@dataclass
class FileServerConfig:
    host: str
    port: int
    projects_bucket: str
    access_key: str
    secret_key: str
    region_name: str
    connect_timeout: int
    read_timeout: int
    retries: int


@dataclass
class ExternalServicesConfig:
    gen_planner_api: str
    hextech_api: str


@dataclass
class FileLogger:
    filename: str
    level: LoggingLevel


@dataclass
class LoggingConfig:
    level: LoggingLevel
    files: list[FileLogger] = field(default_factory=list)

    def __post_init__(self):
        if len(self.files) > 0 and isinstance(self.files[0], dict):
            self.files = [FileLogger(**f) for f in self.files]


@dataclass
class UrbanAPIConfig:
    app: AppConfig
    db: DBConfig
    auth: AuthConfig
    fileserver: FileServerConfig
    external: ExternalServicesConfig
    logging: LoggingConfig

    def to_order_dict(self) -> OrderedDict:
        """OrderDict transformer."""

        def to_ordered_dict_recursive(obj):
            if isinstance(obj, (dict, OrderedDict)):
                return OrderedDict((k, to_ordered_dict_recursive(v)) for k, v in obj.items())
            elif isinstance(obj, list):
                return [to_ordered_dict_recursive(item) for item in obj]
            elif hasattr(obj, "__dataclass_fields__"):
                return OrderedDict(
                    (field, to_ordered_dict_recursive(getattr(obj, field))) for field in obj.__dataclass_fields__
                )
            else:
                return obj

        return OrderedDict(
            [
                ("app", to_ordered_dict_recursive(self.app)),
                ("db", to_ordered_dict_recursive(self.db)),
                ("auth", to_ordered_dict_recursive(self.auth)),
                ("fileserver", to_ordered_dict_recursive(self.fileserver)),
                ("external", to_ordered_dict_recursive(self.external)),
                ("logging", to_ordered_dict_recursive(self.logging)),
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
            app=AppConfig(host="0.0.0.0", port=8000, debug=False, name="urban_api"),
            db=DBConfig(
                addr="localhost", port=5432, name="urban_db", user="postgres", password="postgres", pool_size=15
            ),
            auth=AuthConfig(url="", validate=False, cache_size=100, cache_ttl=1800),
            fileserver=FileServerConfig(
                host="",
                port=9000,
                projects_bucket="",
                access_key="",
                secret_key="",
                region_name="",
                connect_timeout=5,
                read_timeout=20,
                retries=3,
            ),
            external=ExternalServicesConfig(gen_planner_api="", hextech_api=""),
            logging=LoggingConfig(level="INFO"),
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
                logging=LoggingConfig(**data.get("logging", {})),
            )
        except Exception as exc:
            raise ValueError(f"Could not read app config file: {file}") from exc

    @classmethod
    def from_file_or_default(cls, config_path: str = os.getenv("CONFIG_PATH")) -> "UrbanAPIConfig":
        """Try to load configuration from the path specified in the environment variable."""

        if not config_path:
            return cls.example()

        return cls.load(config_path)

    def update(self, other: "UrbanAPIConfig") -> None:
        """Update current config attributes with the values from another UrbanAPIConfig instance."""
        for section in ("app", "db", "auth", "fileserver", "logging"):
            current_subconfig = getattr(self, section)
            other_subconfig = getattr(other, section)

            for param, value in other_subconfig.__dict__.items():
                if param in current_subconfig.__dict__:
                    setattr(current_subconfig, param, value)
