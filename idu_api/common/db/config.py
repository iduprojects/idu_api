"""Database configuration class is defined here."""

from dataclasses import dataclass
from typing import Any


@dataclass
class DBConfig:
    host: str
    port: int
    database: str
    user: str
    password: str
    pool_size: int


@dataclass
class MultipleDBsConfig:
    master: DBConfig
    replicas: list[DBConfig] | None

    def __post_init__(self):
        _dict_to_dataclass(self, "master", DBConfig)
        if self.replicas is not None:
            _list_dict_to_dataclasses(self, "replicas", DBConfig)


def _list_dict_to_dataclasses(config_entry: Any, field_name: str, need_type: type) -> None:
    list_dict = getattr(config_entry, field_name)
    for i in range(len(list_dict)):  # pylint: disable=consider-using-enumerate
        if isinstance(list_dict[i], dict):
            list_dict[i] = need_type(**list_dict[i])


def _dict_to_dataclass(config_entry: Any, field_name: str, need_type: type) -> None:
    value = getattr(config_entry, field_name)
    if isinstance(value, dict):
        setattr(config_entry, field_name, need_type(**value))
