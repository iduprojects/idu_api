"""
All FastApi endpoints are exported from this module.
"""
import importlib
from pathlib import Path

from .routers import routers_list


for file in sorted(Path(__file__).resolve().parent.iterdir()):
    if file.name.endswith(".py"):
        importlib.import_module(f".{file.name[:-3]}", __package__)


list_of_routes = [
    *routers_list,
]


__all__ = [
    "list_of_routes",
]
