"""Api routers are defined here.

It is needed to import files which use these routers to initialize handlers.
"""

from fastapi import APIRouter

from .v1 import routers_list as v1
from .v2 import routers_list as v2

system_router = APIRouter(tags=["system"])

routers_list = [
    *v1,
    *v2,
    system_router,
]

__all__ = [
    "routers_list",
]
