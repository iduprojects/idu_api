"""
Api routers are defined here.

It is needed to import files which use these routers to initialize endpoints.
"""
from fastapi import APIRouter

system_router = APIRouter(tags=["system"])

routers_list = [
    system_router
]

__all__ = [
    "routers_list",
]
