from fastapi import APIRouter, Security
from fastapi.security import HTTPBearer

# projects_router = APIRouter(tags=["projects"], prefix="/v1")
projects_router = APIRouter(tags=["projects"], prefix="/v1", dependencies=[Security(HTTPBearer())])

routers_list = [
    projects_router,
]

__all__ = [
    "routers_list",
]
