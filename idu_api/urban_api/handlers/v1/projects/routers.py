from fastapi import APIRouter

projects_router = APIRouter(tags=["projects"], prefix="/v1")

routers_list = [
    projects_router,
]

__all__ = [
    "routers_list",
]
