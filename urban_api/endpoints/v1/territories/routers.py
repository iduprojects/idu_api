from fastapi import APIRouter

territories_router = APIRouter(tags=["territories"], prefix="/v1")

routers_list = [
    territories_router,
]

__all__ = [
    "routers_list",
]
