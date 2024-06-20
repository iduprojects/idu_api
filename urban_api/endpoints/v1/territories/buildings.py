"""Buildings territories-related endpoints are defined here."""

from fastapi import Depends, Path
from fastapi_pagination import paginate
from sqlalchemy.ext.asyncio import AsyncConnection
from starlette import status

from urban_api.db.connection import get_connection
from urban_api.logic.territories import get_living_buildings_with_geometry_by_territory_id_from_db
from urban_api.schemas import LivingBuildingsWithGeometry
from urban_api.schemas.pages import Page

from .routers import territories_router


@territories_router.get(
    "/territory/{territory_id}/living_buildings_with_geometry",
    response_model=Page[LivingBuildingsWithGeometry],
    status_code=status.HTTP_200_OK,
)
async def get_living_buildings_with_geometry_by_territory_id(
    territory_id: int = Path(description="territory id", gt=0),
    connection: AsyncConnection = Depends(get_connection),
) -> Page[LivingBuildingsWithGeometry]:
    """
    Summary:
        Get living buildings with geometry for territory

    Description:
        Get living buildings for territory
    """

    buildings = await get_living_buildings_with_geometry_by_territory_id_from_db(territory_id, connection)
    buildings = [LivingBuildingsWithGeometry.from_dto(building) for building in buildings]

    return paginate(buildings)
