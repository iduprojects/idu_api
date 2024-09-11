from typing import Annotated

from fastapi import Path, Query

from idu_api.city_api import app
from fastapi.requests import Request

tag = ["territory-level-controller"]


# @app.get("/city/{city}/level/{territory_id}/type-hierarchy")
# async def get_type_hierarchy_by_territory_id(
#         request: Request,
#         territory_id: Annotated[int, Path(gt=0)],
#         no_geometry: Annotated[bool, Query(description="return no geometry")] = False,
# ) -> list[TerritoriesData | TerritoriesWithoutGeometryData]:
#     return
