"""System endpoints are defined here."""

from typing import Any

import fastapi
from geojson_pydantic import Feature
from starlette import status

from idu_api.urban_api.schemas import PingResponse

from .routers import system_router
from ..schemas.geometries import Geometry, GeoJSONResponse, AllPossibleGeometry


@system_router.get("/", status_code=status.HTTP_307_TEMPORARY_REDIRECT, include_in_schema=False)
@system_router.get("/api/", status_code=status.HTTP_307_TEMPORARY_REDIRECT, include_in_schema=False)
async def redirect_to_swagger_docs():
    """Redirects to **/api/docs** from **/**"""
    return fastapi.responses.RedirectResponse("/api/docs", status_code=status.HTTP_307_TEMPORARY_REDIRECT)


@system_router.get(
    "/health_check/ping",
    response_model=PingResponse,
    status_code=status.HTTP_200_OK,
)
async def health_check():
    """
    Return health check response.
    """
    return PingResponse()


@system_router.get(
    "/fix/geometry",
    response_model=GeoJSONResponse,
    status_code=status.HTTP_200_OK,
)
async def fix_geometry(request: fastapi.Request, geometry: AllPossibleGeometry):
    """Returns fixed geometry response."""

    return Geometry()


@system_router.get(
    "/fix/geojson",
    response_model=GeoJSONResponse[Feature[Geometry, Any]],
    status_code=status.HTTP_200_OK,
)
async def fix_geojson(request: fastapi.Request, geojson: GeoJSONResponse[Feature[Geometry, Any]]):
    """Returns fixed geojson response."""

    return GeoJSONResponse(**{"type": "FeatureCollection", "features": []})
