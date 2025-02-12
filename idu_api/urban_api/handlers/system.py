"""System endpoints are defined here."""
from typing import Any

import fastapi
from fastapi import Request, HTTPException
from geojson_pydantic import Feature
from starlette import status

from idu_api.urban_api.schemas import PingResponse

from ..logic.system import SystemService
from ..schemas.geometries import AllPossibleGeometry, GeoJSONResponse, Geometry
from .routers import system_router


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


@system_router.post(
    "/fix/geometry",
    response_model=Geometry,
    status_code=status.HTTP_200_OK,
)
async def fix_geometry(request: Request, geometry: AllPossibleGeometry):
    """
    ## Fix invalid geometry using postgis methods.

    **NOTE:** This endpoint receives a geometry object, attempts to fix any potential issues
    (such as self-intersections or invalid topology), and returns a corrected geometry.

    ### Parameters:
    - **geometry** (AllPossibleGeometry, Body): Input geometry that needs to be fixed.
      NOTE: The geometry must have **SRID=4326**.

    ### Returns:
    - **Geometry**: The fixed geometry object.

    ### Errors:
    - **400 Bad Request**: If the provided geometry is invalid and cannot be fixed.
    """
    system_service: SystemService = request.state.system_service

    try:
        geom = await system_service.fix_geometry(geometry.as_shapely_geometry())
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))

    return Geometry.from_shapely_geometry(geom)


@system_router.post(
    "/fix/geojson",
    response_model=GeoJSONResponse[Feature[Geometry, Any]],
    status_code=status.HTTP_200_OK,
)
async def fix_geojson(request: Request, geojson: GeoJSONResponse[Feature[Geometry, Any]]):
    """
    ## Fix invalid geometries in a GeoJSON object.

    **NOTE:** This endpoint processes a GeoJSON object containing multiple features, attempts to fix
    any invalid geometries, and returns a corrected GeoJSON response.

    ### Parameters:
    - **geojson** (GeoJSONResponse[Feature[Geometry, Any]], Body): A GeoJSON object containing features
      with geometries that need to be fixed.
      NOTE: All geometries must have **SRID=4326**.

    ### Returns:
    - **GeoJSONResponse[Feature[Geometry, Any]]**: A new GeoJSON object with corrected geometries.

    ### Errors:
    - **400 Bad Request**: If any geometry is invalid and cannot be fixed.
    """
    system_service: SystemService = request.state.system_service

    try:
        geoms = [feature.geometry.as_shapely_geometry() for feature in geojson.features]
        geoms = await system_service.fix_geojson(geoms)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))

    return geojson.update_geometries(geoms)
