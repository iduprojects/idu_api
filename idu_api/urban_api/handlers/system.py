"""System endpoints are defined here."""

import io
import json
import os
import zipfile
from typing import Any

import fastapi
import shapely
from fastapi import File, HTTPException, Request, UploadFile
from fastapi.responses import StreamingResponse
from geojson_pydantic import Feature
from starlette import status

from idu_api.urban_api.exceptions.base import IduApiError
from idu_api.urban_api.schemas import PingResponse

from ..logic.system import SystemService
from ..schemas.geometries import AllPossibleGeometry, GeoJSONResponse
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


@system_router.post("/debug/raise_error")
async def raise_error(idu_api_error: bool = True):
    if idu_api_error:
        raise IduApiError()
    raise RuntimeError("Something really unexpected occured")


@system_router.post(
    "/fix/geometry",
    response_model=AllPossibleGeometry,
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
    - **AllPossibleGeometry**: The fixed geometry object.

    ### Errors:
    - **400 Bad Request**: If the provided geometry is invalid and cannot be fixed.
    """
    system_service: SystemService = request.state.system_service

    try:
        geom = await system_service.fix_geometry(geometry.as_shapely_geometry())
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e)) from e

    return AllPossibleGeometry.from_shapely_geometry(geom)


@system_router.post(
    "/fix/geojson",
    response_model=GeoJSONResponse[Feature[AllPossibleGeometry, Any]],
    status_code=status.HTTP_200_OK,
)
async def fix_geojson(request: Request, geojson: GeoJSONResponse[Feature[AllPossibleGeometry, Any]]):
    """
    ## Fix invalid geometries in a GeoJSON object.

    **NOTE:** This endpoint processes a GeoJSON object containing multiple features, attempts to fix
    any invalid geometries, and returns a corrected GeoJSON response.

    ### Parameters:
    - **geojson** (GeoJSONResponse[Feature[AllPossibleGeometry, Any]], Body): A GeoJSON object containing features
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
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e)) from e

    return geojson.update_geometries(geoms)


@system_router.post(
    "/fix/geojson_file",
    status_code=status.HTTP_200_OK,
)
async def fix_geojson_file(request: Request, file: UploadFile = File(...)):
    """
    ## Fix invalid geometries in a GeoJSON file.

    **NOTE:** This endpoint accepts a GeoJSON file containing multiple features with geometries,
    attempts to fix any invalid geometries, and returns a corrected GeoJSON file.

    ### Parameters:
    - **file** (UploadFile, Body): A GeoJSON file containing features with geometries to be fixed.
      NOTE: All geometries must have **SRID=4326**.

    ### Returns:
    - A file containing the corrected GeoJSON object.

    ### Errors:
    - **400 Bad Request**: If any geometry is invalid and cannot be fixed.
    """
    system_service: SystemService = request.state.system_service

    try:
        content = await file.read()
        geojson_data = json.loads(content)

        geojson = GeoJSONResponse(**geojson_data)
        geoms = [
            shapely.from_geojson(
                json.dumps({"type": feature["geometry"]["type"], "coordinates": feature["geometry"]["coordinates"]})
            )
            for feature in geojson_data["features"]
        ]
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid GeoJSON file") from e

    geoms = await system_service.fix_geojson(geoms)
    fixed_geojson = geojson.update_geometries(geoms)

    fixed_content = json.dumps(fixed_geojson.model_dump_json(indent=2), ensure_ascii=False).encode("utf-8")
    file_name, file_ext = os.path.splitext(os.path.basename(file.filename))

    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w") as zip_file:
        zip_file.writestr(f"{file_name}.{file_ext}", io.BytesIO(fixed_content).read())
    zip_buffer.seek(0)

    return StreamingResponse(
        zip_buffer,
        media_type="application/zip",
        headers={"Content-Disposition": f"attachment; filename={file_name}.zip"},
    )
