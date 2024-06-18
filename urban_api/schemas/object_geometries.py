from typing import Optional

from loguru import logger
from pydantic import BaseModel, Field, field_validator, model_validator

from urban_api.dto import ObjectGeometryDTO
from urban_api.schemas.geometries import Geometry


class ObjectGeometries(BaseModel):
    object_geometry_id: int = Field(example=1)
    territory_id: int = Field(example=1)
    address: Optional[str] = Field(None, description="Physical object address", example="--")
    geometry: Geometry = Field(description="Object geometry")
    centre_point: Geometry = Field(description="Centre coordinates")

    @classmethod
    def from_dto(cls, dto: ObjectGeometryDTO) -> "ObjectGeometries":
        """
        Construct from DTO.
        """
        return cls(
            object_geometry_id=dto.object_geometry_id,
            territory_id=dto.territory_id,
            address=dto.address,
            geometry=Geometry.from_shapely_geometry(dto.geometry),
            centre_point=Geometry.from_shapely_geometry(dto.centre_point),
        )


class ObjectGeometriesPut(BaseModel):
    territory_id: int = Field(..., example=1)
    geometry: Geometry = Field(..., description="Object geometry")
    centre_point: Optional[Geometry] = Field(..., description="Centre coordinates")
    address: Optional[str] = Field(..., description="Physical object address", example="--")

    @field_validator("geometry")
    @staticmethod
    def validate_geometry(geometry: Geometry) -> Geometry:
        """
        Validate that given geometry is validity via creating Shapely object.
        """
        try:
            geometry.as_shapely_geometry()
        except (AttributeError, ValueError, TypeError) as exc:
            logger.debug("Exception on passing geometry: {!r}", exc)
            raise ValueError("Invalid geometry passed") from exc
        return geometry

    @field_validator("centre_point")
    @staticmethod
    def validate_center(centre_point: Geometry | None) -> Optional[Geometry]:
        """
        Validate that given geometry is Point and validity via creating Shapely object.
        """
        if centre_point is None:
            return None
        assert centre_point.type == "Point", "Only Point is accepted"
        try:
            centre_point.as_shapely_geometry()
        except (AttributeError, ValueError, TypeError) as exc:
            logger.debug("Exception on passing geometry: {!r}", exc)
            raise ValueError("Invalid geometry passed") from exc
        return centre_point

    @model_validator(mode="after")
    @staticmethod
    def validate_post(model: "ObjectGeometriesPut") -> "ObjectGeometriesPut":
        """
        Use geometry centroid for centre_point if it is missing.
        """
        if model.centre_point is None:
            model.centre_point = Geometry.from_shapely_geometry(model.geometry.as_shapely_geometry().centroid)
        return model


class ObjectGeometriesPatch(BaseModel):
    territory_id: Optional[int] = Field(None, example=1)
    geometry: Optional[Geometry] = Field(None, description="Object geometry")
    centre_point: Optional[Geometry] = Field(None, description="Centre coordinates")
    address: Optional[str] = Field(None, description="Physical object address", example="--")

    @model_validator(mode="before")
    @classmethod
    def check_empty_request(cls, values):
        """
        Ensure the request body is not empty.
        """
        if not values:
            raise ValueError("request body cannot be empty")
        return values

    @model_validator(mode="before")
    @classmethod
    def disallow_nulls(cls, values):
        """
        Ensure the request body hasn't nulls.
        """
        for k, v in values.items():
            if v is None:
                raise ValueError(f"{k} cannot be null")
        return values

    @field_validator("geometry")
    @staticmethod
    def validate_geometry(geometry: Geometry) -> Optional[Geometry]:
        """
        Validate that given geometry is validity via creating Shapely object.
        """
        if geometry is None:
            return None
        try:
            geometry.as_shapely_geometry()
        except (AttributeError, ValueError, TypeError) as exc:
            logger.debug("Exception on passing geometry: {!r}", exc)
            raise ValueError("Invalid geometry passed") from exc
        return geometry

    @field_validator("centre_point")
    @staticmethod
    def validate_center(centre_point: Geometry | None) -> Optional[Geometry]:
        """
        Validate that given geometry is Point and validity via creating Shapely object.
        """
        if centre_point is None:
            return None
        assert centre_point.type == "Point", "Only Point is accepted"
        try:
            centre_point.as_shapely_geometry()
        except (AttributeError, ValueError, TypeError) as exc:
            logger.debug("Exception on passing geometry: {!r}", exc)
            raise ValueError("Invalid geometry passed") from exc
        return centre_point

    @model_validator(mode="after")
    @staticmethod
    def validate_post(model: "ObjectGeometriesPatch") -> "ObjectGeometriesPatch":
        """
        Use geometry centroid for centre_point if it is missing.
        """
        if model.centre_point is None and model.geometry is not None:
            model.centre_point = Geometry.from_shapely_geometry(model.geometry.as_shapely_geometry().centroid)
        return model
