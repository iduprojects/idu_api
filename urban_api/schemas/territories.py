"""
Territory schemas are defined here.
"""

from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional

from loguru import logger
from pydantic import BaseModel, Field, field_validator, model_validator

from urban_api.dto import TerritoryDTO, TerritoryTypeDTO, TerritoryWithoutGeometryDTO
from urban_api.schemas.geometries import Geometry


class TerritoriesOrderByField(str, Enum):
    CREATED_AT = "created_at"
    UPDATED_AT = "updated_at"


class TerritoryType(BaseModel):
    """Territory type with all its attributes."""

    territory_type_id: Optional[int] = Field(example=1, description="Territory type id, if set")
    name: str = Field(description="Territory type unit name", example="Город")

    @classmethod
    def from_dto(cls, dto: TerritoryTypeDTO) -> "TerritoryType":
        """Construct from DTO."""
        return cls(territory_type_id=dto.territory_type_id, name=dto.name)


class TerritoryShortInfo(BaseModel):
    """Minimal territory information - identifier and name."""

    id: Optional[int] = Field(example=1, description="Territory identifier")
    name: str = Field(description="Territory name", example="Санкт-Петербург")

    @classmethod
    def from_dto(cls, dto: TerritoryDTO) -> "TerritoryShortInfo":
        """Construct from DTO."""
        return cls(territory_type_id=dto.territory_type_id, name=dto.name)


class TerritoryTypesPost(BaseModel):
    """
    Schema of territory type for POST request
    """

    name: str = Field(description="Territory type unit name", example="Город")


class TerritoryData(BaseModel):
    """Territory with all its attributes."""

    territory_id: int = Field(examples=[1])
    territory_type: TerritoryType = Field(example={"territory_type_id": 1, "name": "name"})
    parent: TerritoryShortInfo | None = Field(
        description="Parent territory short information", example=TerritoryShortInfo(id=1, name="Россия")
    )
    name: str = Field(example="--", description="Territory name")
    geometry: Geometry = Field(description="Territory geometry")
    level: int = Field(example=1)
    properties: Dict[str, Any] = Field(
        description="Service additional properties",
        example={"additional_attribute_name": "additional_attribute_value"},
    )
    centre_point: Geometry = Field(description="Centre coordinates")
    admin_center: Optional[int] = Field(example=1)
    okato_code: Optional[str] = Field(example="1")
    created_at: datetime = Field(default_factory=datetime.utcnow, description="The time when the territory was created")
    updated_at: datetime = Field(
        default_factory=datetime.utcnow, description="The time when the territory was last updated"
    )

    @classmethod
    def from_dto(cls, dto: TerritoryDTO) -> "TerritoryData":
        """Construct from DTO."""

        return cls(
            territory_id=dto.territory_id,
            territory_type=TerritoryType(territory_type_id=dto.territory_type_id, name=dto.territory_type_name),
            parent=(TerritoryShortInfo(id=dto.parent_id, name=dto.parent_name) if dto.parent_id is not None else None),
            name=dto.name,
            geometry=Geometry.from_shapely_geometry(dto.geometry),
            level=dto.level,
            properties=dto.properties,
            centre_point=Geometry.from_shapely_geometry(dto.centre_point),
            admin_center=dto.admin_center,
            okato_code=dto.okato_code,
            created_at=dto.created_at,
            updated_at=dto.updated_at,
        )


class TerritoryDataPost(BaseModel):
    """Schema of territory for POST request."""

    territory_type_id: int = Field(example=1)
    parent_id: Optional[int] = Field(example=1)
    name: str = Field(example="--", description="Territory name")
    geometry: Geometry = Field(description="Territory geometry")
    level: int = Field(example=1)
    properties: Dict[str, Any] = Field(
        default_factory=dict,
        description="Service additional properties",
        example={"additional_attribute_name": "additional_attribute_value"},
    )
    centre_point: Optional[Geometry] = Field(None, description="Centre coordinates")
    admin_center: Optional[int] = Field(None, example=1)
    okato_code: Optional[str] = Field(None, example="1")

    @field_validator("geometry")
    @staticmethod
    def validate_geometry(geometry: Geometry) -> Geometry:
        """Validate that given geometry is validity via creating Shapely object."""

        try:
            geometry.as_shapely_geometry()
        except (AttributeError, ValueError, TypeError) as exc:
            logger.debug("Exception on passing geometry: {!r}", exc)
            raise ValueError("Invalid geometry passed") from exc
        return geometry

    @field_validator("centre_point")
    @staticmethod
    def validate_center(centre_point: Geometry | None) -> Optional[Geometry]:
        """Validate that given geometry is Point and validity via creating Shapely object."""

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
    def validate_post(model: "TerritoryDataPost") -> "TerritoryDataPost":
        """Use geometry centroid for centre_point if it is missing."""

        if model.centre_point is None:
            model.centre_point = Geometry.from_shapely_geometry(model.geometry.as_shapely_geometry().centroid)
        return model


class TerritoryDataPut(BaseModel):
    """Schema of territory for POST request."""

    territory_type_id: int = Field(..., example=1)
    parent_id: Optional[int] = Field(..., example=1)
    name: str = Field(..., example="--", description="Territory name")
    geometry: Geometry = Field(..., description="Territory geometry")
    level: int = Field(..., example=1)
    properties: Dict[str, Any] = Field(
        ...,
        description="Service additional properties",
        example={"additional_attribute_name": "additional_attribute_value"},
    )
    centre_point: Optional[Geometry] = Field(..., description="Centre coordinates")
    admin_center: Optional[int] = Field(..., example=1)
    okato_code: Optional[str] = Field(..., example="1")

    @field_validator("geometry")
    @staticmethod
    def validate_geometry(geometry: Geometry) -> Geometry:
        """Validate that given geometry is validity via creating Shapely object."""

        try:
            geometry.as_shapely_geometry()
        except (AttributeError, ValueError, TypeError) as exc:
            logger.debug("Exception on passing geometry: {!r}", exc)
            raise ValueError("Invalid geometry passed") from exc
        return geometry

    @field_validator("centre_point")
    @staticmethod
    def validate_center(centre_point: Geometry | None) -> Optional[Geometry]:
        """Validate that given geometry is Point and validity via creating Shapely object."""

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
    def validate_post(model: "TerritoryDataPost") -> "TerritoryDataPost":
        """Use geometry centroid for centre_point if it is missing."""

        if model.centre_point is None:
            model.centre_point = Geometry.from_shapely_geometry(model.geometry.as_shapely_geometry().centroid)
        return model


class TerritoryDataPatch(BaseModel):
    """Schema of territory for POST request."""

    territory_type_id: Optional[int] = Field(None, example=1)
    parent_id: Optional[int] = Field(None, example=1)
    name: Optional[str] = Field(None, example="--", description="Territory name")
    geometry: Optional[Geometry] = Field(None, description="Territory geometry")
    level: Optional[int] = Field(None, example=1)
    properties: Optional[Dict[str, Any]] = Field(
        None,
        description="Service additional properties",
        example={"additional_attribute_name": "additional_attribute_value"},
    )
    centre_point: Optional[Geometry] = Field(None, description="Centre coordinates")
    admin_center: Optional[int] = Field(None, example=1)
    okato_code: Optional[str] = Field(None, example="1")

    @model_validator(mode="before")
    @classmethod
    def check_empty_request(cls, values):
        """Ensure the request body is not empty."""

        if not values:
            raise ValueError("request body cannot be empty")
        return values

    @model_validator(mode="before")
    @classmethod
    def disallow_nulls(cls, values):
        """Ensure the request body hasn't nulls."""

        for k, v in values.items():
            if v is None:
                raise ValueError(f"{k} cannot be null")
        return values

    @field_validator("geometry")
    @staticmethod
    def validate_geometry(geometry: Optional[Geometry]) -> Optional[Geometry]:
        """Validate that given geometry is validity via creating Shapely object."""

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
    def validate_center(centre_point: Geometry | None) -> Geometry | None:
        """Validate that given geometry is Point and validity via creating Shapely object."""

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
    def validate_post(model: "TerritoryDataPost") -> "TerritoryDataPost":
        """Use geometry centroid for centre_point if it is missing."""
        if model.centre_point is None and model.geometry is not None:
            model.centre_point = Geometry.from_shapely_geometry(model.geometry.as_shapely_geometry().centroid)
        return model


class TerritoryWithoutGeometry(BaseModel):
    """Territory with all its attributes, but without center and geometry."""

    territory_id: int = Field(examples=[1])
    territory_type: TerritoryType = Field(example={"territory_type_id": 1, "name": "name"})
    parent_id: Optional[int] = Field(
        examples=[1], description="Parent territory identifier, null only for the one territory"
    )
    name: str = Field(examples=["--"], description="Territory name")
    level: int = Field(examples=[1])
    properties: Dict[str, Any] = Field(
        description="Service additional properties",
        example={"additional_attribute_name": "additional_attribute_value"},
    )
    admin_center: Optional[int] = Field(examples=[1])
    okato_code: Optional[str] = Field(examples=["1"])
    created_at: datetime = Field(default_factory=datetime.utcnow, description="The time when the territory was created")
    updated_at: datetime = Field(
        default_factory=datetime.utcnow, description="The time when the territory was last updated"
    )

    @classmethod
    def from_dto(cls, dto: TerritoryWithoutGeometryDTO) -> "TerritoryWithoutGeometry":
        """Construct from DTO."""
        return cls(
            territory_id=dto.territory_id,
            territory_type=TerritoryType(territory_type_id=dto.territory_type_id, name=dto.territory_type_name),
            parent_id=dto.parent_id,
            name=dto.name,
            level=dto.level,
            properties=dto.properties,
            admin_center=dto.admin_center,
            okato_code=dto.okato_code,
            created_at=dto.created_at,
            updated_at=dto.updated_at,
        )
