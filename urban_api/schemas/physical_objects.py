from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional

from loguru import logger
from pydantic import BaseModel, Field, field_validator, model_validator

from urban_api.dto import (
    PhysicalObjectDataDTO,
    PhysicalObjectTypeDTO,
    PhysicalObjectWithGeometryDTO,
    PhysicalObjectWithTerritoryDTO,
)
from urban_api.schemas.geometries import Geometry
from urban_api.schemas.territories import ShortTerritory


class PhysicalObjectsOrderByField(str, Enum):
    CREATED_AT = "created_at"
    UPDATED_AT = "updated_at"


class PhysicalObjectsTypes(BaseModel):
    """
    Physical object type with all its attributes
    """

    physical_object_type_id: Optional[int] = Field(description="Physical object type id, if set", example=1)
    name: str = Field(description="Physical object type unit name", example="Здание")

    @classmethod
    def from_dto(cls, dto: PhysicalObjectTypeDTO) -> "PhysicalObjectsTypes":
        """
        Construct from DTO.
        """
        return cls(physical_object_type_id=dto.physical_object_type_id, name=dto.name)


class PhysicalObjectsTypesPost(BaseModel):
    """
    Schema of physical object type for POST request
    """

    name: str = Field(description="Physical object type unit name", example="Здание")


class PhysicalObjectsData(BaseModel):
    """
    Physical object with all its attributes
    """

    physical_object_id: int = Field(example=1)
    physical_object_type: PhysicalObjectsTypes = Field(example={"physical_object_type_id": 1, "name": "Здание"})
    name: Optional[str] = Field(None, description="Physical object name", example="--")
    properties: Dict[str, Any] = Field(
        {},
        description="Physical object additional properties",
        example={"additional_attribute_name": "additional_attribute_value"},
    )
    created_at: datetime = Field(default_factory=datetime.utcnow, description="The time when the territory was created")
    updated_at: datetime = Field(
        default_factory=datetime.utcnow, description="The time when the territory was last updated"
    )

    @classmethod
    def from_dto(cls, dto: PhysicalObjectDataDTO) -> "PhysicalObjectsData":
        """
        Construct from DTO.
        """
        return cls(
            physical_object_id=dto.physical_object_id,
            physical_object_type=PhysicalObjectsTypes(
                physical_object_type_id=dto.physical_object_type_id, name=dto.physical_object_type_name
            ),
            name=dto.name,
            properties=dto.properties,
            created_at=dto.created_at,
            updated_at=dto.updated_at,
        )


class PhysicalObjectsWithTerritory(BaseModel):
    """
    Physical object with all its attributes and parent territory
    """

    physical_object_id: int = Field(example=1)
    physical_object_type: PhysicalObjectsTypes = Field(example={"physical_object_type_id": 1, "name": "Здание"})
    name: Optional[str] = Field(None, description="Physical object name", example="--")
    properties: Dict[str, Any] = Field(
        {},
        description="Physical object additional properties",
        example={"additional_attribute_name": "additional_attribute_value"},
    )
    territories: list[ShortTerritory] = Field(example=[{"territory_id": 1, "name": "Санкт-Петербург"}])
    created_at: datetime = Field(default_factory=datetime.utcnow, description="The time when the territory was created")
    updated_at: datetime = Field(
        default_factory=datetime.utcnow, description="The time when the territory was last updated"
    )

    @classmethod
    def from_dto(cls, dto: PhysicalObjectWithTerritoryDTO) -> "PhysicalObjectsWithTerritory":
        """
        Construct from DTO.
        """
        return cls(
            physical_object_id=dto.physical_object_id,
            physical_object_type=PhysicalObjectsTypes(
                physical_object_type_id=dto.physical_object_type_id, name=dto.physical_object_type_name
            ),
            name=dto.name,
            properties=dto.properties,
            territories=[
                ShortTerritory(territory_id=territory["territory_id"], name=territory["name"])
                for territory in dto.territories
            ],
            created_at=dto.created_at,
            updated_at=dto.updated_at,
        )


class PhysicalObjectWithGeometry(BaseModel):
    physical_object_id: int = Field(example=1)
    physical_object_type: PhysicalObjectsTypes = Field(example={"physical_object_type_id": 1, "name": "Здание"})
    name: Optional[str] = Field(None, description="Physical object name", example="--")
    address: Optional[str] = Field(None, description="Physical object address", example="--")
    properties: Dict[str, Any] = Field(
        default_factory=dict,
        description="Physical object additional properties",
        example={"additional_attribute_name": "additional_attribute_value"},
    )
    geometry: Geometry = Field(description="Object geometry")
    centre_point: Geometry = Field(description="Centre coordinates")
    created_at: datetime = Field(default_factory=datetime.utcnow, description="The time when the territory was created")
    updated_at: datetime = Field(
        default_factory=datetime.utcnow, description="The time when the territory was last updated"
    )

    @classmethod
    def from_dto(cls, dto: PhysicalObjectWithGeometryDTO) -> "PhysicalObjectWithGeometry":
        """
        Construct from DTO.
        """
        return cls(
            physical_object_id=dto.physical_object_id,
            physical_object_type=PhysicalObjectsTypes(
                physical_object_type_id=dto.physical_object_type_id, name=dto.physical_object_type_name
            ),
            name=dto.name,
            address=dto.address,
            properties=dto.properties,
            geometry=Geometry.from_shapely_geometry(dto.geometry),
            centre_point=Geometry.from_shapely_geometry(dto.centre_point),
            created_at=dto.created_at,
            updated_at=dto.updated_at,
        )


class PhysicalObjectWithGeometryPost(BaseModel):
    """
    Schema of physical object with geometry for POST request
    """

    territory_id: int = Field(example=1)
    geometry: Geometry = Field(description="Object geometry")
    centre_point: Optional[Geometry] = Field(None, description="Centre coordinates")
    address: Optional[str] = Field(None, description="Physical object address", example="--")
    physical_object_type_id: int = Field(example=1)
    name: Optional[str] = Field(None, description="Physical object name", example="--")
    properties: Dict[str, Any] = Field(
        default_factory=dict,
        description="Physical object additional properties",
        example={"additional_attribute_name": "additional_attribute_value"},
    )

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
    def validate_post(model: "PhysicalObjectWithGeometryPost") -> "PhysicalObjectWithGeometryPost":
        """
        Use geometry centroid for centre_point if it is missing.
        """
        if model.centre_point is None:
            model.centre_point = Geometry.from_shapely_geometry(model.geometry.as_shapely_geometry().centroid)
        return model


class PhysicalObjectsDataPost(BaseModel):
    """
    Schema of physical object for POST request
    """

    physical_object_type_id: int = Field(..., example=1)
    name: Optional[str] = Field(None, description="Physical object name", example="--")
    properties: Dict[str, Any] = Field(
        default_factory=dict,
        description="Physical object additional properties",
        example={"additional_attribute_name": "additional_attribute_value"},
    )


class PhysicalObjectsDataPut(BaseModel):
    """
    Schema of physical object for PUT request
    """

    physical_object_type_id: int = Field(..., example=1)
    name: Optional[str] = Field(..., description="Physical object name", example="--")
    properties: Dict[str, Any] = Field(
        ...,
        description="Physical object additional properties",
        example={"additional_attribute_name": "additional_attribute_value"},
    )


class PhysicalObjectsDataPatch(BaseModel):
    """
    Schema of physical object for PATCH request
    """

    physical_object_type_id: Optional[int] = Field(None, example=1)
    name: Optional[str] = Field(None, description="Physical object name", example="--")
    properties: Optional[Dict[str, Any]] = Field(
        None,
        description="Physical object additional properties",
        example={"additional_attribute_name": "additional_attribute_value"},
    )

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
