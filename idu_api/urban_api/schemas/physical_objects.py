"""Physical object schemas are defined here."""

from datetime import datetime
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field, model_validator

from idu_api.urban_api.dto import (
    PhysicalObjectDataDTO,
    PhysicalObjectWithGeometryDTO,
    PhysicalObjectWithTerritoryDTO,
    ScenarioPhysicalObjectDTO,
)
from idu_api.urban_api.schemas.geometries import Geometry, GeometryValidationModel
from idu_api.urban_api.schemas.physical_object_types import PhysicalObjectFunctionBasic, PhysicalObjectsTypes
from idu_api.urban_api.schemas.territories import ShortTerritory


class PhysicalObjectsOrderByField(str, Enum):
    CREATED_AT = "created_at"
    UPDATED_AT = "updated_at"


class PhysicalObjectsData(BaseModel):
    """Physical object with all its attributes."""

    physical_object_id: int = Field(..., examples=[1])
    physical_object_type: PhysicalObjectsTypes
    name: str | None = Field(None, description="physical object name", examples=["--"])
    properties: dict[str, Any] = Field(
        default_factory=dict,
        description="physical object additional properties",
        examples=[{"additional_attribute_name": "additional_attribute_value"}],
    )
    created_at: datetime = Field(
        default_factory=datetime.utcnow, description="the time when the physical object was created"
    )
    updated_at: datetime = Field(
        default_factory=datetime.utcnow, description="the time when the physical object was last updated"
    )

    @classmethod
    def from_dto(cls, dto: PhysicalObjectDataDTO) -> "PhysicalObjectsData":
        """
        Construct from DTO.
        """
        return cls(
            physical_object_id=dto.physical_object_id,
            physical_object_type=PhysicalObjectsTypes(
                physical_object_type_id=dto.physical_object_type_id,
                name=dto.physical_object_type_name,
                physical_object_function=(
                    PhysicalObjectFunctionBasic(
                        id=dto.physical_object_function_id, name=dto.physical_object_function_name
                    )
                    if dto.physical_object_function_id is not None
                    else None
                ),
            ),
            name=dto.name,
            properties=dto.properties,
            created_at=dto.created_at,
            updated_at=dto.updated_at,
        )


class PhysicalObjectsWithTerritory(BaseModel):
    """Physical object with all its attributes and parent territory."""

    physical_object_id: int = Field(..., examples=[1])
    physical_object_type: PhysicalObjectsTypes
    name: str | None = Field(None, description="physical object name", examples=["--"])
    properties: dict[str, Any] = Field(
        default_factory=dict,
        description="physical object additional properties",
        examples=[{"additional_attribute_name": "additional_attribute_value"}],
    )
    territories: list[ShortTerritory]
    created_at: datetime = Field(
        default_factory=datetime.utcnow, description="the time when the physical object was created"
    )
    updated_at: datetime = Field(
        default_factory=datetime.utcnow, description="the time when the physical object was last updated"
    )

    @classmethod
    def from_dto(cls, dto: PhysicalObjectWithTerritoryDTO) -> "PhysicalObjectsWithTerritory":
        """
        Construct from DTO.
        """
        return cls(
            physical_object_id=dto.physical_object_id,
            physical_object_type=PhysicalObjectsTypes(
                physical_object_type_id=dto.physical_object_type_id,
                name=dto.physical_object_type_name,
                physical_object_function=(
                    PhysicalObjectFunctionBasic(
                        id=dto.physical_object_function_id, name=dto.physical_object_function_name
                    )
                    if dto.physical_object_function_id is not None
                    else None
                ),
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
    """Physical object with all its attributes and geometry."""

    physical_object_id: int = Field(..., examples=[1])
    physical_object_type: PhysicalObjectsTypes
    name: str | None = Field(None, description="physical object name", examples=["--"])
    address: str | None = Field(None, description="physical object address", examples=["--"])
    osm_id: str | None = Field(None, description="open street map identifier", examples=["1"])
    properties: dict[str, Any] = Field(
        default_factory=dict,
        description="physical object additional properties",
        examples=[{"additional_attribute_name": "additional_attribute_value"}],
    )
    geometry: Geometry
    centre_point: Geometry
    created_at: datetime = Field(
        default_factory=datetime.utcnow, description="the time when the physical object was created"
    )
    updated_at: datetime = Field(
        default_factory=datetime.utcnow, description="the time when the physical object was last updated"
    )

    @classmethod
    def from_dto(cls, dto: PhysicalObjectWithGeometryDTO) -> "PhysicalObjectWithGeometry":
        """
        Construct from DTO.
        """
        return cls(
            physical_object_id=dto.physical_object_id,
            physical_object_type=PhysicalObjectsTypes(
                physical_object_type_id=dto.physical_object_type_id,
                name=dto.physical_object_type_name,
                physical_object_function=(
                    PhysicalObjectFunctionBasic(
                        id=dto.physical_object_function_id, name=dto.physical_object_function_name
                    )
                    if dto.physical_object_function_id is not None
                    else None
                ),
            ),
            name=dto.name,
            address=dto.address,
            properties=dto.properties,
            geometry=Geometry.from_shapely_geometry(dto.geometry),
            centre_point=Geometry.from_shapely_geometry(dto.centre_point),
            created_at=dto.created_at,
            updated_at=dto.updated_at,
        )


class PhysicalObjectWithGeometryPost(GeometryValidationModel):
    """Physical object schema with geometry for POST request."""

    territory_id: int = Field(..., examples=[1])
    geometry: Geometry
    centre_point: Geometry | None = None
    address: str | None = Field(None, description="physical object address", examples=["--"])
    osm_id: str | None = Field(None, description="open street map identifier", examples=["1"])
    physical_object_type_id: int = Field(..., examples=[1])
    name: str | None = Field(None, description="physical object name", examples=["--"])
    properties: dict[str, Any] = Field(
        default_factory=dict,
        description="physical object additional properties",
        examples=[{"additional_attribute_name": "additional_attribute_value"}],
    )


class PhysicalObjectsDataPost(BaseModel):
    """Physical object schema for POST request."""

    physical_object_type_id: int = Field(..., examples=[1])
    name: str | None = Field(None, description="physical object name", examples=["--"])
    properties: dict[str, Any] = Field(
        default_factory=dict,
        description="physical object additional properties",
        examples=[{"additional_attribute_name": "additional_attribute_value"}],
    )


class PhysicalObjectsDataPut(BaseModel):
    """Physical object schema for PUT request."""

    physical_object_type_id: int = Field(..., examples=[1])
    name: str | None = Field(..., description="physical object name", examples=["--"])
    properties: dict[str, Any] = Field(
        ...,
        description="physical object additional properties",
        examples=[{"additional_attribute_name": "additional_attribute_value"}],
    )


class PhysicalObjectsDataPatch(BaseModel):
    """Physical object schema for PATCH request."""

    physical_object_type_id: int | None = Field(None, examples=[1])
    name: str | None = Field(None, description="physical object name", examples=["--"])
    properties: Optional[dict[str, Any]] = Field(
        None,
        description="physical object additional properties",
        examples=[{"additional_attribute_name": "additional_attribute_value"}],
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


class ScenarioPhysicalObject(PhysicalObjectsData):
    """Scenario physical object with all its attributes."""

    is_scenario_object: bool = Field(..., description="boolean parameter to determine scenario object")

    @classmethod
    def from_dto(cls, dto: ScenarioPhysicalObjectDTO) -> "ScenarioPhysicalObject":
        """
        Construct from DTO.
        """
        return cls(
            physical_object_id=dto.physical_object_id,
            physical_object_type=PhysicalObjectsTypes(
                physical_object_type_id=dto.physical_object_type_id,
                name=dto.physical_object_type_name,
                physical_object_function=(
                    PhysicalObjectFunctionBasic(
                        id=dto.physical_object_function_id, name=dto.physical_object_function_name
                    )
                    if dto.physical_object_function_id is not None
                    else None
                ),
            ),
            name=dto.name,
            properties=dto.properties,
            created_at=dto.created_at,
            updated_at=dto.updated_at,
            is_scenario_object=dto.is_scenario_object,
        )
