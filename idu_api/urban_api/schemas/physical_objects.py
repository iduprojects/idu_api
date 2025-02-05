"""Physical object schemas are defined here."""

from datetime import datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field, model_validator

from idu_api.urban_api.dto import (
    PhysicalObjectDTO,
    PhysicalObjectWithGeometryDTO,
    ScenarioPhysicalObjectDTO,
)
from idu_api.urban_api.schemas.geometries import Geometry, GeometryValidationModel
from idu_api.urban_api.schemas.physical_object_types import PhysicalObjectFunctionBasic, PhysicalObjectType
from idu_api.urban_api.schemas.short_models import ShortLivingBuilding
from idu_api.urban_api.schemas.territories import ShortTerritory


class PhysicalObjectsOrderByField(str, Enum):
    CREATED_AT = "created_at"
    UPDATED_AT = "updated_at"


class PhysicalObject(BaseModel):
    """Physical object with all its attributes."""

    physical_object_id: int = Field(..., examples=[1])
    physical_object_type: PhysicalObjectType
    name: str | None = Field(None, description="physical object name", examples=["--"])
    properties: dict[str, Any] = Field(
        default_factory=dict,
        description="physical object additional properties",
        examples=[{"additional_attribute_name": "additional_attribute_value"}],
    )
    living_building: ShortLivingBuilding | None
    territories: list[ShortTerritory] | None = None
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc), description="the time when the physical object was created"
    )
    updated_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="the time when the physical object was last updated",
    )

    @classmethod
    def from_dto(cls, dto: PhysicalObjectDTO) -> "PhysicalObject":
        """
        Construct from DTO.
        """
        return cls(
            physical_object_id=dto.physical_object_id,
            physical_object_type=PhysicalObjectType(
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
            living_building=(
                ShortLivingBuilding(
                    id=dto.living_building_id,
                    living_area=dto.living_area,
                    properties=dto.living_building_properties,
                )
                if dto.living_building_id is not None
                else None
            ),
            territories=(
                [ShortTerritory(id=territory["territory_id"], name=territory["name"]) for territory in dto.territories]
                if dto.territories is not None
                else None
            ),
            created_at=dto.created_at,
            updated_at=dto.updated_at,
        )


class PhysicalObjectWithGeometry(BaseModel):
    """Physical object with all its attributes and geometry."""

    physical_object_id: int = Field(..., examples=[1])
    physical_object_type: PhysicalObjectType
    territory: ShortTerritory
    name: str | None = Field(None, description="physical object name", examples=["--"])
    properties: dict[str, Any] = Field(
        default_factory=dict,
        description="physical object additional properties",
        examples=[{"additional_attribute_name": "additional_attribute_value"}],
    )
    living_building: ShortLivingBuilding | None
    object_geometry_id: int = Field(..., description="object geometry identifier", examples=[1])
    address: str | None = Field(None, description="physical object address", examples=["--"])
    osm_id: str | None = Field(None, description="open street map identifier", examples=["1"])
    geometry: Geometry
    centre_point: Geometry
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc), description="the time when the physical object was created"
    )
    updated_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="the time when the physical object was last updated",
    )

    @classmethod
    def from_dto(cls, dto: PhysicalObjectWithGeometryDTO) -> "PhysicalObjectWithGeometry":
        """
        Construct from DTO.
        """
        return cls(
            physical_object_id=dto.physical_object_id,
            physical_object_type=PhysicalObjectType(
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
            territory=ShortTerritory(id=dto.territory_id, name=dto.territory_name),
            name=dto.name,
            properties=dto.properties,
            living_building=(
                ShortLivingBuilding(
                    id=dto.living_building_id,
                    living_area=dto.living_area,
                    properties=dto.living_building_properties,
                )
                if dto.living_building_id is not None
                else None
            ),
            object_geometry_id=dto.object_geometry_id,
            address=dto.address,
            osm_id=dto.osm_id,
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


class PhysicalObjectPost(BaseModel):
    """Physical object schema for POST request."""

    physical_object_type_id: int = Field(..., examples=[1])
    name: str | None = Field(None, description="physical object name", examples=["--"])
    properties: dict[str, Any] = Field(
        default_factory=dict,
        description="physical object additional properties",
        examples=[{"additional_attribute_name": "additional_attribute_value"}],
    )


class PhysicalObjectPut(BaseModel):
    """Physical object schema for PUT request."""

    physical_object_type_id: int = Field(..., examples=[1])
    name: str | None = Field(..., description="physical object name", examples=["--"])
    properties: dict[str, Any] = Field(
        ...,
        description="physical object additional properties",
        examples=[{"additional_attribute_name": "additional_attribute_value"}],
    )


class PhysicalObjectPatch(BaseModel):
    """Physical object schema for PATCH request."""

    physical_object_type_id: int | None = Field(None, examples=[1])
    name: str | None = Field(None, description="physical object name", examples=["--"])
    properties: dict[str, Any] | None = Field(
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


class ScenarioPhysicalObject(PhysicalObject):
    """Scenario physical object with all its attributes."""

    is_scenario_object: bool = Field(..., description="boolean parameter to determine scenario object")

    @classmethod
    def from_dto(cls, dto: ScenarioPhysicalObjectDTO) -> "ScenarioPhysicalObject":
        """
        Construct from DTO.
        """
        return cls(
            physical_object_id=dto.physical_object_id,
            physical_object_type=PhysicalObjectType(
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
            living_building=(
                ShortLivingBuilding(
                    id=dto.living_building_id,
                    living_area=dto.living_area,
                    properties=dto.living_building_properties,
                )
                if dto.living_building_id is not None
                else None
            ),
            territories=(
                [ShortTerritory(id=territory["territory_id"], name=territory["name"]) for territory in dto.territories]
                if dto.territories is not None
                else None
            ),
            name=dto.name,
            properties=dto.properties,
            created_at=dto.created_at,
            updated_at=dto.updated_at,
            is_scenario_object=dto.is_scenario_object,
        )
