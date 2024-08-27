from datetime import datetime
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field, model_validator

from idu_api.urban_api.dto import (
    PhysicalObjectDataDTO,
    PhysicalObjectTypeDTO,
    PhysicalObjectWithGeometryDTO,
    PhysicalObjectWithTerritoryDTO,
)
from idu_api.urban_api.schemas.geometries import Geometry, GeometryValidationModel
from idu_api.urban_api.schemas.territories import ShortTerritory


class PhysicalObjectsOrderByField(str, Enum):
    CREATED_AT = "created_at"
    UPDATED_AT = "updated_at"


class PhysicalObjectsTypes(BaseModel):
    """
    Physical object type with all its attributes
    """

    physical_object_type_id: int = Field(..., description="Physical object type id, if set", examples=[1])
    name: str = Field(..., description="Physical object type unit name", examples=["Здание"])

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

    name: str = Field(..., description="Physical object type unit name", examples=["Здание"])


class PhysicalObjectsData(BaseModel):
    """
    Physical object with all its attributes
    """

    physical_object_id: int = Field(..., examples=[1])
    physical_object_type: PhysicalObjectsTypes
    name: str | None = Field(None, description="Physical object name", examples=["--"])
    properties: dict[str, Any] = Field(
        default_factory=dict,
        description="Physical object additional properties",
        examples=[{"additional_attribute_name": "additional_attribute_value"}],
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

    physical_object_id: int = Field(..., examples=[1])
    physical_object_type: PhysicalObjectsTypes
    name: str | None = Field(None, description="Physical object name", examples=["--"])
    properties: dict[str, Any] = Field(
        default_factory=dict,
        description="Physical object additional properties",
        examples=[{"additional_attribute_name": "additional_attribute_value"}],
    )
    territories: list[ShortTerritory]
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
    physical_object_id: int = Field(..., examples=[1])
    physical_object_type: PhysicalObjectsTypes
    name: str | None = Field(None, description="Physical object name", examples=["--"])
    address: str | None = Field(None, description="Physical object address", examples=["--"])
    properties: dict[str, Any] = Field(
        default_factory=dict,
        description="Physical object additional properties",
        examples=[{"additional_attribute_name": "additional_attribute_value"}],
    )
    geometry: Geometry
    centre_point: Geometry
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


class PhysicalObjectWithGeometryPost(GeometryValidationModel):
    """
    Schema of physical object with geometry for POST request
    """

    territory_id: int = Field(..., examples=[1])
    geometry: Geometry
    centre_point: Geometry | None = Field(None, description="Centre coordinates")
    address: str | None = Field(None, description="Physical object address", examples=["--"])
    physical_object_type_id: int = Field(..., examples=[1])
    name: str | None = Field(None, description="Physical object name", examples=["--"])
    properties: dict[str, Any] = Field(
        default_factory=dict,
        description="Physical object additional properties",
        examples=[{"additional_attribute_name": "additional_attribute_value"}],
    )


class PhysicalObjectsDataPost(BaseModel):
    """
    Schema of physical object for POST request
    """

    physical_object_type_id: int = Field(..., examples=[1])
    name: str | None = Field(None, description="Physical object name", examples=["--"])
    properties: dict[str, Any] = Field(
        default_factory=dict,
        description="Physical object additional properties",
        examples=[{"additional_attribute_name": "additional_attribute_value"}],
    )


class PhysicalObjectsDataPut(BaseModel):
    """
    Schema of physical object for PUT request
    """

    physical_object_type_id: int = Field(..., examples=[1])
    name: str | None = Field(..., description="Physical object name", examples=["--"])
    properties: dict[str, Any] = Field(
        ...,
        description="Physical object additional properties",
        examples=[{"additional_attribute_name": "additional_attribute_value"}],
    )


class PhysicalObjectsDataPatch(BaseModel):
    """
    Schema of physical object for PATCH request
    """

    physical_object_type_id: int | None = Field(None, examples=[1])
    name: str | None = Field(None, description="Physical object name", examples=["--"])
    properties: Optional[dict[str, Any]] = Field(
        None,
        description="Physical object additional properties",
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
