"""
Territory schemas are defined here.
"""

from datetime import datetime
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field, model_validator

from idu_api.urban_api.dto import TerritoryDTO, TerritoryTypeDTO, TerritoryWithoutGeometryDTO
from idu_api.urban_api.schemas.geometries import Geometry, GeometryValidationModel
from idu_api.urban_api.schemas.indicators import ShortIndicatorValueInfo
from idu_api.urban_api.schemas.normatives import ShortNormativeInfo


class TerritoriesOrderByField(str, Enum):
    CREATED_AT = "created_at"
    UPDATED_AT = "updated_at"


class TerritoryType(BaseModel):
    """Territory type with all its attributes."""

    territory_type_id: int | None = Field(..., description="Territory type id, if set", examples=[1])
    name: str = Field(..., description="Territory type unit name", examples=["Город"])

    @classmethod
    def from_dto(cls, dto: TerritoryTypeDTO) -> "TerritoryType":
        """Construct from DTO."""
        return cls(territory_type_id=dto.territory_type_id, name=dto.name)


class TerritoryShortInfo(BaseModel):
    """Minimal territory information - identifier and name."""

    id: int | None = Field(..., description="Territory identifier", examples=[1])
    name: str = Field(..., description="Territory name", examples=["Санкт-Петербург"])

    @classmethod
    def from_dto(cls, dto: TerritoryDTO) -> "TerritoryShortInfo":
        """Construct from DTO."""
        return cls(id=dto.territory_id, name=dto.name)


class TerritoryTypesPost(BaseModel):
    """
    Schema of territory type for POST request
    """

    name: str = Field(..., description="Territory type unit name", examples=["Город"])


class TerritoryData(BaseModel):
    """Territory with all its attributes."""

    territory_id: int = Field(..., examples=[1])
    territory_type: TerritoryType
    parent: TerritoryShortInfo | None
    name: str = Field(..., description="Territory name", examples=["--"])
    geometry: Geometry
    level: int = Field(..., examples=[1])
    properties: dict[str, Any] = Field(
        default_factory=dict,
        description="Service additional properties",
        examples=[{"additional_attribute_name": "additional_attribute_value"}],
    )
    centre_point: Geometry
    admin_center: int | None = Field(..., examples=[1])
    okato_code: str | None = Field(..., examples=["1"])
    oktmo_code: str | None = Field(..., examples=["1"])
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
            oktmo_code=dto.oktmo_code,
            created_at=dto.created_at,
            updated_at=dto.updated_at,
        )


class TerritoryDataPost(GeometryValidationModel):
    """Schema of territory for POST request."""

    territory_type_id: int = Field(..., examples=[1])
    parent_id: int | None = Field(..., examples=[1])
    name: str = Field(..., description="Territory name", examples=["--"])
    geometry: Geometry = Field(..., description="Territory geometry")
    properties: dict[str, Any] = Field(
        default_factory=dict,
        description="Service additional properties",
        examples=[{"additional_attribute_name": "additional_attribute_value"}],
    )
    centre_point: Geometry | None = Field(None, description="Centre coordinates")
    admin_center: int | None = Field(None, examples=[1])
    okato_code: str | None = Field(None, examples=["1"])
    oktmo_code: str | None = Field(None, examples=["1"])


class TerritoryDataPut(GeometryValidationModel):
    """Schema of territory for POST request."""

    territory_type_id: int = Field(..., examples=[1])
    parent_id: int | None = Field(..., examples=[1])
    name: str = Field(..., description="Territory name", examples=["--"])
    geometry: Geometry
    properties: dict[str, Any] = Field(
        ...,
        description="Service additional properties",
        examples=[{"additional_attribute_name": "additional_attribute_value"}],
    )
    centre_point: Geometry
    admin_center: int | None = Field(..., examples=[1])
    okato_code: str | None = Field(..., examples=["1"])
    oktmo_code: str | None = Field(..., examples=["1"])


class TerritoryDataPatch(GeometryValidationModel):
    """Schema of territory for POST request."""

    territory_type_id: int | None = Field(None, examples=[1])
    parent_id: int | None = Field(None, examples=[1])
    name: str | None = Field(None, description="Territory name", examples=["--"])
    geometry: Geometry | None = Field(None, description="Territory geometry")
    properties: Optional[dict[str, Any]] = Field(
        None,
        description="Service additional properties",
        examples=[{"additional_attribute_name": "additional_attribute_value"}],
    )
    centre_point: Geometry | None = Field(None, description="Centre coordinates")
    admin_center: int | None = Field(None, examples=[1])
    okato_code: str | None = Field(None, examples=["1"])
    oktmo_code: str | None = Field(None, examples=["1"])

    @model_validator(mode="before")
    @classmethod
    def check_empty_request(cls, values):
        """Ensure the request body is not empty."""
        if not values:
            raise ValueError("request body cannot be empty")
        return values


class TerritoryWithoutGeometry(BaseModel):
    """Territory with all its attributes, but without center and geometry."""

    territory_id: int = Field(..., examples=[1])
    territory_type: TerritoryType
    parent_id: int | None = Field(
        ..., description="Parent territory identifier, null only for the one territory", examples=[1]
    )
    name: str = Field(..., description="Territory name", examples=["--"])
    level: int = Field(..., examples=[1])
    properties: dict[str, Any] = Field(
        default_factory=dict,
        description="Service additional properties",
        examples=[{"additional_attribute_name": "additional_attribute_value"}],
    )
    admin_center: int | None = Field(..., examples=[1])
    okato_code: str | None = Field(..., examples=["1"])
    oktmo_code: str | None = Field(..., examples=["1"])
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
            oktmo_code=dto.oktmo_code,
            created_at=dto.created_at,
            updated_at=dto.updated_at,
        )


class ShortTerritory(BaseModel):
    """Territory with only id and name."""

    territory_id: int = Field(..., examples=[1])
    name: str = Field(..., description="Territory name", examples=["--"])


class TerritoryWithIndicator(BaseModel):
    """Short territory info with geometry and requested indicator."""

    territory_id: int = Field(..., examples=[1])
    name: str = Field(..., description="Territory name", examples=["--"])
    indicator_name: str = Field(
        ...,
        description="Indicator unit full name",
        examples=["Общее количество людей, постоянно проживающих на территории"],
    )
    indicator_value: float = Field(..., description="Indicator value for territory at time", examples=[23])
    measurement_unit_name: str = Field(..., description="Measurement unit name", examples=["Количество людей"])


class TerritoryWithIndicators(BaseModel):
    """Short territory info with geometry and all indicators."""

    territory_id: int = Field(..., examples=[1])
    name: str = Field(..., description="Territory name", examples=["--"])
    indicators: list[ShortIndicatorValueInfo]


class TerritoryWithNormatives(BaseModel):
    """Short territory info with geometry and list of all normatives."""

    territory_id: int = Field(..., examples=[1])
    name: str = Field(..., description="Territory name", examples=["--"])
    normatives: list[ShortNormativeInfo]
