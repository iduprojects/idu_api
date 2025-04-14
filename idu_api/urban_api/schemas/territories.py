"""Territory schemas are defined here."""

from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, Field, model_validator

from idu_api.urban_api.dto import (
    TargetCityTypeDTO,
    TerritoryDTO,
    TerritoryTreeWithoutGeometryDTO,
    TerritoryTypeDTO,
    TerritoryWithoutGeometryDTO,
)
from idu_api.urban_api.schemas.geometries import Geometry, GeometryValidationModel
from idu_api.urban_api.schemas.short_models import (
    ShortIndicatorValueInfo,
    ShortNormativeInfo,
    ShortTerritory,
    TargetCityTypeBasic,
    TerritoryTypeBasic,
)


class TerritoryType(BaseModel):
    """Territory type with all its attributes."""

    territory_type_id: int = Field(..., description="territory type identifier", examples=[1])
    name: str = Field(..., description="territory type unit name", examples=["Город"])

    @classmethod
    def from_dto(cls, dto: TerritoryTypeDTO) -> "TerritoryType":
        """Construct from DTO."""
        return cls(territory_type_id=dto.territory_type_id, name=dto.name)


class TerritoryTypePost(BaseModel):
    """Schema of territory type for POST request."""

    name: str = Field(..., description="territory type unit name", examples=["Город"])


class TargetCityType(BaseModel):
    """Target city type with all its attributes."""

    target_city_type_id: int = Field(..., description="target city type identifier", examples=[1])
    name: str = Field(..., description="target city type unit name", examples=["Административный центр"])
    description: str = Field(
        ...,
        description="target city type unit description",
        examples=["Статус адинистративного центра субъекта Российской Федерации"],
    )

    @classmethod
    def from_dto(cls, dto: TargetCityTypeDTO) -> "TargetCityType":
        """Construct from DTO."""
        return cls(target_city_type_id=dto.target_city_type_id, name=dto.name, description=dto.description)


class TargetCityTypePost(BaseModel):
    """Schema of target city type for POST request."""

    name: str = Field(..., description="target city type unit name", examples=["Административный центр"])
    description: str = Field(
        ...,
        description="target city type unit description",
        examples=["Статус адинистративного центра субъекта Российской Федерации"],
    )


class Territory(BaseModel):
    """Territory with all its attributes."""

    territory_id: int = Field(..., examples=[1])
    territory_type: TerritoryTypeBasic
    parent: ShortTerritory | None
    name: str = Field(..., description="territory name", examples=["--"])
    geometry: Geometry
    level: int = Field(..., examples=[1])
    properties: dict[str, Any] = Field(
        default_factory=dict,
        description="territory additional properties",
        examples=[{"additional_attribute_name": "additional_attribute_value"}],
    )
    centre_point: Geometry
    admin_center: ShortTerritory | None
    target_city_type: TargetCityTypeBasic | None
    okato_code: str | None = Field(..., examples=["1"])
    oktmo_code: str | None = Field(..., examples=["1"])
    is_city: bool = Field(..., description="boolean parameter to determine cities")
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc), description="the time when the territory was created"
    )
    updated_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc), description="the time when the territory was last updated"
    )

    @classmethod
    def from_dto(cls, dto: TerritoryDTO) -> "Territory":
        """Construct from DTO."""

        return cls(
            territory_id=dto.territory_id,
            territory_type=TerritoryTypeBasic(id=dto.territory_type_id, name=dto.territory_type_name),
            parent=(ShortTerritory(id=dto.parent_id, name=dto.parent_name) if dto.parent_id is not None else None),
            name=dto.name,
            geometry=Geometry.from_shapely_geometry(dto.geometry),
            level=dto.level,
            properties=dto.properties,
            centre_point=Geometry.from_shapely_geometry(dto.centre_point),
            admin_center=(
                ShortTerritory(
                    id=dto.admin_center_id,
                    name=dto.admin_center_name,
                )
                if dto.admin_center_id is not None
                else None
            ),
            target_city_type=(
                TargetCityTypeBasic(
                    id=dto.target_city_type_id,
                    name=dto.target_city_type_name,
                    description=dto.target_city_type_description,
                )
                if dto.target_city_type_id is not None
                else None
            ),
            okato_code=dto.okato_code,
            oktmo_code=dto.oktmo_code,
            is_city=dto.is_city,
            created_at=dto.created_at,
            updated_at=dto.updated_at,
        )


class TerritoryPost(GeometryValidationModel):
    """Territory schema for POST request."""

    territory_type_id: int = Field(..., examples=[1])
    parent_id: int | None = Field(..., examples=[1])
    name: str = Field(..., description="territory name", examples=["--"])
    geometry: Geometry
    properties: dict[str, Any] = Field(
        default_factory=dict,
        description="territory additional properties",
        examples=[{"additional_attribute_name": "additional_attribute_value"}],
    )
    centre_point: Geometry | None = None
    admin_center_id: int | None = Field(None, examples=[1])
    target_city_type_id: int | None = Field(None, examples=[1])
    okato_code: str | None = Field(None, examples=["1"])
    oktmo_code: str | None = Field(None, examples=["1"])
    is_city: bool = Field(..., description="boolean parameter to determine cities")


class TerritoryPut(GeometryValidationModel):
    """Territory schema for PUT request."""

    territory_type_id: int = Field(..., examples=[1])
    parent_id: int | None = Field(..., examples=[1])
    name: str = Field(..., description="territory name", examples=["--"])
    geometry: Geometry
    properties: dict[str, Any] = Field(
        ...,
        description="territory additional properties",
        examples=[{"additional_attribute_name": "additional_attribute_value"}],
    )
    centre_point: Geometry
    admin_center_id: int | None = Field(..., examples=[1])
    target_city_type_id: int | None = Field(..., examples=[1])
    okato_code: str | None = Field(..., examples=["1"])
    oktmo_code: str | None = Field(..., examples=["1"])
    is_city: bool = Field(..., description="boolean parameter to determine cities")


class TerritoryPatch(GeometryValidationModel):
    """Territory schema for PATCH request."""

    territory_type_id: int | None = Field(None, examples=[1])
    parent_id: int | None = Field(None, examples=[1])
    name: str | None = Field(None, description="territory name", examples=["--"])
    geometry: Geometry | None = None
    properties: dict[str, Any] | None = Field(
        None,
        description="territory additional properties",
        examples=[{"additional_attribute_name": "additional_attribute_value"}],
    )
    centre_point: Geometry | None = None
    admin_center_id: int | None = Field(None, examples=[1])
    target_city_type_id: int | None = Field(None, examples=[1])
    okato_code: str | None = Field(None, examples=["1"])
    oktmo_code: str | None = Field(None, examples=["1"])
    is_city: bool = Field(None, description="boolean parameter to determine cities")

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
    territory_type: TerritoryTypeBasic
    parent: ShortTerritory | None
    name: str = Field(..., description="territory name", examples=["--"])
    level: int = Field(..., examples=[1])
    properties: dict[str, Any] = Field(
        default_factory=dict,
        description="territory additional properties",
        examples=[{"additional_attribute_name": "additional_attribute_value"}],
    )
    admin_center: ShortTerritory | None
    target_city_type: TargetCityTypeBasic | None
    okato_code: str | None = Field(..., examples=["1"])
    oktmo_code: str | None = Field(..., examples=["1"])
    is_city: bool = Field(..., description="boolean parameter to determine cities")
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc), description="the time when the territory was created"
    )
    updated_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc), description="the time when the territory was last updated"
    )

    @classmethod
    def from_dto(cls, dto: TerritoryWithoutGeometryDTO) -> "TerritoryWithoutGeometry":
        """Construct from DTO."""
        return cls(
            territory_id=dto.territory_id,
            territory_type=TerritoryTypeBasic(id=dto.territory_type_id, name=dto.territory_type_name),
            parent=(ShortTerritory(id=dto.parent_id, name=dto.parent_name) if dto.parent_id is not None else None),
            name=dto.name,
            level=dto.level,
            properties=dto.properties,
            admin_center=(
                ShortTerritory(
                    id=dto.admin_center_id,
                    name=dto.admin_center_name,
                )
                if dto.admin_center_id is not None
                else None
            ),
            target_city_type=(
                TargetCityTypeBasic(
                    id=dto.target_city_type_id,
                    name=dto.target_city_type_name,
                    description=dto.target_city_type_description,
                )
                if dto.target_city_type_id is not None
                else None
            ),
            okato_code=dto.okato_code,
            oktmo_code=dto.oktmo_code,
            is_city=dto.is_city,
            created_at=dto.created_at,
            updated_at=dto.updated_at,
        )


class TerritoryTreeWithoutGeometry(TerritoryWithoutGeometry):
    """
    Territory with all its attributes, but without center and geometry

    which also contains child territories."""

    children: list["TerritoryTreeWithoutGeometry"] = Field(default_factory=list)

    @classmethod
    def from_dto(cls, dto: TerritoryTreeWithoutGeometryDTO) -> "TerritoryTreeWithoutGeometry":
        """Construct from DTO with nested children."""
        return cls(
            territory_id=dto.territory_id,
            territory_type=TerritoryTypeBasic(id=dto.territory_type_id, name=dto.territory_type_name),
            parent=(ShortTerritory(id=dto.parent_id, name=dto.parent_name) if dto.parent_id is not None else None),
            name=dto.name,
            level=dto.level,
            properties=dto.properties,
            admin_center=(
                ShortTerritory(id=dto.admin_center_id, name=dto.admin_center_name)
                if dto.admin_center_id is not None
                else None
            ),
            target_city_type=(
                TargetCityTypeBasic(
                    id=dto.target_city_type_id,
                    name=dto.target_city_type_name,
                    description=dto.target_city_type_description,
                )
                if dto.target_city_type_id is not None
                else None
            ),
            okato_code=dto.okato_code,
            oktmo_code=dto.oktmo_code,
            is_city=dto.is_city,
            created_at=dto.created_at,
            updated_at=dto.updated_at,
            children=[TerritoryTreeWithoutGeometry.from_dto(child_dto) for child_dto in dto.children],
        )


class TerritoryWithIndicator(BaseModel):
    """Short territory info with geometry and requested indicator."""

    territory_id: int = Field(..., examples=[1])
    name: str = Field(..., description="territory name", examples=["--"])
    is_city: bool = Field(..., description="boolean parameter to determine cities")
    indicator_name: str = Field(
        ...,
        description="indicator unit full name",
        examples=["Общее количество людей, постоянно проживающих на территории"],
    )
    indicator_value: float = Field(..., description="indicator value for territory at time", examples=[23])
    measurement_unit_name: str = Field(..., description="measurement unit name", examples=["Количество людей"])


class TerritoryWithIndicators(BaseModel):
    """Short territory info with geometry and all indicators."""

    territory_id: int = Field(..., examples=[1])
    name: str = Field(..., description="territory name", examples=["--"])
    indicators: list[ShortIndicatorValueInfo]


class TerritoryWithNormatives(BaseModel):
    """Short territory info with geometry and list of all normatives."""

    territory_id: int = Field(..., examples=[1])
    name: str = Field(..., description="territory name", examples=["--"])
    normatives: list[ShortNormativeInfo]
