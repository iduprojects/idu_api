"""Functional zones schemas are defined here."""

from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, Field, model_validator

from idu_api.urban_api.dto import (
    FunctionalZoneDTO,
    FunctionalZoneSourceDTO,
    FunctionalZoneTypeDTO,
    ScenarioFunctionalZoneDTO,
)
from idu_api.urban_api.schemas.geometries import Geometry, NotPointGeometryValidationModel
from idu_api.urban_api.schemas.short_models import FunctionalZoneTypeBasic, ShortScenario, ShortTerritory


class FunctionalZoneSource(BaseModel):
    """Functional zone schema with only year and source."""

    year: int = Field(..., description="year when functional zone was loaded", examples=[2024])
    source: str = Field(..., description="source from which functional zone was loaded", examples=["--"])

    @classmethod
    def from_dto(cls, dto: FunctionalZoneSourceDTO) -> "FunctionalZoneSource":
        return cls(year=dto.year, source=dto.source)


class FunctionalZoneType(BaseModel):
    """Functional zone type with all its attributes."""

    functional_zone_type_id: int = Field(..., description="functional zone type identifier", examples=[1])
    name: str = Field(..., description="functional zone type name", examples=["residential"])
    zone_nickname: str | None = Field(..., description="functional zone type nickname", examples=["ИЖС"])
    description: str | None = Field(None, description="description of functional zone type", examples=["--"])

    @classmethod
    def from_dto(cls, dto: FunctionalZoneTypeDTO) -> "FunctionalZoneType":
        return cls(
            functional_zone_type_id=dto.functional_zone_type_id,
            name=dto.name,
            zone_nickname=dto.zone_nickname,
            description=dto.description,
        )


class FunctionalZoneTypePost(BaseModel):
    """Functional zone schema for POST requests."""

    name: str = Field(..., description="target profile name", examples=["residential"])
    zone_nickname: str | None = Field(..., description="target profile name", examples=["ИЖС"])
    description: str | None = Field(None, description="description of functional zone type", examples=["--"])


class FunctionalZone(BaseModel):
    """Functional zone with all its attributes."""

    functional_zone_id: int = Field(..., description="functional zone identifier", examples=[1])
    territory: ShortTerritory
    functional_zone_type: FunctionalZoneTypeBasic
    name: str | None = Field(..., description="functional zone name", examples=["--"])
    geometry: Geometry
    year: int = Field(..., description="year when functional zone was loaded", examples=[2024])
    source: str = Field(..., description="source from which functional zone was loaded", examples=["--"])
    properties: dict[str, Any] = Field(
        default_factory=dict,
        description="functional zone additional properties",
        examples=[{"additional_attribute_name": "additional_attribute_value"}],
    )
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc), description="the time when the functional zone was created"
    )
    updated_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="the time when the functional zone was last updated",
    )

    @classmethod
    def from_dto(cls, dto: FunctionalZoneDTO) -> "FunctionalZone":
        return cls(
            functional_zone_id=dto.functional_zone_id,
            territory=ShortTerritory(id=dto.territory_id, name=dto.territory_name),
            functional_zone_type=FunctionalZoneTypeBasic(
                id=dto.functional_zone_type_id,
                name=dto.functional_zone_type_name,
                nickname=dto.functional_zone_type_nickname,
                description=dto.functional_zone_type_description,
            ),
            name=dto.name,
            year=dto.year,
            source=dto.source,
            geometry=Geometry.from_shapely_geometry(dto.geometry),
            properties=dto.properties,
            created_at=dto.created_at,
            updated_at=dto.updated_at,
        )


class FunctionalZoneWithoutGeometry(BaseModel):
    """Functional zone with all its attributes except geometry."""

    functional_zone_id: int = Field(..., description="functional zone identifier", examples=[1])
    territory: ShortTerritory
    functional_zone_type: FunctionalZoneTypeBasic
    name: str | None = Field(..., description="functional zone name", examples=["--"])
    year: int = Field(..., description="year when functional zone was loaded", examples=[2024])
    source: str = Field(..., description="source from which functional zone was loaded", examples=["--"])
    properties: dict[str, Any] = Field(
        default_factory=dict,
        description="functional zone additional properties",
        examples=[{"additional_attribute_name": "additional_attribute_value"}],
    )
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc), description="the time when the functional zone was created"
    )
    updated_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="the time when the functional zone was last updated",
    )


class FunctionalZonePost(NotPointGeometryValidationModel):
    """Functional zone schema for POST requests."""

    territory_id: int = Field(..., description="territory identifier where functional zone is", examples=[1])
    functional_zone_type_id: int = Field(..., description="functional zone type identifier", examples=[1])
    name: str | None = Field(None, description="functional zone name", examples=["--"])
    geometry: Geometry
    year: int = Field(..., description="year when functional zone was loaded", examples=[2024])
    source: str = Field(..., description="source from which functional zone was loaded", examples=["--"])
    properties: dict[str, Any] = Field(
        default_factory=dict,
        description="functional zone additional properties",
        examples=[{"additional_attribute_name": "additional_attribute_value"}],
    )


class FunctionalZonePut(NotPointGeometryValidationModel):
    """Functional zone schema for PUT requests."""

    territory_id: int = Field(..., description="territory identifier where functional zone is", examples=[1])
    functional_zone_type_id: int = Field(..., description="functional zone type identifier", examples=[1])
    name: str = Field(..., description="functional zone name", examples=["--"])
    geometry: Geometry
    year: int = Field(..., description="year when functional zone was loaded", examples=[2024])
    source: str = Field(..., description="source from which functional zone was loaded", examples=["--"])
    properties: dict[str, Any] = Field(
        ...,
        description="functional zone additional properties",
        examples=[{"additional_attribute_name": "additional_attribute_value"}],
    )


class FunctionalZonePatch(NotPointGeometryValidationModel):
    """Functional zone schema for PATCH requests."""

    territory_id: int | None = Field(None, description="territory identifier where functional zone is", examples=[1])
    functional_zone_type_id: int | None = Field(None, description="functional zone type identifier", examples=[1])
    name: str | None = Field(None, description="functional zone name", examples=["--"])
    geometry: Geometry | None = None
    year: int | None = Field(None, description="year when functional zone was loaded", examples=[2024])
    source: str | None = Field(None, description="source from which functional zone was loaded", examples=["--"])
    properties: dict[str, Any] | None = Field(
        None,
        description="functional zone additional properties",
        examples=[{"additional_attribute_name": "additional_attribute_value"}],
    )

    @model_validator(mode="before")
    @classmethod
    def check_empty_request(cls, values):
        """Ensure the request body is not empty."""
        if not values:
            raise ValueError("request body cannot be empty")
        return values


class ScenarioFunctionalZone(BaseModel):
    """Project scenario functional zone with all its attributes."""

    functional_zone_id: int = Field(..., description="scenario functional zone identifier", examples=[1])
    scenario: ShortScenario
    functional_zone_type: FunctionalZoneTypeBasic
    name: str | None = Field(..., description="functional zone name", examples=["--"])
    year: int = Field(..., description="year when functional zone was loaded", examples=[2024])
    source: str = Field(..., description="source from which functional zone was loaded", examples=["--"])
    geometry: Geometry
    properties: dict[str, Any] = Field(
        default_factory=dict,
        description="scenario functional zone additional properties",
        examples=[{"additional_attribute_name": "additional_attribute_value"}],
    )
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="the time when the scenario functional zone was created",
    )
    updated_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="the time when the scenario functional zone was last updated",
    )

    @classmethod
    def from_dto(cls, dto: ScenarioFunctionalZoneDTO) -> "ScenarioFunctionalZone":
        return cls(
            functional_zone_id=dto.functional_zone_id,
            scenario=ShortScenario(id=dto.scenario_id, name=dto.scenario_name),
            functional_zone_type=FunctionalZoneTypeBasic(
                id=dto.functional_zone_type_id,
                name=dto.functional_zone_type_name,
                nickname=dto.functional_zone_type_nickname,
                description=dto.functional_zone_type_description,
            ),
            name=dto.name,
            geometry=Geometry.from_shapely_geometry(dto.geometry),
            year=dto.year,
            source=dto.source,
            properties=dto.properties,
            created_at=dto.created_at,
            updated_at=dto.updated_at,
        )


class ScenarioFunctionalZoneWithoutGeometry(BaseModel):
    """Project scenario functional zone with all its attributes except geometry and scenario info."""

    functional_zone_id: int = Field(..., description="scenario functional zone identifier", examples=[1])
    functional_zone_type: FunctionalZoneTypeBasic
    name: str | None = Field(..., description="functional zone name", examples=["--"])
    year: int = Field(..., description="year when functional zone was loaded", examples=[2024])
    source: str = Field(..., description="source from which functional zone was loaded", examples=["--"])
    properties: dict[str, Any] = Field(
        default_factory=dict,
        description="scenario functional zone additional properties",
        examples=[{"additional_attribute_name": "additional_attribute_value"}],
    )
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="the time when the scenario functional zone was created",
    )
    updated_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="the time when the scenario functional zone was last updated",
    )


class ScenarioFunctionalZonePost(NotPointGeometryValidationModel):
    """Project scenario functional zone schema for POST requests."""

    functional_zone_type_id: int = Field(
        ..., description="functional zone type identifier for the scenario functional zone", examples=[1]
    )
    name: str | None = Field(None, description="functional zone name", examples=["--"])
    year: int = Field(..., description="year when functional zone was loaded", examples=[2024])
    source: str = Field(..., description="source from which functional zone was loaded", examples=["--"])
    geometry: Geometry
    properties: dict[str, Any] = Field(
        default_factory=dict,
        description="scenario functional zone additional properties",
        examples=[{"additional_attribute_name": "additional_attribute_value"}],
    )


class ScenarioFunctionalZonePut(NotPointGeometryValidationModel):
    """Project scenario functional zone schema for PUT requests."""

    functional_zone_type_id: int = Field(
        ..., description="functional zone type identifier for the scenario functional zone", examples=[1]
    )
    name: str | None = Field(..., description="functional zone name", examples=["--"])
    geometry: Geometry
    year: int = Field(..., description="year when functional zone was loaded", examples=[2024])
    source: str = Field(..., description="source from which functional zone was loaded", examples=["--"])
    properties: dict[str, Any] = Field(
        ...,
        description="scenario functional zone additional properties",
        examples=[{"additional_attribute_name": "additional_attribute_value"}],
    )


class ScenarioFunctionalZonePatch(NotPointGeometryValidationModel):
    """Project scenario functional zone schema for PATCH requests."""

    functional_zone_type_id: int | None = Field(
        None, description="functional zone type identifier for the scenario functional zone", examples=[1]
    )
    name: str | None = Field(None, description="functional zone name", examples=["--"])
    year: int | None = Field(None, description="year when functional zone was loaded", examples=[2024])
    source: str | None = Field(None, description="source from which functional zone was loaded", examples=["--"])
    geometry: Geometry | None = None
    properties: dict[str, Any] | None = Field(
        None,
        description="scenario functional zone additional properties",
        examples=[{"additional_attribute_name": "additional_attribute_value"}],
    )

    @classmethod
    @model_validator(mode="before")
    def check_empty_request(cls, values):
        """Ensure the request body is not empty."""
        if not values:
            raise ValueError("request body cannot be empty")
        return values
