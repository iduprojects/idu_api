"""Functional zones schemas are defined here."""

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field, model_validator

from idu_api.urban_api.dto import FunctionalZoneDataDTO, FunctionalZoneTypeDTO, ProjectsProfileDTO
from idu_api.urban_api.schemas.geometries import Geometry, NotPointGeometryValidationModel
from idu_api.urban_api.schemas.short_models import FunctionalZoneTypeBasic, ShortScenario, ShortTerritory


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


class FunctionalZoneData(BaseModel):
    """Functional zone with all its attributes."""

    functional_zone_id: int = Field(..., description="functional zone identifier", examples=[1])
    territory: ShortTerritory
    functional_zone_type: FunctionalZoneTypeBasic
    name: str | None = Field(..., description="functional zone name", examples=[1])
    geometry: Geometry
    properties: dict[str, Any] = Field(
        default_factory=dict,
        description="functional zone additional properties",
        examples=[{"additional_attribute_name": "additional_attribute_value"}],
    )
    created_at: datetime = Field(
        default_factory=datetime.utcnow, description="the time when the functional zone was created"
    )
    updated_at: datetime = Field(
        default_factory=datetime.utcnow, description="the time when the functional zone was last updated"
    )

    @classmethod
    def from_dto(cls, dto: FunctionalZoneDataDTO) -> "FunctionalZoneData":
        return cls(
            functional_zone_id=dto.functional_zone_id,
            territory=ShortTerritory(id=dto.territory_id, name=dto.territory_name),
            functional_zone_type=FunctionalZoneTypeBasic(
                id=dto.functional_zone_type_id,
                name=dto.functional_zone_type_name,
            ),
            name=dto.name,
            geometry=Geometry.from_shapely_geometry(dto.geometry),
            properties=dto.properties,
            created_at=dto.created_at,
            updated_at=dto.updated_at,
        )


class FunctionalZoneDataPost(NotPointGeometryValidationModel):
    """Functional zone schema for POST requests."""

    territory_id: int = Field(..., description="territory identifier where functional zone is", examples=[1])
    functional_zone_type_id: int = Field(..., description="functional zone type identifier", examples=[1])
    name: str | None = Field(None, description="functional zone name", examples=[1])
    geometry: Geometry
    properties: dict[str, Any] = Field(
        default_factory=dict,
        description="functional zone additional properties",
        examples=[{"additional_attribute_name": "additional_attribute_value"}],
    )


class FunctionalZoneDataPut(NotPointGeometryValidationModel):
    """Functional zone schema for PUT requests."""

    territory_id: int = Field(..., description="territory identifier where functional zone is", examples=[1])
    functional_zone_type_id: int = Field(..., description="functional zone type identifier", examples=[1])
    name: str = Field(..., description="functional zone name", examples=[1])
    geometry: Geometry
    properties: dict[str, Any] = Field(
        ...,
        description="functional zone additional properties",
        examples=[{"additional_attribute_name": "additional_attribute_value"}],
    )


class FunctionalZoneDataPatch(NotPointGeometryValidationModel):
    """Functional zone schema for PATCH requests."""

    territory_id: int | None = Field(None, description="territory identifier where functional zone is", examples=[1])
    functional_zone_type_id: int | None = Field(None, description="functional zone type identifier", examples=[1])
    name: str | None = Field(None, description="functional zone name", examples=[1])
    geometry: Geometry | None = None
    properties: dict[str, Any] | None = Field(
        default_factory=None,
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


class ProjectsProfile(BaseModel):
    """Project profile with all its attributes."""

    profile_id: int = Field(..., description="profile identifier", examples=[1])
    scenario: ShortScenario
    functional_zone_type: FunctionalZoneTypeBasic
    name: str | None = Field(..., description="functional zone name", examples=[1])
    geometry: Geometry
    properties: dict[str, Any] = Field(
        default_factory=dict,
        description="profile additional properties",
        examples=[{"additional_attribute_name": "additional_attribute_value"}],
    )
    created_at: datetime = Field(default_factory=datetime.utcnow, description="the time when the profile was created")
    updated_at: datetime = Field(
        default_factory=datetime.utcnow, description="the time when the profile was last updated"
    )

    @classmethod
    def from_dto(cls, dto: ProjectsProfileDTO) -> "ProjectsProfile":
        return cls(
            profile_id=dto.profile_id,
            scenario=ShortScenario(id=dto.scenario_id, name=dto.scenario_name),
            functional_zone_type=FunctionalZoneTypeBasic(
                id=dto.functional_zone_type_id,
                name=dto.functional_zone_type_name,
            ),
            name=dto.name,
            geometry=Geometry.from_shapely_geometry(dto.geometry),
            properties=dto.properties,
            created_at=dto.created_at,
            updated_at=dto.updated_at,
        )


class ProjectsProfilePost(NotPointGeometryValidationModel):
    """Project profile schema for POST requests."""

    profile_id: int = Field(..., description="profile identifier", examples=[1])
    scenario_id: int = Field(..., description="scenario identifier where profile is used", examples=[1])
    functional_zone_type_id: int = Field(
        ..., description="functional zone type identifier for the profile", examples=[1]
    )
    name: str | None = Field(None, description="functional zone name", examples=[1])
    geometry: Geometry
    properties: dict[str, Any] = Field(
        default_factory=dict,
        description="profile additional properties",
        examples=[{"additional_attribute_name": "additional_attribute_value"}],
    )


class ProjectsProfilePut(NotPointGeometryValidationModel):
    """Project profile schema for PUT requests."""

    profile_id: int = Field(..., description="profile identifier", examples=[1])
    scenario_id: int = Field(..., description="scenario identifier where profile is used", examples=[1])
    functional_zone_type_id: int = Field(
        ..., description="functional zone type identifier for the profile", examples=[1]
    )
    name: str = Field(..., description="functional zone name", examples=[1])
    geometry: Geometry
    properties: dict[str, Any] = Field(
        ...,
        description="profile additional properties",
        examples=[{"additional_attribute_name": "additional_attribute_value"}],
    )


class ProjectsProfilePatch(NotPointGeometryValidationModel):
    """Project profile schema for PATCH requests."""

    profile_id: int | None = Field(None, description="profile identifier", examples=[1])
    scenario_id: int | None = Field(None, description="scenario identifier where profile is used", examples=[1])
    functional_zone_type_id: int | None = Field(
        None, description="functional zone type identifier for the profile", examples=[1]
    )
    name: str | None = Field(None, description="functional zone name", examples=[1])
    geometry: Geometry | None = None
    properties: dict[str, Any] | None = Field(
        default_factory=None,
        description="profile additional properties",
        examples=[{"additional_attribute_name": "additional_attribute_value"}],
    )

    @model_validator(mode="before")
    @classmethod
    def check_empty_request(cls, values):
        """Ensure the request body is not empty."""
        if not values:
            raise ValueError("request body cannot be empty")
        return values
