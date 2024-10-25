from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field, model_validator

from idu_api.urban_api.dto import ProjectDTO, ProjectTerritoryDTO
from idu_api.urban_api.schemas.geometries import Geometry, GeometryValidationModel


class ProjectTerritory(BaseModel):
    """Schema of project territory for GET request."""

    project_territory_id: int = Field(description="Project territory id", examples=[1])
    parent_territory_id: int | None = Field(None, description="Project parent territory id", examples=[1])
    geometry: Geometry
    centre_point: Geometry
    properties: dict[str, Any] = Field(
        default_factory=dict,
        description="Project territory additional properties",
        examples=[{"attribute_name": "attribute_value"}],
    )

    @classmethod
    def from_dto(cls, dto: ProjectTerritoryDTO) -> "ProjectTerritory":
        """Construct from DTO"""

        return cls(
            project_territory_id=dto.project_territory_id,
            parent_territory_id=dto.parent_territory_id,
            geometry=Geometry.from_shapely_geometry(dto.geometry),
            centre_point=Geometry.from_shapely_geometry(dto.centre_point),
            properties=dto.properties,
        )


class ProjectTerritoryPost(GeometryValidationModel):
    """Schema of project territory for POST request."""

    geometry: Geometry
    centre_point: Geometry | None = None
    properties: dict[str, Any] = Field(
        default_factory=dict,
        description="Project territory additional properties",
        examples=[{"attribute_name": "attribute_value"}],
    )


class ProjectTerritoryPut(GeometryValidationModel):
    """Schema of project territory for PUT request."""

    parent_territory_id: int | None = Field(..., description="Project parent territory id", examples=[1])
    geometry: Geometry
    centre_point: Geometry
    properties: dict[str, Any] = Field(
        ...,
        description="Project territory additional properties",
        example={"attribute_name": "attribute_value"},
    )


class ProjectTerritoryPatch(GeometryValidationModel):
    """Schema of project territory for PATCH request."""

    parent_territory_id: int | None = Field(None, description="Project parent territory id", examples=[1])
    geometry: Geometry | None = Field(None, description="Project geometry")
    centre_point: Geometry | None = Field(None, description="Project centre point")
    properties: dict[str, Any] | None = Field(
        None,
        description="Project territory additional properties",
        example={"attribute_name": "attribute_value"},
    )

    @model_validator(mode="before")
    @classmethod
    def check_empty_request(cls, values):
        """Ensure the request body is not empty."""
        if not values:
            raise ValueError("request body cannot be empty")
        return values


class Project(BaseModel):
    """Schema of project for GET request."""

    project_id: int = Field(description="Project id", examples=[1])
    user_id: str = Field(description="Project creator id", examples=["admin@test.ru"])
    name: str = Field(description="Project name", examples=["--"])
    project_territory_id: int = Field(description="Project territory id", examples=[1])
    description: str | None = Field(None, description="Project description", examples=["--"])
    public: bool = Field(description="Project publicity", examples=[True])
    created_at: datetime = Field(default_factory=datetime.utcnow, description="Project created at")
    updated_at: datetime = Field(default_factory=datetime.utcnow, description="Project updated at")
    properties: dict[str, Any] = Field(
        default_factory=dict,
        description="Project's additional properties",
        examples=[{"additional_attribute_name": "additional_attribute_value"}],
    )

    @classmethod
    def from_dto(cls, dto: ProjectDTO) -> "Project":
        return cls(
            project_id=dto.project_id,
            user_id=dto.user_id,
            name=dto.name,
            project_territory_id=dto.project_territory_id,
            description=dto.description,
            public=dto.public,
            created_at=dto.created_at,
            updated_at=dto.updated_at,
            properties=dto.properties,
        )


class ProjectPost(BaseModel):
    """Schema of project for POST request."""

    name: str = Field(description="Project name", examples=["--"])
    project_territory_info: ProjectTerritoryPost
    description: str | None = Field(None, description="Project description", examples=["--"])
    public: bool = Field(description="Project publicity", examples=[True])
    properties: dict[str, Any] = Field(
        default_factory=dict,
        description="Project's additional properties",
        examples=[{"additional_attribute_name": "additional_attribute_value"}],
    )


class ProjectPut(BaseModel):
    """Schema of project for PUT request."""

    name: str = Field(..., description="Project name", examples=["--"])
    project_territory_info: ProjectTerritoryPost
    description: str | None = Field(None, description="Project description", examples=["--"])
    public: bool = Field(..., description="Project publicity", examples=[True])
    properties: dict[str, Any] = Field(
        default_factory=dict,
        description="Project's additional properties",
        examples=[{"additional_attribute_name": "additional_attribute_value"}],
    )


class ProjectPatch(BaseModel):
    """Schema of project for PATCH request."""

    name: str | None = Field(None, description="Project name", examples=["--"])
    project_territory_info: ProjectTerritoryPatch | None = None
    description: str | None = Field(None, description="Project description", examples=["--"])
    public: bool | None = Field(None, description="Project publicity", examples=[True])
    properties: dict[str, Any] | None = Field(
        default_factory=dict,
        description="Project's additional properties",
        examples=[{"additional_attribute_name": "additional_attribute_value"}],
    )

    @model_validator(mode="before")
    @classmethod
    def check_empty_request(cls, values):
        """Ensure the request body is not empty."""
        if not values:
            raise ValueError("request body cannot be empty")
        return values
