from datetime import datetime
from typing import Any

from loguru import logger
from pydantic import BaseModel, Field, field_validator, model_validator

from idu_api.urban_api.dto import ProjectDTO, ProjectTerritoryDTO
from idu_api.urban_api.schemas.geometries import Geometry


class ProjectTerritory(BaseModel):
    """Schema of project's territory for GET request."""

    project_territory_id: int = Field(None, primary_key=True, examples=[1])
    parent_territory_id: int | None = Field(None, description="Project's parent territory id")
    geometry: Geometry = Field(None, description="Project geometry")
    centre_point: Geometry = Field(None, description="Project centre point")
    properties: dict[str, Any] = Field(
        None,
        description="Project's territory additional properties",
        example={"additional_attribute_name": "additional_attribute_value"},
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


class ProjectTerritoryPost(BaseModel):
    """Schema of project's territory for POST request."""

    parent_territory_id: int | None = Field(description="Project's parent territory id")
    geometry: Geometry = Field(description="Project geometry")
    centre_point: Geometry | None = Field(description="Project centre point")
    properties: dict[str, Any] = Field(
        default_factory=dict,
        description="Service additional properties",
        example={"additional_attribute_name": "additional_attribute_value"},
    )

    @field_validator("geometry")
    @staticmethod
    def validate_geometry(geometry: Geometry) -> Geometry:
        return validate_geometry(geometry)

    @field_validator("centre_point")
    @staticmethod
    def validate_center(centre_point: Geometry | None) -> Geometry | None:
        return validate_center(centre_point)

    @model_validator(mode="after")
    @staticmethod
    def validate_post(model: "ProjectTerritoryPost") -> "ProjectTerritoryPost":
        """Use geometry centroid for centre_point if it is missing."""

        if model.centre_point is None:
            model.centre_point = Geometry.from_shapely_geometry(model.geometry.as_shapely_geometry().centroid)
        return model


class ProjectTerritoryPut(BaseModel):
    """Schema of project's territory for PUT request."""

    parent_territory_id: int | None = Field(description="Project's parent territory id")
    geometry: Geometry = Field(description="Project geometry")
    centre_point: Geometry | None = Field(description="Project centre point")
    properties: dict[str, Any] = Field(
        default_factory=dict,
        description="Service additional properties",
        example={"additional_attribute_name": "additional_attribute_value"},
    )

    @field_validator("geometry")
    @staticmethod
    def validate_geometry(geometry: Geometry) -> Geometry:
        return validate_geometry(geometry)

    @field_validator("centre_point")
    @staticmethod
    def validate_center(centre_point: Geometry | None) -> Geometry | None:
        return validate_center(centre_point)

    @model_validator(mode="after")
    @staticmethod
    def validate_post(model: "ProjectTerritoryPost") -> "ProjectTerritoryPost":
        """Use geometry centroid for centre_point if it is missing."""

        if model.centre_point is None:
            model.centre_point = Geometry.from_shapely_geometry(model.geometry.as_shapely_geometry().centroid)
        return model


class ProjectTerritoryPatch(BaseModel):
    """Schema of project's territory for PATCH request."""

    parent_territory_id: int | None = Field(None, description="Project's parent territory id")
    geometry: Geometry | None = Field(None, description="Project geometry")
    centre_point: Geometry | None = Field(None, description="Project centre point")
    properties: dict[str, Any] | None = Field(
        default_factory=dict,
        description="Service additional properties",
        example={"additional_attribute_name": "additional_attribute_value"},
    )

    @model_validator(mode="before")
    @classmethod
    def check_empty_request(cls, values):
        """Ensure the request body is not empty."""
        if not values:
            raise ValueError("request body cannot be empty")
        return values

    @field_validator("geometry")
    @staticmethod
    def validate_geometry(geometry: Geometry) -> Geometry:
        return validate_geometry(geometry)

    @field_validator("centre_point")
    @staticmethod
    def validate_center(centre_point: Geometry | None) -> Geometry | None:
        return validate_center(centre_point)

    @model_validator(mode="after")
    @staticmethod
    def validate_post(model: "ProjectTerritoryPatch") -> "ProjectTerritoryPatch":
        """Use geometry centroid for centre_point if it is missing."""

        if model.centre_point is None:
            model.centre_point = Geometry.from_shapely_geometry(model.geometry.as_shapely_geometry().centroid)
        return model


class Project(BaseModel):
    """Schema of project for GET request."""

    project_id: int = Field(primary_key=True, examples=[1])
    user_id: int = Field(examples=[1])
    name: str = Field(example="--")
    project_territory_id: int = Field(examples=[1])
    description: str = Field(description="Project description")
    public: bool = Field(description="Project publicity")
    image_url: str = Field(description="Project image url")
    created_at: datetime = Field(description="Project created at")
    updated_at: datetime = Field(description="Project updated at")

    @classmethod
    def from_dto(cls, dto: ProjectDTO) -> "Project":
        return cls(
            project_id=dto.project_id,
            user_id=dto.user_id,
            name=dto.name,
            project_territory_id=dto.project_territory_id,
            description=dto.description,
            public=dto.public,
            image_url=dto.image_url,
            created_at=dto.created_at,
            updated_at=dto.updated_at,
        )


class ProjectPost(BaseModel):
    """Schema of project for POST request."""

    user_id: int = Field(examples=[1])
    name: str = Field(example="--")
    project_territory_info: ProjectTerritoryPost = Field(description="Project territory info")
    description: str = Field(description="Project description")
    public: bool = Field(description="Project publicity")
    image_url: str = Field(description="Project image url")


class ProjectPut(BaseModel):
    """Schema of project for PUT request."""

    user_id: int = Field(examples=[1])
    name: str = Field(example="--")
    project_territory_info: ProjectTerritoryPut = Field(description="Project territory info")
    description: str = Field(description="Project description")
    public: bool = Field(description="Project publicity")
    image_url: str = Field(description="Project image url")


class ProjectPatch(BaseModel):
    """Schema of project for PATCH request."""

    user_id: int | None = Field(None, examples=[1])
    name: str | None = Field(None, example="--")
    project_territory_info: ProjectTerritoryPatch | None = Field(None, description="Project territory info")
    description: str | None = Field(None, description="Project description")
    public: bool | None = Field(None, description="Project publicity")
    image_url: str | None = Field(None, description="Project image url")

    @model_validator(mode="before")
    @classmethod
    def check_empty_request(cls, values):
        """Ensure the request body is not empty."""
        if not values:
            raise ValueError("request body cannot be empty")
        return values


def validate_geometry(geometry: Geometry) -> Geometry:
    """Validate that given geometry is validity via creating Shapely object."""

    try:
        geometry.as_shapely_geometry()
    except (AttributeError, ValueError, TypeError) as exc:
        logger.debug("Exception on passing geometry: {!r}", exc)
        raise ValueError("Invalid geometry passed") from exc
    return geometry


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
