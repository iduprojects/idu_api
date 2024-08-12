from datetime import datetime
from typing import Optional, Dict, Any

from pydantic import BaseModel, Field, field_validator, model_validator
from loguru import logger

from idu_api.urban_api.schemas.geometries import Geometry
from idu_api.urban_api.dto import ProjectDTO, ProjectTerritoryDTO


class ProjectTerritory(BaseModel):
    """
    Schema of project's territory for GET request
    """

    project_territory_id: int = Field(None, primary_key=True, examples=[1])
    parent_id: Optional[int] = Field(None, description="Project's parent territory id")
    geometry: Geometry = Field(None, description="Project geometry")
    centre_point: Geometry = Field(None, description="Project centre point")
    properties: Dict[str, Any] = Field(
        None,
        description="Project's territory additional properties",
        example={"additional_attribute_name": "additional_attribute_value"},
    )

    @classmethod
    def from_dto(cls, dto: ProjectTerritoryDTO) -> "ProjectTerritory":
        """Construct from DTO"""

        return cls(
            project_territory_id=dto.project_territory_id,
            parent_id=dto.parent_id,
            geometry=Geometry.from_shapely_geometry(dto.geometry),
            centre_point=Geometry.from_shapely_geometry(dto.centre_point),
            properties=dto.properties,
        )


class ProjectTerritoryPost(BaseModel):
    """
    Schema of project's territory for POST request
    """

    parent_id: Optional[int] = Field(description="Project's parent territory id")
    geometry: Geometry = Field(description="Project geometry")
    centre_point: Optional[Geometry] = Field(description="Project centre point")
    properties: Dict[str, Any] = Field(
        default_factory=dict,
        description="Service additional properties",
        example={"additional_attribute_name": "additional_attribute_value"},
    )

    @field_validator("geometry")
    @staticmethod
    def validate_geometry(geometry: Geometry) -> Geometry:
        """Validate that given geometry is validity via creating Shapely object."""

        try:
            geometry.as_shapely_geometry()
        except (AttributeError, ValueError, TypeError) as exc:
            logger.debug("Exception on passing geometry: {!r}", exc)
            raise ValueError("Invalid geometry passed") from exc
        return geometry

    @field_validator("centre_point")
    @staticmethod
    def validate_center(centre_point: Geometry | None) -> Optional[Geometry]:
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

    @model_validator(mode="after")
    @staticmethod
    def validate_post(model: "ProjectTerritoryPost") -> "ProjectTerritoryPost":
        """Use geometry centroid for centre_point if it is missing."""

        if model.centre_point is None:
            model.centre_point = Geometry.from_shapely_geometry(model.geometry.as_shapely_geometry().centroid)
        return model


class ProjectTerritoryPut(BaseModel):
    """
    Schema of project's territory for PUT request
    """

    parent_id: Optional[int] = Field(description="Project's parent territory id")
    geometry: Geometry = Field(description="Project geometry")
    centre_point: Optional[Geometry] = Field(description="Project centre point")
    properties: Dict[str, Any] = Field(
        default_factory=dict,
        description="Service additional properties",
        example={"additional_attribute_name": "additional_attribute_value"},
    )

    @field_validator("geometry")
    @staticmethod
    def validate_geometry(geometry: Geometry) -> Geometry:
        """Validate that given geometry is validity via creating Shapely object."""

        try:
            geometry.as_shapely_geometry()
        except (AttributeError, ValueError, TypeError) as exc:
            logger.debug("Exception on passing geometry: {!r}", exc)
            raise ValueError("Invalid geometry passed") from exc
        return geometry

    @field_validator("centre_point")
    @staticmethod
    def validate_center(centre_point: Geometry | None) -> Optional[Geometry]:
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

    @model_validator(mode="after")
    @staticmethod
    def validate_post(model: "ProjectTerritoryPost") -> "ProjectTerritoryPost":
        """Use geometry centroid for centre_point if it is missing."""

        if model.centre_point is None:
            model.centre_point = Geometry.from_shapely_geometry(model.geometry.as_shapely_geometry().centroid)
        return model


class ProjectTerritoryPatch(BaseModel):
    """
    Schema of project's territory for PATCH request
    """

    parent_id: Optional[int] = Field(None, description="Project's parent territory id")
    geometry: Optional[Geometry] = Field(None, description="Project geometry")
    centre_point: Optional[Geometry] = Field(None, description="Project centre point")
    properties: Optional[Dict[str, Any]] = Field(
        default_factory=dict,
        description="Service additional properties",
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

    @field_validator("geometry")
    @staticmethod
    def validate_geometry(geometry: Geometry) -> Geometry:
        """Validate that given geometry is validity via creating Shapely object."""

        try:
            geometry.as_shapely_geometry()
        except (AttributeError, ValueError, TypeError) as exc:
            logger.debug("Exception on passing geometry: {!r}", exc)
            raise ValueError("Invalid geometry passed") from exc
        return geometry

    @field_validator("centre_point")
    @staticmethod
    def validate_center(centre_point: Geometry | None) -> Optional[Geometry]:
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

    @model_validator(mode="after")
    @staticmethod
    def validate_post(model: "ProjectTerritoryPatch") -> "ProjectTerritoryPatch":
        """Use geometry centroid for centre_point if it is missing."""

        if model.centre_point is None:
            model.centre_point = Geometry.from_shapely_geometry(model.geometry.as_shapely_geometry().centroid)
        return model


class Project(BaseModel):
    """
    Schema of project for GET request
    """

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
    """
    Schema of project for POST request
    """

    user_id: int = Field(examples=[1])
    name: str = Field(example="--")
    project_territory_info: ProjectTerritoryPost = Field(description="Project territory info")
    description: str = Field(description="Project description")
    public: bool = Field(description="Project publicity")
    image_url: str = Field(description="Project image url")


class ProjectPut(BaseModel):
    """
    Schema of project for PUT request
    """

    user_id: int = Field(examples=[1])
    name: str = Field(example="--")
    project_territory_info: ProjectTerritoryPut = Field(description="Project territory info")
    description: str = Field(description="Project description")
    public: bool = Field(description="Project publicity")
    image_url: str = Field(description="Project image url")


class ProjectPatch(BaseModel):
    """
    Schema of project for PATCH request
    """

    user_id: Optional[int] = Field(None, examples=[1])
    name: Optional[str] = Field(None, example="--")
    project_territory_info: Optional[ProjectTerritoryPatch] = Field(None, description="Project territory info")
    description: Optional[str] = Field(None, description="Project description")
    public: Optional[bool] = Field(None, description="Project publicity")
    image_url: Optional[str] = Field(None, description="Project image url")

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
