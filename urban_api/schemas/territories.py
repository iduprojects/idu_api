"""
Territory schemas are defined here.
"""

from typing import Any, Optional

from loguru import logger
from pydantic import BaseModel, Field, field_validator, model_validator

from urban_api.dto import TerritoryDTO, TerritoryTypeDTO, TerritoryWithoutGeometryDTO
from urban_api.schemas.geometries import Geometry


class TerritoryTypes(BaseModel):
    """
    Territory type with all its attributes
    """

    territory_type_id: Optional[int] = Field(example=1, description="Territory type id, if set")
    name: str = Field(description="Territory type unit name", example="Город")

    @classmethod
    def from_dto(cls, dto: TerritoryTypeDTO) -> "TerritoryTypes":
        """
        Construct from DTO.
        """
        return cls(territory_type_id=dto.territory_type_id, name=dto.name)


class TerritoryTypesPost(BaseModel):
    """
    Schema of territory type for POST request
    """

    name: str = Field(description="Territory type unit name", example="Город")


class TerritoriesData(BaseModel):
    """
    Territory with all its attributes
    """

    territory_id: int = Field(examples=[1])
    territory_type: TerritoryTypes = Field(example={"territory_type_id": 1, "name": "name"})
    parent_id: Optional[int] = Field(
        example=1, description="Parent territory identifier, null only for the one territory"
    )
    name: str = Field(example="--", description="Territory name")
    geometry: Geometry = Field(description="Territory geometry")
    level: int = Field(example=1)
    properties: dict[str, str] = Field(
        description="Service additional properties",
        example={"additional_attribute_name": "additional_attribute_value"},
    )
    centre_point: Geometry = Field(description="Centre coordinates")
    admin_center: Optional[int] = Field(example=1)
    okato_code: Optional[str] = Field(example="1")

    @classmethod
    def from_dto(cls, dto: TerritoryDTO, territory_type_dto: TerritoryTypeDTO) -> "TerritoriesData":
        """
        Construct from DTO.
        """
        return cls(
            territory_id=dto.territory_id,
            territory_type=TerritoryTypes.from_dto(territory_type_dto),
            parent_id=dto.parent_id,
            name=dto.name,
            geometry=Geometry.from_shapely_geometry(dto.geometry),
            level=dto.level,
            properties=dto.properties,
            centre_point=Geometry.from_shapely_geometry(dto.centre_point),
            admin_center=dto.admin_center,
            okato_code=dto.okato_code,
        )


class TerritoriesDataPost(BaseModel):
    """
    Schema of territory for POST request
    """

    territory_type_id: int = Field(examples=[1])
    parent_id: int = Field(examples=[1])
    name: str = Field(examples=["--"], description="Territory name")
    geometry: Geometry = Field(description="Territory geometry")
    level: int = Field(examples=[1])
    properties: dict[str, Any] = Field(
        default_factory=dict,
        description="Service additional properties",
        example={"additional_attribute_name": "additional_attribute_value"},
    )
    centre_point: Optional[Geometry] = Field(None, description="Centre coordinates")
    admin_center: Optional[int] = Field(None, examples=[1])
    okato_code: Optional[str] = Field(None, examples=["1"])

    @field_validator("geometry")
    @staticmethod
    def validate_geometry(geometry: Geometry) -> Geometry:
        """
        Validate that given geometry is validity via creating Shapely object.
        """
        try:
            geometry.as_shapely_geometry()
        except (AttributeError, ValueError, TypeError) as exc:
            logger.debug("Exception on passing geometry: {!r}", exc)
            raise ValueError("Invalid geometry passed") from exc
        return geometry

    @field_validator("centre_point")
    @staticmethod
    def validate_center(centre_point: Geometry | None) -> Geometry:
        """
        Validate that given geometry is Point and validity via creating Shapely object.
        """
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
    def validate_post(model: "TerritoriesDataPost") -> "TerritoriesDataPost":
        """
        Use geometry centroid for centre_point if it is missing.
        """
        if model.centre_point is None:
            model.centre_point = Geometry.from_shapely_geometry(model.geometry.as_shapely_geometry().centroid)
        return model


class TerritoryWithoutGeometry(BaseModel):
    """
    Territory with all its attributes
    """

    territory_id: int = Field(examples=[1])
    territory_type: TerritoryTypes = Field(example={"territory_type_id": 1, "name": "name"})
    parent_id: Optional[int] = Field(
        examples=[1], description="Parent territory identifier, null only for the one territory"
    )
    name: str = Field(examples=["--"], description="Territory name")
    level: int = Field(examples=[1])
    properties: dict[str, str] = Field(
        description="Service additional properties",
        example={"additional_attribute_name": "additional_attribute_value"},
    )
    admin_center: Optional[int] = Field(examples=[1])
    okato_code: Optional[str] = Field(examples=["1"])

    @classmethod
    def from_dto(
            cls, dto: TerritoryWithoutGeometryDTO, territory_type_dto: TerritoryTypeDTO
    ) -> "TerritoryWithoutGeometry":
        """
        Construct from DTO.
        """
        return cls(
            territory_id=dto.territory_id,
            territory_type=TerritoryTypes.from_dto(territory_type_dto),
            parent_id=dto.parent_id,
            name=dto.name,
            level=dto.level,
            properties=dto.properties,
            admin_center=dto.admin_center,
            okato_code=dto.okato_code,
        )
