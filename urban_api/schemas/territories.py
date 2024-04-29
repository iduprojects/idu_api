"""
Territory schemas are defined here.
"""
from pydantic import BaseModel, Field, validator
from typing import Optional
from loguru import logger

from urban_api.dto import TerritoryTypeDTO, TerritoryDTO
from urban_api.schemas.geometries import Geometry


class TerritoryTypes(BaseModel):
    """
    Territory type with all its attributes
    """

    territory_type_id: Optional[int] = Field(examples=[1], description="Territory type id, if set")
    name: str = Field(description="Territory type unit name", examples=["Город"])

    @classmethod
    def from_dto(cls, dto: TerritoryTypeDTO) -> "TerritoryTypes":
        """
        Construct from DTO.
        """
        return cls(
            territory_type_id=dto.territory_type_id,
            name=dto.name
        )


class TerritoryTypesPost(BaseModel):
    """
    Schema of territory type for POST request
    """

    name: str = Field(description="Territory type unit name", examples=["Город"])


class TerritoriesData(BaseModel):
    """
    Territory with all its attributes
    """

    territory_id: int = Field(examples=[1])
    territory_type_id: int = Field(examples=[1])
    parent_id: int = Field(examples=[1])
    name: str = Field(examples=["--"], description="Territory name")
    geometry: Geometry = Field(description="Territory geometry")
    level: int = Field(examples=[1])
    properties: dict = Field(examples=[{"additional_attribute_name": "additional_attribute_value"}],
                             description="Territory additional parameters")
    centre_point: Geometry
    admin_center: int
    okato_code: str

    @classmethod
    def from_dto(cls, dto: TerritoryDTO) -> "TerritoriesData":
        """
        Construct from DTO.
        """
        return cls(
            territory_id=dto.territory_id,
            territory_type_id=dto.territory_type_id,
            parent_id=dto.parent_id,
            name=dto.name,
            geometry=dto.geometry,
            level=dto.level,
            properties=dto.properties,
            centre_point=dto.centre_point,
            admin_center=dto.admin_center,
            okato_code=dto.okato_code
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
    properties: dict = Field(examples=[{"additional_attribute_name": "additional_attribute_value"}],
                             description="Territory additional parameters")
    centre_point: Geometry
    admin_center: int
    okato_code: str

    @validator("geometry")
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

    @validator("centre_point")
    @staticmethod
    def validate_geometry(geometry: Geometry) -> Geometry:
        """
        Validate that given geometry is Point and validity via creating Shapely object.
        """
        assert geometry.type == "Point", "Only Point is accepted"
        try:
            geometry.as_shapely_geometry()
        except (AttributeError, ValueError, TypeError) as exc:
            logger.debug("Exception on passing geometry: {!r}", exc)
            raise ValueError("Invalid geometry passed") from exc
        return geometry
