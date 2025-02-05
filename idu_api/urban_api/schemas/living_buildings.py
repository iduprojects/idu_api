"""Living buildings schemas are defined here."""

from typing import Any

from pydantic import BaseModel, Field, model_validator

from idu_api.urban_api.dto import LivingBuildingDTO, LivingBuildingWithGeometryDTO
from idu_api.urban_api.schemas.geometries import Geometry
from idu_api.urban_api.schemas.short_models import PhysicalObjectTypeBasic, ShortPhysicalObjectWithoutLivingBuilding


class LivingBuildingWithGeometry(BaseModel):
    """Living building with all its attributes and geometry."""

    living_building_id: int = Field(..., examples=[1])
    physical_object: ShortPhysicalObjectWithoutLivingBuilding
    living_area: float | None = Field(..., examples=[300.0])
    properties: dict[str, Any] = Field(
        default_factory=dict,
        description="additional properties",
        examples=[{"additional_attribute_name": "additional_attribute_value"}],
    )
    object_geometry_id: int = Field(..., description="object geometry identifier", examples=[1])
    address: str | None = Field(None, description="geometry address", examples=["--"])
    osm_id: str | None = Field(None, description="open street map identifier", examples=["1"])
    geometry: Geometry
    centre_point: Geometry

    @classmethod
    def from_dto(cls, dto: LivingBuildingWithGeometryDTO) -> "LivingBuildingWithGeometry":
        """
        Construct from DTO.
        """
        return cls(
            living_building_id=dto.living_building_id,
            physical_object=ShortPhysicalObjectWithoutLivingBuilding(
                physical_object_id=dto.physical_object_id,
                physical_object_type=PhysicalObjectTypeBasic(
                    id=dto.physical_object_type_id,
                    name=dto.physical_object_type_name,
                ),
                name=dto.physical_object_name,
                properties=dto.physical_object_properties,
            ),
            living_area=dto.living_area,
            properties=dto.properties,
            object_geometry_id=dto.object_geometry_id,
            address=dto.address,
            osm_id=dto.osm_id,
            geometry=Geometry.from_shapely_geometry(dto.geometry),
            centre_point=Geometry.from_shapely_geometry(dto.centre_point),
        )


class LivingBuilding(BaseModel):
    """Living building with all its attributes."""

    living_building_id: int = Field(..., examples=[1])
    physical_object: ShortPhysicalObjectWithoutLivingBuilding
    living_area: float | None = Field(..., examples=[300.0])
    properties: dict[str, Any] = Field(
        default_factory=dict,
        description="additional properties",
        examples=[{"additional_attribute_name": "additional_attribute_value"}],
    )

    @classmethod
    def from_dto(cls, dto: LivingBuildingDTO) -> "LivingBuilding":
        """
        Construct from DTO.
        """
        return cls(
            living_building_id=dto.living_building_id,
            physical_object=ShortPhysicalObjectWithoutLivingBuilding(
                physical_object_id=dto.physical_object_id,
                physical_object_type=PhysicalObjectTypeBasic(
                    id=dto.physical_object_type_id,
                    name=dto.physical_object_type_name,
                ),
                name=dto.physical_object_name,
                properties=dto.physical_object_properties,
            ),
            living_area=dto.living_area,
            properties=dto.properties,
        )


class LivingBuildingPost(BaseModel):
    """Living building schema for POST requests."""

    physical_object_id: int = Field(..., examples=[1])
    living_area: float | None = Field(None, examples=[300.0])
    properties: dict[str, Any] = Field(
        default_factory=dict,
        description="additional properties",
        examples=[{"additional_attribute_name": "additional_attribute_value"}],
    )


class LivingBuildingPut(BaseModel):
    """Living building schema for PUT requests."""

    physical_object_id: int = Field(..., examples=[1])
    living_area: float | None = Field(..., examples=[300.0])
    properties: dict[str, Any] = Field(
        ..., description="additional properties", examples=[{"additional_attribute_name": "additional_attribute_value"}]
    )


class LivingBuildingPatch(BaseModel):
    """Living building schema for PATCH requests."""

    physical_object_id: int | None = Field(None, examples=[1])
    living_area: float | None = Field(None, examples=[300.0])
    properties: dict[str, Any] | None = Field(
        None,
        description="additional properties",
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
