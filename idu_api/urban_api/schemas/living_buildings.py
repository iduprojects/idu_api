"""Living buildings schemas are defined here."""

from typing import Any

from pydantic import BaseModel, Field, model_validator

from idu_api.urban_api.dto import LivingBuildingsDTO, LivingBuildingsWithGeometryDTO
from idu_api.urban_api.schemas.geometries import Geometry
from idu_api.urban_api.schemas.physical_object_types import PhysicalObjectFunctionBasic
from idu_api.urban_api.schemas.physical_objects import PhysicalObjectsData, PhysicalObjectsTypes


class LivingBuildingsWithGeometry(BaseModel):
    """Living building with all its attributes and geometry."""

    living_building_id: int = Field(..., examples=[1])
    physical_object: PhysicalObjectsData
    living_area: float | None = Field(..., examples=[300.0])
    properties: dict[str, Any] = Field(
        default_factory=dict,
        description="additional properties",
        examples=[{"additional_attribute_name": "additional_attribute_value"}],
    )
    geometry: Geometry
    centre_point: Geometry
    address: str | None = Field(None, description="geometry address", examples=["--"])
    osm_id: str | None = Field(None, description="open street map identifier", examples=["1"])

    @classmethod
    def from_dto(cls, dto: LivingBuildingsWithGeometryDTO) -> "LivingBuildingsWithGeometry":
        """
        Construct from DTO.
        """
        return cls(
            living_building_id=dto.living_building_id,
            physical_object=PhysicalObjectsData(
                physical_object_id=dto.physical_object_id,
                physical_object_type=PhysicalObjectsTypes(
                    physical_object_type_id=dto.physical_object_type_id,
                    name=dto.physical_object_type_name,
                    physical_object_function=PhysicalObjectFunctionBasic(
                        id=dto.physical_object_function_id, name=dto.physical_object_function_name
                    ),
                ),
                name=dto.physical_object_name,
                properties=dto.physical_object_properties,
                created_at=dto.physical_object_created_at,
                updated_at=dto.physical_object_updated_at,
            ),
            address=dto.physical_object_address,
            osm_id=dto.object_geometry_osm_id,
            living_area=dto.living_area,
            properties=dto.properties,
            geometry=Geometry.from_shapely_geometry(dto.geometry),
            centre_point=Geometry.from_shapely_geometry(dto.centre_point),
        )


class LivingBuildingsData(BaseModel):
    """Living building with all its attributes."""

    living_building_id: int = Field(..., examples=[1])
    physical_object: PhysicalObjectsData
    living_area: float | None = Field(..., examples=[300.0])
    properties: dict[str, Any] = Field(
        default_factory=dict,
        description="additional properties",
        examples=[{"additional_attribute_name": "additional_attribute_value"}],
    )

    @classmethod
    def from_dto(cls, dto: LivingBuildingsDTO) -> "LivingBuildingsData":
        """
        Construct from DTO.
        """
        return cls(
            living_building_id=dto.living_building_id,
            physical_object=PhysicalObjectsData(
                physical_object_id=dto.physical_object_id,
                physical_object_type=PhysicalObjectsTypes(
                    physical_object_type_id=dto.physical_object_type_id,
                    name=dto.physical_object_type_name,
                    physical_object_function=(
                        PhysicalObjectFunctionBasic(
                            id=dto.physical_object_function_id, name=dto.physical_object_function_name
                        )
                        if dto.physical_object_function_id is not None
                        else None
                    ),
                ),
                name=dto.physical_object_name,
                created_at=dto.physical_object_created_at,
                updated_at=dto.physical_object_updated_at,
            ),
            living_area=dto.living_area,
            properties=dto.properties,
        )


class LivingBuildingsDataPost(BaseModel):
    """Living building schema for POST requests."""

    physical_object_id: int = Field(..., examples=[1])
    living_area: float | None = Field(None, examples=[300.0])
    properties: dict[str, Any] = Field(
        default_factory=dict,
        description="additional properties",
        examples=[{"additional_attribute_name": "additional_attribute_value"}],
    )


class LivingBuildingsDataPut(BaseModel):
    """Living building schema for PUT requests."""

    physical_object_id: int = Field(..., examples=[1])
    living_area: float | None = Field(..., examples=[300.0])
    properties: dict[str, Any] = Field(
        ..., description="additional properties", examples=[{"additional_attribute_name": "additional_attribute_value"}]
    )


class LivingBuildingsDataPatch(BaseModel):
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
