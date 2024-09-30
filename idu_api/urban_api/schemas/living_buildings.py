from typing import Any, Optional

from pydantic import BaseModel, Field, model_validator

from idu_api.urban_api.dto import LivingBuildingsDTO, LivingBuildingsWithGeometryDTO
from idu_api.urban_api.schemas.geometries import Geometry
from idu_api.urban_api.schemas.physical_objects import PhysicalObjectsData, PhysicalObjectsTypes


class LivingBuildingsWithGeometry(BaseModel):
    living_building_id: int = Field(..., examples=[1])
    physical_object: PhysicalObjectsData
    residents_number: int | None = Field(..., examples=[200])
    living_area: float | None = Field(..., examples=[300.0])
    properties: dict[str, Any] = Field(
        default_factory=dict,
        description="Additional properties",
        examples=[{"additional_attribute_name": "additional_attribute_value"}],
    )
    geometry: Geometry
    centre_point: Geometry
    address: Optional[str] = Field(None, description="geometry address", examples=["--"])

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
                    physical_object_type_id=dto.physical_object_type_id, name=dto.physical_object_type_name
                ),
                name=dto.physical_object_name,
                properties=dto.physical_object_properties,
                created_at=dto.physical_object_created_at,
                updated_at=dto.physical_object_updated_at,
            ),
            address=dto.physical_object_address,
            residents_number=dto.residents_number,
            living_area=dto.living_area,
            properties=dto.properties,
            geometry=Geometry.from_shapely_geometry(dto.geometry),
            centre_point=Geometry.from_shapely_geometry(dto.centre_point),
        )


class LivingBuildingsData(BaseModel):
    living_building_id: int = Field(..., examples=[1])
    physical_object: PhysicalObjectsData
    residents_number: int | None = Field(..., examples=[200])
    living_area: float | None = Field(..., examples=[300.0])
    properties: dict[str, Any] = Field(
        default_factory=dict,
        description="Additional properties",
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
                    physical_object_type_id=dto.physical_object_type_id, name=dto.physical_object_type_name
                ),
                name=dto.physical_object_name,
                created_at=dto.physical_object_created_at,
                updated_at=dto.physical_object_updated_at,
            ),
            residents_number=dto.residents_number,
            living_area=dto.living_area,
            properties=dto.properties,
        )


class LivingBuildingsDataPost(BaseModel):
    physical_object_id: int = Field(..., examples=[1])
    residents_number: int | None = Field(None, examples=[200])
    living_area: float | None = Field(None, examples=[300.0])
    properties: dict[str, Any] = Field(
        default_factory=dict,
        description="Additional properties",
        examples=[{"additional_attribute_name": "additional_attribute_value"}],
    )


class LivingBuildingsDataPut(BaseModel):
    physical_object_id: int = Field(..., examples=[1])
    residents_number: int | None = Field(..., examples=[200])
    living_area: float | None = Field(..., examples=[300.0])
    properties: dict[str, Any] = Field(
        ..., description="Additional properties", examples=[{"additional_attribute_name": "additional_attribute_value"}]
    )


class LivingBuildingsDataPatch(BaseModel):
    physical_object_id: int | None = Field(None, examples=[1])
    residents_number: int | None = Field(None, examples=[200])
    living_area: float | None = Field(None, examples=[300.0])
    properties: Optional[dict[str, Any]] = Field(
        None,
        description="Additional properties",
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
