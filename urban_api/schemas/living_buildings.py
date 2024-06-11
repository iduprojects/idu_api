from typing import Any, Dict

from pydantic import BaseModel, Field

from urban_api.dto import LivingBuildingsDTO, LivingBuildingsWithGeometryDTO
from urban_api.schemas.geometries import Geometry
from urban_api.schemas.physical_objects import PhysicalObjectsData, PhysicalObjectsTypes


class LivingBuildingsWithGeometry(BaseModel):
    living_building_id: int = Field(example=1)
    physical_object: PhysicalObjectsData = Field(
        example={
            "physical_object_type": {"physical_object_type_id": 1, "name": "Здание"},
            "name": "--",
            "properties": {"additional_attribute_name": "additional_attribute_value"},
        }
    )
    residents_number: int = Field(example=200)
    living_area: float = Field(example=300.0)
    properties: Dict[str, Any] = Field(
        {}, description="Additional properties", example={"additional_attribute_name": "additional_attribute_value"}
    )
    geometry: Geometry = Field(description="Object geometry")
    centre_point: Geometry = Field(description="Centre coordinates")

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
                address=dto.physical_object_type_address,
                properties=dto.physical_object_properties,
            ),
            residents_number=dto.residents_number,
            living_area=dto.living_area,
            properties=dto.properties,
            geometry=Geometry.from_shapely_geometry(dto.geometry),
            centre_point=Geometry.from_shapely_geometry(dto.centre_point),
        )


class LivingBuildingsData(BaseModel):
    living_building_id: int = Field(example=1)
    physical_object: PhysicalObjectsData = Field(
        example={
            "physical_object_type": {"physical_object_type_id": 1, "name": "Здание"},
            "name": "--",
            "properties": {"additional_attribute_name": "additional_attribute_value"},
        }
    )
    residents_number: int = Field(example=200)
    living_area: float = Field(example=300.0)
    properties: Dict[str, Any] = Field(
        {}, description="Additional properties", example={"additional_attribute_name": "additional_attribute_value"}
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
                address=dto.physical_object_type_address,
                properties=dto.physical_object_properties,
            ),
            residents_number=dto.residents_number,
            living_area=dto.living_area,
            properties=dto.properties,
        )


class LivingBuildingsDataPost(BaseModel):
    physical_object_id: int = Field(example=1)
    residents_number: int = Field(example=200)
    living_area: float = Field(example=300.0)
    properties: Dict[str, Any] = Field(
        {}, description="Additional properties", example={"additional_attribute_name": "additional_attribute_value"}
    )
