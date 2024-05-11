from typing import Dict

from pydantic import BaseModel, Field

from urban_api.dto import PhysicalObjectsDataDTO, PhysicalObjectWithGeometryDTO
from urban_api.schemas.geometries import Geometry


class PhysicalObjectsData(BaseModel):
    physical_object_id: int = Field(example=1)
    physical_object_type_id: int = Field(example=1)
    name: str = Field(None, description="Physical object name", example="--")
    address: str = Field(None, description="Physical object address", example="--")
    properties: Dict[str, str] = Field(
        {},
        description="Physical object additional properties",
        example={"additional_attribute_name": "additional_attribute_value"},
    )

    @classmethod
    def from_dto(cls, dto: PhysicalObjectsDataDTO) -> "PhysicalObjectsData":
        """
        Construct from DTO.
        """
        return cls(
            physical_object_id=dto.physical_object_id,
            physical_object_type_id=dto.physical_object_type_id,
            name=dto.name,
            address=dto.address,
            properties=dto.properties,
        )


class PhysicalObjectWithGeometry(BaseModel):
    physical_object_id: int = Field(example=1)
    physical_object_type_id: int = Field(example=1)
    name: str = Field(None, description="Physical object name", example="--")
    address: str = Field(None, description="Physical object address", example="--")
    properties: Dict[str, str] = Field(
        {},
        description="Physical object additional properties",
        example={"additional_attribute_name": "additional_attribute_value"},
    )
    geometry: Geometry = Field(description="Object geometry")
    centre_point: Geometry = Field(description="Centre coordinates")

    @classmethod
    def from_dto(cls, dto: PhysicalObjectWithGeometryDTO) -> "PhysicalObjectWithGeometry":
        """
        Construct from DTO.
        """
        return cls(
            physical_object_id=dto.physical_object_id,
            physical_object_type_id=dto.physical_object_type_id,
            name=dto.name,
            address=dto.address,
            properties=dto.properties,
            geometry=dto.geometry,
            centre_point=dto.centre_point,
        )
