from typing import Dict
from pydantic import BaseModel, Field

from urban_api.schemas.geometries import Geometry
from urban_api.dto import LivingBuildingsWithGeometryDTO


class LivingBuildingsWithGeometry(BaseModel):
    living_building_real_id: int = Field(..., example=1)
    physical_object_id: int = Field(..., example=1)
    residents_number: int = Field(..., example=200)
    living_area: float = Field(..., example=300.0)
    properties: Dict[str, str] = Field({}, description="Additional properties",
                                       example={"additional_attribute_name": "additional_attribute_value"})
    geometry: Geometry = Field(..., description="Object geometry")

    @classmethod
    def from_dto(cls, dto: LivingBuildingsWithGeometryDTO) -> "LivingBuildingsWithGeometry":
        """
        Construct from DTO.
        """
        return cls(
            living_building_real_id=dto.living_building_real_id,
            physical_object_id=dto.physical_object_id,
            residents_number=dto.residents_number,
            living_area=dto.living_area,
            properties=dto.properties,
            geometry=dto.geometry
        )
