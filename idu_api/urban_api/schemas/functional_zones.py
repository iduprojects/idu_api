from pydantic import BaseModel, Field

from idu_api.urban_api.dto import FunctionalZoneDataDTO
from idu_api.urban_api.schemas.geometries import Geometry


class FunctionalZoneData(BaseModel):
    functional_zone_id: int = Field(..., examples=[1])
    territory_id: int = Field(..., examples=[1])
    functional_zone_type_id: int = Field(..., examples=[1])
    geometry: Geometry

    @classmethod
    def from_dto(cls, dto: FunctionalZoneDataDTO) -> "FunctionalZoneData":
        return cls(
            functional_zone_id=dto.functional_zone_id,
            territory_id=dto.territory_id,
            functional_zone_type_id=dto.functional_zone_type_id,
            geometry=dto.geometry,
        )
