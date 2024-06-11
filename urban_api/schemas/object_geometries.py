from typing import Optional

from pydantic import BaseModel, Field

from urban_api.dto import ObjectGeometryDTO
from urban_api.schemas.geometries import Geometry


class ObjectGeometries(BaseModel):
    object_geometry_id: int = Field(example=1)
    territory_id: int = Field(example=1)
    address: Optional[str] = Field(None, description="Physical object address", example="--")
    geometry: Geometry = Field(description="Object geometry")
    centre_point: Geometry = Field(description="Centre coordinates")

    @classmethod
    def from_dto(cls, dto: ObjectGeometryDTO) -> "ObjectGeometries":
        """
        Construct from DTO.
        """
        return cls(
            object_geometry_id=dto.object_geometry_id,
            territory_id=dto.territory_id,
            address=dto.address,
            geometry=Geometry.from_shapely_geometry(dto.geometry),
            centre_point=Geometry.from_shapely_geometry(dto.centre_point),
        )
