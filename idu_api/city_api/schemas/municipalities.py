from pydantic import BaseModel, Field

from idu_api.city_api.dto.munipalities import MunicipalitiesDTO
from idu_api.urban_api.schemas.geometries import Geometry


class MunicipalitiesData(BaseModel):
    """Administrative unit schema"""

    id: int = Field(examples=[1, 4443])
    name: str = Field(example="name1", description="Municipality name")
    geometry: Geometry = Field(description="Municipality geometry")
    center: Geometry = Field(description="Municipality center coordinates")
    type: str = Field(example="type1", description="Municipality type")

    @classmethod
    async def from_dto(cls, dto: MunicipalitiesDTO):
        """Constructor from DTO"""

        return cls(
            id=dto.id,
            name=dto.name,
            geometry=Geometry.from_shapely_geometry(dto.geometry),
            center=Geometry.from_shapely_geometry(dto.center),
            type=dto.type
        )
