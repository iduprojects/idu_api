from typing import Optional

from pydantic import BaseModel, Field

from idu_api.city_api.dto.blocks import BlocksDTO
from idu_api.urban_api.schemas.geometries import Geometry


class BlocksData(BaseModel):
    """Blocks schema"""

    id: int = Field(examples=[1, 4443])
    population: Optional[int] = Field(example=100)
    geometry: Geometry = Field(description="Administrative unit geometry")
    center: Geometry = Field(description="Administrative unit center coordinates")

    @classmethod
    async def from_dto(cls, dto: BlocksDTO):
        """Constructor from DTO"""

        return cls(
            id=dto.id,
            population=dto.population,
            geometry=Geometry.from_shapely_geometry(dto.geometry),
            center=Geometry.from_shapely_geometry(dto.center),
        )


class BlocksWithoutGeometryData(BaseModel):
    """Blocks schema"""

    id: int = Field(examples=[1, 4443])
    population: Optional[int] = Field(example=100)

    @classmethod
    async def from_dto(cls, dto: BlocksDTO):
        """Constructor from DTO"""

        return cls(
            id=dto.id,
            population=dto.population,
        )
