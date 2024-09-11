from typing import Optional

from pydantic import BaseModel, Field

from idu_api.city_api.dto.territory_level import TerritoryLevelDTO, TerritoryLevelWithoutGeometryDTO
from idu_api.urban_api.schemas.geometries import Geometry


class TerritoryLevelData(BaseModel):
    """Territory level schema"""

    id: int = Field(examples=[1, 4443])
    name: str = Field(example="name1", description="territory name")
    population: Optional[int] = Field(example=123, description="territory population")
    geometry: Geometry = Field(description="territory geometry")
    center: Geometry = Field(description="territory center coordinates")
    type: str = Field(example="type1", description="territory type")

    @classmethod
    async def from_dto(cls, dto: TerritoryLevelDTO):
        """Constructor from DTO"""

        return cls(
            id=dto.id,
            name=dto.name,
            population=dto.population,
            geometry=Geometry.from_shapely_geometry(dto.geometry),
            center=Geometry.from_shapely_geometry(dto.center),
            type=dto.type
        )


class TerritoryLevelWithoutGeometryData(BaseModel):
    """Territory level without geometry schema"""

    id: int = Field(examples=[1, 4443])
    name: str = Field(example="name1", description="territory name")
    population: Optional[int] = Field(example=123, description="territory population")
    type: str = Field(example="type1", description="territory type")

    @classmethod
    async def from_dto(cls, dto: TerritoryLevelWithoutGeometryDTO):
        """Constructor from DTO"""

        return cls(
            id=dto.id,
            name=dto.name,
            population=dto.population,
            type=dto.type
        )
