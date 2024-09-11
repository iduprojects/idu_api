from typing import Optional

from pydantic import BaseModel, Field

from idu_api.city_api.dto.territory import CATerritoryDTO, CATerritoryWithoutGeometryDTO
from idu_api.urban_api.schemas.geometries import Geometry


class CATerritoriesData(BaseModel):
    """Administrative unit schema"""

    id: int = Field(examples=[1, 4443])
    name: str = Field(example="name1", description="Administrative unit name")
    population: Optional[int] = Field(example=123, description="territory population")
    geometry: Optional[Geometry] = Field(description="Administrative unit geometry")
    center: Geometry = Field(description="Administrative unit center coordinates")
    type: str = Field(example="type1", description="Administrative unit type")

    @classmethod
    async def from_dto(cls, dto: CATerritoryDTO):
        """Constructor from DTO"""

        return cls(
            id=dto.territory_id,
            name=dto.name,
            population=dto.population,
            geometry=Geometry.from_shapely_geometry(dto.geometry),
            center=Geometry.from_shapely_geometry(dto.centre_point),
            type=dto.territory_type_name
        )


class CATerritoriesWithoutGeometryData(BaseModel):
    """Administrative unit schema"""

    id: int = Field(examples=[1, 4443])
    name: str = Field(example="name1", description="Administrative unit name")
    population: Optional[int] = Field(example=123, description="territory population")
    type: str = Field(example="type1", description="Administrative unit type")

    @classmethod
    async def from_dto(cls, dto: CATerritoryWithoutGeometryDTO):
        """Constructor from DTO"""

        return cls(
            id=dto.territory_id,
            name=dto.name,
            population=dto.population,
            type=dto.territory_type_name
        )
