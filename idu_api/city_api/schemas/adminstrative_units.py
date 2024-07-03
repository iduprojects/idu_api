from pydantic import BaseModel, Field

from idu_api.city_api.dto.administrative_units import AdministrativeUnitsDTO
from idu_api.urban_api.schemas.geometries import Geometry


class AdministrativeUnitsData(BaseModel):
    """Administrative unit schema"""

    id: int = Field(examples=[1, 4443])
    name: str = Field(example="name1", description="Administrative unit name")
    geometry: Geometry = Field(description="Administrative unit geometry")
    center: Geometry = Field(description="Administrative unit center coordinates")
    type: str = Field(example="type1", description="Administrative unit type")

    @classmethod
    async def from_dto(cls, dto: AdministrativeUnitsDTO):
        """Constructor from DTO"""

        return cls(
            id=dto.id,
            name=dto.name,
            geometry=Geometry.from_shapely_geometry(dto.geometry),
            center=Geometry.from_shapely_geometry(dto.center),
            type=dto.type
        )
