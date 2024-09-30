from pydantic import BaseModel, Field

from idu_api.city_api.dto.territory_hierarchy import TerritoryHierarchyDTO


class TerritoryHierarchyData(BaseModel):
    territory_type_id: int = Field(example=1, description="id of type")
    territory_type_name: str = Field(example="Район", description="name of type")
    level: int = Field(example=3)

    @classmethod
    async def from_dto(cls, dto: TerritoryHierarchyDTO):
        return cls(
            territory_type_id=dto.territory_type_id,
            territory_type_name=dto.territory_type_name,
            level=dto.level,
        )
