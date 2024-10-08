from pydantic import BaseModel, Field

from idu_api.city_api.dto.services_count import ServiceCountDTO


class ServiceTypesCountsData(BaseModel):
    """Service types counts schema"""

    name: str = Field(example="name1", description="Service type name")
    count: int = Field(example="1", description="Service type count type")

    @classmethod
    async def from_dto(cls, dto: ServiceCountDTO):
        """Constructor from DTO"""

        return cls(name=dto.name, count=dto.count)
