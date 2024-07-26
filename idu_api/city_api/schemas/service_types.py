from pydantic import BaseModel, Field

from idu_api.city_api.dto.services_count import ServiceCountDTO


class ServiceTypesData(BaseModel):
    """Administrative unit schema"""

    id: int = Field(examples=[1, 4443])
    name: str = Field(example="name1", description="Service type name")
    code: str = Field(example="name1", description="Service type code")
    urban_function_id: int = Field(examples=[1, 4])

    @classmethod
    async def from_dto(cls, dto: ServiceCountDTO):
        """Constructor from DTO"""

        return cls(
            id=dto.service_type_id,
            name=dto.name,
            code=dto.code,
            urban_function_id=dto.urban_function_id,
        )
