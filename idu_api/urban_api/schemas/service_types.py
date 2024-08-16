"""Service types and urban function models are defined here."""

from pydantic import BaseModel, Field

from idu_api.urban_api.dto import ServiceTypesDTO, UrbanFunctionDTO


class ServiceTypeBasic(BaseModel):
    """Basic service type model to encapsulate in other models."""

    id: int
    name: str


class ServiceTypes(BaseModel):
    service_type_id: int = Field(..., examples=[1])
    urban_function_id: int = Field(..., description="Urban function id, if set", examples=[[1]])
    name: str = Field(..., description="Service type unit name", examples=["Школа"])
    capacity_modeled: int | None = Field(None, description="default capacity", examples=[1])
    code: str = Field(..., description="Service type code", examples=["1"])

    @classmethod
    def from_dto(cls, dto: ServiceTypesDTO) -> "ServiceTypes":
        """
        Construct from DTO.
        """
        return cls(
            service_type_id=dto.service_type_id,
            name=dto.name,
            urban_function_id=dto.urban_function_id,
            capacity_modeled=dto.capacity_modeled,
            code=dto.code,
        )


class ServiceTypesPost(BaseModel):
    urban_function_id: int = Field(..., description="Urban function id, if set", examples=[1])
    name: str = Field(..., description="Service type unit name", examples=["Школа"])
    capacity_modeled: int | None = Field(None, description="default capacity", examples=[1])
    code: str = Field(..., description="Service type code", examples=["1"])


class UrbanFunctionBasic(BaseModel):
    """Basic urban function model to encapsulate in other models."""

    id: int
    name: str


class UrbanFunction(BaseModel):
    urban_function_id: int = Field(..., examples=[1])
    parent_urban_function_id: int | None = Field(None, description="Urban function parent id, if set", examples=[1])
    name: str = Field(..., description="Urban function unit name", examples=["Образование"])
    level: int = Field(..., description="Number of urban functions above in a tree + [1]", examples=[1])
    list_label: str = Field(..., description="Urban function list label", examples=["1.1.1"])
    code: str = Field(..., description="Urban function code", examples=["1"])

    @classmethod
    def from_dto(cls, dto: UrbanFunctionDTO) -> "UrbanFunction":
        """
        Construct from DTO.
        """
        return cls(
            urban_function_id=dto.urban_function_id,
            parent_urban_function_id=dto.parent_urban_function_id,
            name=dto.name,
            level=dto.level,
            list_label=dto.list_label,
            code=dto.code,
        )


class UrbanFunctionPost(BaseModel):
    name: str = Field(..., description="Urban function unit name", examples=["Образование"])
    parent_id: int | None = Field(None, description="Urban function parent id, if set", examples=[1])
    level: int = Field(..., description="Number of urban functions above in a tree + [1]", examples=[1])
    list_label: str = Field(..., description="Urban function list label", examples=["1.1.1"])
    code: str = Field(..., description="Urban function code", examples=["1"])
