"""Service types and urban function models are defined here."""

from pydantic import BaseModel, Field, model_validator

from idu_api.urban_api.dto import ServiceTypesDTO, UrbanFunctionDTO


class ServiceTypeBasic(BaseModel):
    """Basic service type model to encapsulate in other models."""

    id: int
    name: str


class UrbanFunctionBasic(BaseModel):
    """Basic urban function model to encapsulate in other models."""

    id: int
    name: str


class ServiceTypes(BaseModel):
    service_type_id: int = Field(..., examples=[1])
    urban_function: UrbanFunctionBasic
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
            urban_function=UrbanFunctionBasic(
                id=dto.urban_function_id,
                name=dto.urban_function_name,
            ),
            capacity_modeled=dto.capacity_modeled,
            code=dto.code,
        )


class ServiceTypesPost(BaseModel):
    urban_function_id: int = Field(..., description="Urban function id, if set", examples=[1])
    name: str = Field(..., description="Service type unit name", examples=["Школа"])
    capacity_modeled: int | None = Field(None, description="default capacity", examples=[1])
    code: str = Field(..., description="Service type code", examples=["1"])


class ServiceTypesPut(BaseModel):
    urban_function_id: int = Field(..., description="Urban function id, if set", examples=[1])
    name: str = Field(..., description="Service type unit name", examples=["Школа"])
    capacity_modeled: int | None = Field(..., description="default capacity", examples=[1])
    code: str = Field(..., description="Service type code", examples=["1"])


class ServiceTypesPatch(BaseModel):
    urban_function_id: int | None = Field(None, description="Urban function id, if set", examples=[1])
    name: str | None = Field(None, description="Service type unit name", examples=["Школа"])
    capacity_modeled: int | None = Field(None, description="default capacity", examples=[1])
    code: str | None = Field(None, description="Service type code", examples=["1"])

    @model_validator(mode="before")
    @classmethod
    def check_empty_request(cls, values):
        """Ensure the request body is not empty."""

        if not values:
            raise ValueError("request body cannot be empty")
        return values


class UrbanFunction(BaseModel):
    urban_function_id: int = Field(..., examples=[1])
    parent_urban_function: UrbanFunctionBasic | None
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
            parent_urban_function=(
                UrbanFunctionBasic(
                    id=dto.parent_urban_function_id,
                    name=dto.parent_urban_function_name,
                )
                if dto.parent_urban_function_id is not None
                else None
            ),
            name=dto.name,
            level=dto.level,
            list_label=dto.list_label,
            code=dto.code,
        )


class UrbanFunctionPost(BaseModel):
    name: str = Field(..., description="Urban function unit name", examples=["Образование"])
    parent_id: int | None = Field(None, description="Urban function parent id, if set", examples=[1])
    code: str = Field(..., description="Urban function code", examples=["1"])


class UrbanFunctionPut(BaseModel):
    name: str = Field(..., description="Urban function unit name", examples=["Образование"])
    parent_id: int | None = Field(..., description="Urban function parent id, if set", examples=[1])
    code: str = Field(..., description="Urban function code", examples=["1"])


class UrbanFunctionPatch(BaseModel):
    name: str | None = Field(None, description="Urban function unit name", examples=["Образование"])
    parent_id: int | None = Field(None, description="Urban function parent id, if set", examples=[1])
    code: str | None = Field(None, description="Urban function code", examples=["1"])

    @model_validator(mode="before")
    @classmethod
    def check_empty_request(cls, values):
        """Ensure the request body is not empty."""

        if not values:
            raise ValueError("request body cannot be empty")
        return values
