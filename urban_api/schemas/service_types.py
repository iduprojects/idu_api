"""Service types and urban function models are defined here."""

from typing import Optional

from pydantic import BaseModel, Field

from urban_api.dto import ServiceTypesDTO, ServiceTypesNormativesDTO, UrbanFunctionDTO


class ServiceTypeBasic(BaseModel):
    """Basic service type model to encapsulate in other models."""

    id: int
    name: str


class ServiceTypes(BaseModel):
    service_type_id: int = Field(example=1)
    urban_function_id: int = Field(description="Urban function id, if set", example=1)
    name: str = Field(description="Service type unit name", example="Школа")
    capacity_modeled: Optional[int] = Field(None, description="default capacity", example=1)
    code: str = Field(description="Service type code", example="1")

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
    urban_function_id: int = Field(description="Urban function id, if set", example=1)
    name: str = Field(description="Service type unit name", example="Школа")
    capacity_modeled: Optional[int] = Field(None, description="default capacity", example=1)
    code: str = Field(description="Service type code", example="1")


class ServiceTypesNormativesData(BaseModel):
    normative_id: int = Field(example=1)
    service_type_id: Optional[int] = Field(None, example=1)
    urban_function_id: Optional[int] = Field(None, example=1)
    territory_id: int = Field(example=1)
    is_regulated: bool
    radius_availability_meters: Optional[int] = Field(None, example=1)
    time_availability_minutes: Optional[int] = Field(None, example=1)
    services_per_1000_normative: Optional[float] = Field(None, example=1.0)
    services_capacity_per_1000_normative: Optional[float] = Field(None, example=1.0)

    @classmethod
    def from_dto(cls, dto: ServiceTypesNormativesDTO) -> "ServiceTypesNormativesData":
        """
        Construct from DTO.
        """
        return cls(
            normative_id=dto.normative_id,
            service_type_id=dto.service_type_id,
            urban_function_id=dto.urban_function_id,
            territory_id=dto.territory_id,
            is_regulated=dto.is_regulated,
            radius_availability_meters=dto.radius_availability_meters,
            time_availability_minutes=dto.time_availability_minutes,
            services_per_1000_normative=dto.services_per_1000_normative,
            services_capacity_per_1000_normative=dto.services_capacity_per_1000_normative,
        )


class ServiceTypesNormativesDataPost(BaseModel):
    service_type_id: Optional[int] = Field(None, example=1)
    urban_function_id: Optional[int] = Field(None, example=1)
    territory_id: int = Field(example=1)
    is_regulated: bool
    radius_availability_meters: Optional[int] = Field(None, example=1)
    time_availability_minutes: Optional[int] = Field(None, example=1)
    services_per_1000_normative: Optional[float] = Field(None, example=1.0)
    services_capacity_per_1000_normative: Optional[float] = Field(None, example=1.0)


class UrbanFunctionBasic(BaseModel):
    """Basic urban function model to encapsulate in other models."""

    id: int
    name: str


class UrbanFunction(BaseModel):
    urban_function_id: int = Field(example=1)
    parent_urban_function_id: Optional[int] = Field(None, example=1, description="Urban function parent id, if set")
    name: str = Field(example="Образование", description="Urban function unit name")
    level: int = Field(example=1, description="Number of urban functions above in a tree + 1")
    list_label: str = Field(example="1.1.1", description="Urban function list label")
    code: str = Field(example="1", description="Urban function code")

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
    name: str = Field(example="Образование", description="Measurement unit name")
    parent_id: Optional[int] = Field(None, example=1, description="Urban function parent id, if set")
    level: int = Field(example=1, description="Number of urban functions above in a tree + 1")
    list_label: str = Field(example="1.1.1", description="Urban function unit name")
    code: str = Field(example="1", description="Urban function code")
