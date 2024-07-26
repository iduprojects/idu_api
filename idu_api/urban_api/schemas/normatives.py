"""Normatives schemas are defined here."""

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field, field_validator, model_validator

from idu_api.urban_api.dto import NormativeDTO
from idu_api.urban_api.schemas.enums import NormativeType

from .service_types import ServiceTypeBasic, UrbanFunctionBasic


class Normative(BaseModel):
    """Normative response model for a given territory"""

    service_type: ServiceTypeBasic | None = Field(None, example=ServiceTypeBasic(id=1, name="Школа"))
    urban_function: UrbanFunctionBasic | None = Field(None, example=UrbanFunctionBasic(id=1, name="--"))
    year: int = Field(..., example=2024)
    radius_availability_meters: int | None = Field(None, example=1)
    time_availability_minutes: int | None = Field(None, example=None)
    services_per_1000_normative: int | None = Field(None, example=1)
    services_capacity_per_1000_normative: int | None = Field(None, example=None)
    normative_type: NormativeType = Field(NormativeType.SELF, example=NormativeType.SELF)
    is_regulated: bool = Field(..., example=True)
    source: str | None = Field(
        description="Information source",
        example="https://data.gov.spb.ru/irsi/7832000076-Obuekty-nedvizhimogo-imushestva-i-zemelnye-uchastki/"
        "structure_version/229/",
    )
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    @field_validator("normative_type", mode="before")
    @staticmethod
    def value_type_to_string(normative_type: Any) -> str:
        if isinstance(normative_type, Enum):
            return normative_type.value
        return normative_type

    @model_validator(mode="after")
    def validate_service_type_or_urban_function(self):
        if self.service_type is None and self.urban_function is None:
            raise ValueError("service_type and urban_function cannot both be unset")
        if self.service_type is not None and self.urban_function is not None:
            raise ValueError("service_type and urban_function cannot both be set")
        return self

    @model_validator(mode="after")
    def validate_radius_or_time_availability(self):
        if self.radius_availability_meters is None and self.time_availability_minutes is None:
            raise ValueError("radius_availability_meters and time_availability_minutes cannot both be unset")
        if self.radius_availability_meters is not None and self.time_availability_minutes is not None:
            raise ValueError("radius_availability_meters and time_availability_minutes cannot both be set")
        return self

    @model_validator(mode="after")
    def validate_services_or_capacity_normative(self):
        if self.services_per_1000_normative is None and self.services_capacity_per_1000_normative is None:
            raise ValueError(
                "services_per_1000_normative and services_capacity_per_1000_normative cannot both be unset"
            )
        if self.services_per_1000_normative is not None and self.services_capacity_per_1000_normative is not None:
            raise ValueError("services_per_1000_normative and services_capacity_per_1000_normative cannot both be set")
        return self

    @classmethod
    def from_dto(cls, dto: NormativeDTO) -> "Normative":
        """
        Construct from DTO.
        """
        if dto.urban_function_id is not None:
            return cls(
                urban_function=UrbanFunctionBasic(id=dto.urban_function_id, name=dto.urban_function_name),
                year=dto.year,
                is_regulated=dto.is_regulated,
                radius_availability_meters=dto.radius_availability_meters,
                time_availability_minutes=dto.time_availability_minutes,
                services_per_1000_normative=dto.services_per_1000_normative,
                services_capacity_per_1000_normative=dto.services_capacity_per_1000_normative,
                normative_type=dto.normative_type,
                source=dto.source,
                created_at=dto.created_at,
                updated_at=dto.updated_at,
            )
        return cls(
            service_type=ServiceTypeBasic(id=dto.service_type_id, name=dto.service_type_name),
            year=dto.year,
            is_regulated=dto.is_regulated,
            radius_availability_meters=dto.radius_availability_meters,
            time_availability_minutes=dto.time_availability_minutes,
            services_per_1000_normative=dto.services_per_1000_normative,
            services_capacity_per_1000_normative=dto.services_capacity_per_1000_normative,
            normative_type=dto.normative_type,
            source=dto.source,
            created_at=dto.created_at,
            updated_at=dto.updated_at,
        )


class NormativePost(BaseModel):
    """Normative post/put model.

    Either service_type_id or urban_function_id should be set,
    same with radius_availability_meters and time_availability_minutes and
    services_per_1000_normative andservices_capacity_per_1000_normative.
    """

    service_type_id: int | None = None
    urban_function_id: int | None = None
    year: int = Field(..., example=2024)
    radius_availability_meters: int | None = Field(None, example=1)
    time_availability_minutes: int | None = Field(None, example=None)
    services_per_1000_normative: int | None = Field(None, example=1)
    services_capacity_per_1000_normative: int | None = Field(None, example=None)
    is_regulated: bool = Field(..., example=True)
    source: str | None = Field(
        description="Information source",
        example="https://data.gov.spb.ru/irsi/7832000076-Obuekty-nedvizhimogo-imushestva-i-zemelnye-uchastki/"
        "structure_version/229/",
    )

    @model_validator(mode="after")
    def validate_service_type_or_urban_function(self):
        if self.service_type_id is None and self.urban_function_id is None:
            raise ValueError("service_type and urban_function cannot both be unset")
        if self.service_type_id is not None and self.urban_function_id is not None:
            raise ValueError("service_type and urban_function cannot both be set")
        return self

    @model_validator(mode="after")
    def validate_radius_or_time_availability(self):
        if self.radius_availability_meters is None and self.time_availability_minutes is None:
            raise ValueError("radius_availability_meters and time_availability_minutes cannot both be unset")
        if self.radius_availability_meters is not None and self.time_availability_minutes is not None:
            raise ValueError("radius_availability_meters and time_availability_minutes cannot both be set")
        return self

    @model_validator(mode="after")
    def validate_services_or_capacity_normative(self):
        if self.services_per_1000_normative is None and self.services_capacity_per_1000_normative is None:
            raise ValueError(
                "services_per_1000_normative and services_capacity_per_1000_normative cannot both be unset"
            )
        if self.services_per_1000_normative is not None and self.services_capacity_per_1000_normative is not None:
            raise ValueError("services_per_1000_normative and services_capacity_per_1000_normative cannot both be set")
        return self


class NormativePatch(BaseModel):
    """Normative patch model.
    Either service_type_id or urban_function_id can be set,
    same with radius_availability_meters and time_availability_minutes and
    services_per_1000_normative andservices_capacity_per_1000_normative.
    """

    service_type_id: int | None = None
    urban_function_id: int | None = None
    year: int = Field(..., example=2024)
    radius_availability_meters: int | None = Field(None, example=1)
    time_availability_minutes: int | None = Field(None, example=None)
    services_per_1000_normative: int | None = Field(None, example=1)
    services_capacity_per_1000_normative: int | None = Field(None, example=None)
    is_regulated: bool = Field(..., example=True)
    source: str | None = Field(
        description="Information source",
        example="https://data.gov.spb.ru/irsi/7832000076-Obuekty-nedvizhimogo-imushestva-i-zemelnye-uchastki/"
        "structure_version/229/",
    )

    @model_validator(mode="after")
    def validate_service_type_or_urban_function(self):
        if self.service_type_id is not None and self.urban_function_id is not None:
            raise ValueError("service_type and urban_function cannot both be set")
        if self.service_type_id is None and self.urban_function_id is None:
            raise ValueError("service_type and urban_function cannot both be unset")
        return self

    @model_validator(mode="after")
    def validate_radius_or_time_availability(self):
        if self.radius_availability_meters is not None and self.time_availability_minutes is not None:
            raise ValueError("radius_availability_meters and time_availability_minutes cannot both be set")
        return self

    @model_validator(mode="after")
    def validate_services_or_capacity_normative(self):
        if self.services_per_1000_normative is not None and self.services_capacity_per_1000_normative is not None:
            raise ValueError("services_per_1000_normative and services_capacity_per_1000_normative cannot both be set")
        return self

    @model_validator(mode="before")
    @classmethod
    def check_empty_request(cls, values):
        """Ensure the request body is not empty."""

        if not values:
            raise ValueError("request body cannot be empty")
        return values

    @model_validator(mode="before")
    @classmethod
    def disallow_nulls(cls, values):
        """Ensure the request body hasn't nulls."""

        for k, v in values.items():
            if v is None:
                raise ValueError(f"{k} cannot be null")
        return values


class NormativeDelete(BaseModel):
    """Normative delete model"""

    service_type_id: int | None = None
    urban_function_id: int | None = None
    year: int = Field(..., example=2024)


class ShortNormativeInfo(BaseModel):
    """Normative geojson response model for a given territory"""

    type: str = Field(..., example="Школа")
    year: int = Field(..., example=2024)
    radius_availability_meters: int | None = Field(None, example=1)
    time_availability_minutes: int | None = Field(None, example=None)
    services_per_1000_normative: int | None = Field(None, example=1)
    services_capacity_per_1000_normative: int | None = Field(None, example=None)
    is_regulated: bool = Field(..., example=True)

    @model_validator(mode="after")
    def validate_radius_or_time_availability(self):
        if self.radius_availability_meters is None and self.time_availability_minutes is None:
            raise ValueError("radius_availability_meters and time_availability_minutes cannot both be unset")
        if self.radius_availability_meters is not None and self.time_availability_minutes is not None:
            raise ValueError("radius_availability_meters and time_availability_minutes cannot both be set")
        return self

    @model_validator(mode="after")
    def validate_services_or_capacity_normative(self):
        if self.services_per_1000_normative is None and self.services_capacity_per_1000_normative is None:
            raise ValueError(
                "services_per_1000_normative and services_capacity_per_1000_normative cannot both be unset"
            )
        if self.services_per_1000_normative is not None and self.services_capacity_per_1000_normative is not None:
            raise ValueError("services_per_1000_normative and services_capacity_per_1000_normative cannot both be set")
        return self
