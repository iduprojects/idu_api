"""Social groups and values schemas are defined here."""

from datetime import date, datetime, timezone
from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator

from idu_api.urban_api.dto import (
    SocGroupDTO,
    SocGroupIndicatorValueDTO,
    SocGroupWithServiceTypesDTO,
    SocValueDTO,
    SocValueWithSocGroupsDTO,
)
from idu_api.urban_api.schemas.short_models import ServiceTypeForSocGroup, ShortTerritory, SocGroupBasic, SocValueBasic


class SocGroup(BaseModel):
    """Social group with all its attributes."""

    soc_group_id: int = Field(..., description="social group identifier", examples=[1])
    name: str = Field(..., description="social group name", examples=["Трудоспособные"])

    @classmethod
    def from_dto(cls, dto: SocGroupDTO) -> "SocGroup":
        """Construct from DTO."""
        return cls(soc_group_id=dto.soc_group_id, name=dto.name)


class SocGroupWithServiceTypes(BaseModel):
    """Social group with all its attributes and list of service types."""

    soc_group_id: int = Field(..., description="social group identifier", examples=[1])
    name: str = Field(..., description="social group name", examples=["Трудоспособные"])
    service_types: list[ServiceTypeForSocGroup]

    @classmethod
    def from_dto(cls, dto: SocGroupWithServiceTypesDTO) -> "SocGroupWithServiceTypes":
        """Construct from DTO."""
        return cls(
            soc_group_id=dto.soc_group_id,
            name=dto.name,
            service_types=[
                ServiceTypeForSocGroup(
                    id=service_type["id"],
                    name=service_type["name"],
                    infrastructure_type=service_type["infrastructure_type"],
                )
                for service_type in dto.service_types
            ],
        )


class SocGroupPost(BaseModel):
    """Schema of social group for POST request."""

    name: str = Field(..., description="social group name", examples=["Трудоспособные"])


class SocGroupServiceTypePost(BaseModel):
    """Schema of social group's service type for POST request."""

    service_type_id: int = Field(..., description="service type identifier", examples=[1])
    infrastructure_type: Literal["basic", "additional", "comfort"] = Field(
        ..., description="infrastructure type", examples=["basic"]
    )


class SocValue(BaseModel):
    """Social value with all its attributes."""

    soc_value_id: int = Field(..., description="social value identifier", examples=[1])
    name: str = Field(..., description="social value name", examples=["Ценность"])

    @classmethod
    def from_dto(cls, dto: SocValueDTO) -> "SocValue":
        """Construct from DTO."""
        return cls(soc_value_id=dto.soc_value_id, name=dto.name)


class SocValueWithSocGroups(BaseModel):
    """Social value with all its attributes and list of social groups (with service types)."""

    soc_value_id: int = Field(..., description="social value identifier", examples=[1])
    name: str = Field(..., description="social value name", examples=["Ценность"])
    soc_groups: list[SocGroupWithServiceTypes]

    @classmethod
    def from_dto(cls, dto: SocValueWithSocGroupsDTO) -> "SocValueWithSocGroups":
        """Construct from DTO."""
        return cls(
            soc_value_id=dto.soc_value_id,
            name=dto.name,
            soc_groups=[SocGroupWithServiceTypes.from_dto(group) for group in dto.soc_groups],
        )


class SocValuePost(BaseModel):
    """Schema of social value for POST request."""

    name: str = Field(..., description="social value name", examples=["Ценность"])


class SocGroupIndicatorValue(BaseModel):
    """Social group's indicator value with all its attributes."""

    soc_group: SocGroupBasic
    soc_value: SocValueBasic
    territory: ShortTerritory
    year: int = Field(..., description="year when value was modeled", examples=[date.today().year])
    value: float = Field(..., description="indicator value for territory at time", examples=[23.5])
    value_type: Literal["real", "forecast", "target"] = Field(
        ..., description="indicator value type", examples=["real"]
    )
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc), description="the time when the indicator value was created"
    )
    updated_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="the time when the indicator value was last updated",
    )

    @field_validator("value_type", mode="before")
    @staticmethod
    def value_type_to_string(value_type: Any) -> str:
        if isinstance(value_type, Enum):
            return value_type.value
        return value_type

    @classmethod
    def from_dto(cls, dto: SocGroupIndicatorValueDTO) -> "SocGroupIndicatorValue":
        """Construct from DTO."""
        return cls(
            soc_group=SocGroupBasic(id=dto.soc_group_id, name=dto.soc_group_name),
            soc_value=SocValueBasic(id=dto.soc_value_id, name=dto.soc_value_name),
            territory=ShortTerritory(id=dto.territory_id, name=dto.territory_name),
            year=dto.year,
            value=dto.value,
            value_type=dto.value_type,
            created_at=dto.created_at,
            updated_at=dto.updated_at,
        )


class SocGroupIndicatorValuePost(BaseModel):
    """Schema of social group's indicator value for POST request."""

    soc_value_id: int = Field(..., description="social value identifier", examples=[1])
    territory_id: int = Field(..., description="territory identifier", examples=[1])
    year: int = Field(date.today().year, description="year when value was modeled", examples=[date.today().year])
    value: float = Field(..., description="indicator value for social group and territory at time", examples=[23.5])
    value_type: Literal["real", "forecast", "target"] = Field(
        "real", description="indicator value type", examples=["real"]
    )

    @field_validator("value_type", mode="before")
    @staticmethod
    def value_type_to_string(value_type: Any) -> str:
        if isinstance(value_type, Enum):
            return value_type.value
        return value_type


class SocGroupIndicatorValuePut(BaseModel):
    """Schema of social group's indicator value for PATCH request."""

    soc_value_id: int = Field(..., description="social value identifier", examples=[1])
    territory_id: int = Field(..., description="territory identifier", examples=[1])
    year: int = Field(..., description="year when value was modeled", examples=[date.today().year])
    value: float = Field(..., description="indicator value for social group and territory at time", examples=[23.5])
    value_type: Literal["real", "forecast", "target"] = Field(
        ..., description="indicator value type", examples=["real"]
    )

    @field_validator("value_type", mode="before")
    @staticmethod
    def value_type_to_string(value_type: Any) -> str:
        if isinstance(value_type, Enum):
            return value_type.value
        return value_type
