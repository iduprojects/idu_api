"""Social groups and values schemas are defined here."""

from datetime import date, datetime, timezone
from typing import Literal

from pydantic import BaseModel, Field

from idu_api.urban_api.dto import (
    SocGroupDTO,
    SocGroupWithServiceTypesDTO,
    SocValueDTO,
    SocValueIndicatorValueDTO,
    SocValueWithServiceTypesDTO,
)
from idu_api.urban_api.schemas.short_models import ShortServiceType, ShortTerritory, SocValueBasic


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
    service_types: list[ShortServiceType]

    @classmethod
    def from_dto(cls, dto: SocGroupWithServiceTypesDTO) -> "SocGroupWithServiceTypes":
        """Construct from DTO."""
        return cls(
            soc_group_id=dto.soc_group_id,
            name=dto.name,
            service_types=[
                ShortServiceType(
                    id=service_type.service_type_id,
                    name=service_type.name,
                    infrastructure_type=service_type.infrastructure_type,
                )
                for service_type in dto.service_types
            ],
        )


class SocValueWithServiceTypes(BaseModel):
    """Social value with all its attributes and list of service types."""

    soc_value_id: int = Field(..., description="social value identifier", examples=[1])
    name: str = Field(..., description="social value name", examples=["Ценность"])
    rank: int = Field(..., description="rank", examples=[3])
    normative_value: float = Field(..., description="normative value", examples=[0.56])
    decree_value: float = Field(..., description="decree value", examples=[0.75])
    service_types: list[ShortServiceType]

    @classmethod
    def from_dto(cls, dto: SocValueWithServiceTypesDTO) -> "SocValueWithServiceTypes":
        """Construct from DTO."""
        return cls(
            soc_value_id=dto.soc_value_id,
            name=dto.name,
            rank=dto.rank,
            normative_value=dto.normative_value,
            decree_value=dto.decree_value,
            service_types=[
                ShortServiceType(
                    id=service_type.service_type_id,
                    name=service_type.name,
                    infrastructure_type=service_type.infrastructure_type,
                )
                for service_type in dto.service_types
            ],
        )


class SocGroupPost(BaseModel):
    """Schema of social group for POST request."""

    name: str = Field(..., description="social group name", examples=["Трудоспособные"])


class SocServiceTypePost(BaseModel):
    """Schema of social service type for POST request."""

    service_type_id: int = Field(..., description="service type identifier", examples=[1])
    infrastructure_type: Literal["basic", "additional", "comfort"] = Field(
        ..., description="infrastructure type", examples=["basic"]
    )


class SocValue(BaseModel):
    """Social value with all its attributes."""

    soc_value_id: int = Field(..., description="social value identifier", examples=[1])
    name: str = Field(..., description="social value name", examples=["Ценность"])
    rank: int = Field(..., description="rank", examples=[3])
    normative_value: float = Field(..., description="normative value", examples=[0.56])
    decree_value: float = Field(..., description="decree value", examples=[0.75])

    @classmethod
    def from_dto(cls, dto: SocValueDTO) -> "SocValue":
        """Construct from DTO."""
        return cls(
            soc_value_id=dto.soc_value_id,
            name=dto.name,
            rank=dto.rank,
            normative_value=dto.normative_value,
            decree_value=dto.decree_value,
        )


class SocValuePost(BaseModel):
    """Schema of social value for POST request."""

    name: str = Field(..., description="social value name", examples=["Ценность"])
    rank: int = Field(..., description="rank", examples=[3])
    normative_value: float = Field(..., description="normative_value", examples=[0.56])
    decree_value: float = Field(..., description="decree_value", examples=[0.75])


class SocValueIndicatorValue(BaseModel):
    """Social value's indicator value with all its attributes."""

    soc_value: SocValueBasic
    territory: ShortTerritory
    year: int = Field(..., description="year when value was modeled", examples=[date.today().year])
    value: float = Field(..., description="indicator value for territory at time", examples=[23.5])
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc), description="the time when the indicator value was created"
    )
    updated_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="the time when the indicator value was last updated",
    )

    @classmethod
    def from_dto(cls, dto: SocValueIndicatorValueDTO) -> "SocValueIndicatorValue":
        """Construct from DTO."""
        return cls(
            soc_value=SocValueBasic(id=dto.soc_value_id, name=dto.soc_value_name),
            territory=ShortTerritory(id=dto.territory_id, name=dto.territory_name),
            year=dto.year,
            value=dto.value,
            created_at=dto.created_at,
            updated_at=dto.updated_at,
        )


class SocValueIndicatorValuePost(BaseModel):
    """Schema of social value's indicator value for POST request."""

    soc_value_id: int = Field(..., description="social value identifier", examples=[1])
    territory_id: int = Field(..., description="territory identifier", examples=[1])
    year: int = Field(date.today().year, description="year when value was modeled", examples=[date.today().year])
    value: float = Field(..., description="indicator value for social group and territory at time", examples=[23.5])


class SocValueIndicatorValuePut(BaseModel):
    """Schema of social value's indicator value for PATCH request."""

    soc_value_id: int = Field(..., description="social value identifier", examples=[1])
    territory_id: int = Field(..., description="territory identifier", examples=[1])
    year: int = Field(..., description="year when value was modeled", examples=[date.today().year])
    value: float = Field(..., description="indicator value for social group and territory at time", examples=[23.5])
