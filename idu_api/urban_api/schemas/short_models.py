"""Basic models to encapsulated in other models are defined here."""

from datetime import date
from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator, model_validator

from idu_api.urban_api.dto import (
    ShortPhysicalObjectDTO,
    ShortScenarioPhysicalObjectDTO,
    ShortScenarioServiceDTO,
    ShortServiceDTO,
)


class FunctionalZoneTypeBasic(BaseModel):
    """Basic functional zone type model to encapsulate in other models."""

    id: int
    name: str


class ShortScenario(BaseModel):
    """Scenario with only id and name."""

    id: int
    name: str


class TerritoryTypeBasic(BaseModel):
    """Basic territory type model to encapsulate in other models."""

    id: int
    name: str


class ShortIndicatorValueInfo(BaseModel):
    """Indicator value without territory."""

    name_full: str = Field(
        ...,
        description="indicator unit full name",
        examples=["Общее количество людей, постоянно проживающих на территории"],
    )
    measurement_unit_name: str | None = Field(..., description="measurement unit name", examples=["Количество людей"])
    level: int = Field(..., description="number of indicator functions above in a tree + 1", examples=[1])
    list_label: str = Field(..., description="indicator marker in lists", examples=["1.1.1"])
    date_value: date = Field(
        ...,
        description="first day of the year for 'year' period, first of june for 'half_year',"
        " first day of jan/apr/jul/oct for quarter, first day of month for 'month', any valid day value for 'day'",
        examples=["2024-01-01"],
    )
    value: float = Field(..., description="indicator value for territory at time", examples=[23.5])
    value_type: Literal["real", "forecast", "target"] = Field(
        ..., description="indicator value type", examples=["real"]
    )
    information_source: str = Field(
        ...,
        description="information source",
        examples=[
            "https://data.gov.spb.ru/irsi/7832000076-Obuekty-nedvizhimogo-imushestva-i-zemelnye-uchastki/"
            "structure_version/229/"
        ],
    )

    @field_validator("value_type", mode="before")
    @staticmethod
    def value_type_to_string(value_type: Any) -> str:
        if isinstance(value_type, Enum):
            return value_type.value
        return value_type


class ShortNormativeInfo(BaseModel):
    """Normative geojson response model for a given territory"""

    type: str = Field(..., examples=["Школа"])
    year: int = Field(..., examples=[2024])
    radius_availability_meters: int | None = Field(None, examples=[1])
    time_availability_minutes: int | None = Field(None, examples=None)
    services_per_1000_normative: int | None = Field(None, examples=[1])
    services_capacity_per_1000_normative: int | None = Field(None, examples=None)
    is_regulated: bool = Field(..., examples=[True])

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


class PhysicalObjectTypeBasic(BaseModel):
    """Basic physical object type model to encapsulate in other models."""

    id: int
    name: str


class PhysicalObjectFunctionBasic(BaseModel):
    """Basic physical object function model to encapsulate in other models."""

    id: int
    name: str


class ShortTerritory(BaseModel):
    """Territory with only id and name."""

    id: int
    name: str


class ShortProject(BaseModel):
    """Basic project model to encapsulate in other models."""

    project_id: int = Field(..., description="project identifier", examples=[1])
    user_id: str = Field(..., description="project creator identifier", examples=["admin@test.ru"])
    name: str = Field(..., description="project name", examples=["--"])
    region: ShortTerritory


class ServiceTypeBasic(BaseModel):
    """Basic service type model to encapsulate in other models."""

    id: int
    name: str


class UrbanFunctionBasic(BaseModel):
    """Basic urban function model to encapsulate in other models."""

    id: int
    name: str


class ShortLivingBuilding(BaseModel):
    """Living building without info about physical objects."""

    id: int = Field(..., examples=[1])
    living_area: float | None = Field(..., examples=[300.0])
    properties: dict[str, Any] = Field(
        default_factory=dict,
        description="additional properties",
        examples=[{"additional_attribute_name": "additional_attribute_value"}],
    )


class ShortPhysicalObject(BaseModel):
    """Basic physical object model to encapsulate in other models."""

    physical_object_id: int = Field(..., examples=[1])
    physical_object_type: PhysicalObjectTypeBasic
    name: str | None = Field(None, description="physical object name", examples=["--"])
    properties: dict[str, Any] = Field(
        default_factory=dict,
        description="physical object additional properties",
        examples=[{"additional_attribute_name": "additional_attribute_value"}],
    )
    living_building: ShortLivingBuilding | None

    @classmethod
    def from_dto(cls, dto: ShortPhysicalObjectDTO) -> "ShortPhysicalObject":
        """
        Construct from DTO.
        """
        return cls(
            physical_object_id=dto.physical_object_id,
            physical_object_type=PhysicalObjectTypeBasic(
                id=dto.physical_object_type_id,
                name=dto.physical_object_type_name,
            ),
            name=dto.name,
            properties=dto.properties,
            living_building=(
                ShortLivingBuilding(
                    id=dto.living_building_id,
                    living_area=dto.living_area,
                    properties=dto.living_building_properties,
                )
                if dto.living_building_id is not None
                else None
            ),
        )


class ShortScenarioPhysicalObject(ShortPhysicalObject):
    """Basic scenario physical object model to encapsulate in other models."""

    is_scenario_object: bool = Field(..., description="boolean parameter to determine scenario object")

    @classmethod
    def from_dto(cls, dto: ShortScenarioPhysicalObjectDTO) -> "ShortPhysicalObject":
        """
        Construct from DTO.
        """
        return cls(
            physical_object_id=dto.physical_object_id,
            physical_object_type=PhysicalObjectTypeBasic(
                id=dto.physical_object_type_id,
                name=dto.physical_object_type_name,
            ),
            name=dto.name,
            properties=dto.properties,
            living_building=(
                ShortLivingBuilding(
                    id=dto.living_building_id,
                    living_area=dto.living_area,
                    properties=dto.living_building_properties,
                )
                if dto.living_building_id is not None
                else None
            ),
            is_scenario_object=dto.is_scenario_object,
        )


class ShortService(BaseModel):
    """Basic service model to encapsulate in other models."""

    service_id: int = Field(..., examples=[1])
    service_type: ServiceTypeBasic
    territory_type: TerritoryTypeBasic | None
    name: str | None = Field(None, description="service name", examples=["--"])
    capacity_real: int | None = Field(None, examples=[1])
    properties: dict[str, Any] = Field(
        default_factory=dict,
        description="service additional properties",
        examples=[{"additional_attribute_name": "additional_attribute_value"}],
    )

    @classmethod
    def from_dto(cls, dto: ShortServiceDTO) -> "ShortService":
        """
        Construct from DTO.
        """
        return cls(
            service_id=dto.service_id,
            service_type=ServiceTypeBasic(
                id=dto.service_type_id,
                name=dto.service_type_name,
            ),
            territory_type=(
                TerritoryTypeBasic(id=dto.territory_type_id, name=dto.territory_type_name)
                if dto.territory_type_id is not None
                else None
            ),
            name=dto.name,
            capacity_real=dto.capacity_real,
            properties=dto.properties,
        )


class ShortScenarioService(ShortService):
    """Basic scenario service model to encapsulate in other models."""

    is_scenario_object: bool = Field(..., description="boolean parameter to determine scenario object")

    @classmethod
    def from_dto(cls, dto: ShortScenarioServiceDTO) -> "ShortScenarioService":
        """
        Construct from DTO.
        """
        return cls(
            service_id=dto.service_id,
            service_type=ServiceTypeBasic(
                id=dto.service_type_id,
                name=dto.service_type_name,
            ),
            territory_type=(
                TerritoryTypeBasic(id=dto.territory_type_id, name=dto.territory_type_name)
                if dto.territory_type_id is not None
                else None
            ),
            name=dto.name,
            capacity_real=dto.capacity_real,
            properties=dto.properties,
            is_scenario_object=dto.is_scenario_object,
        )


class ShortProjectIndicatorValue(BaseModel):
    """Project indicator value with all its attributes."""

    indicator_id: int = Field(..., description="indicator identifier", examples=[1])
    name_full: str = Field(
        ...,
        description="indicator unit full name",
        examples=["Общее количество людей, постоянно проживающих на территории"],
    )
    measurement_unit_name: str | None = Field(..., description="measurement unit name", examples=["Количество людей"])
    value: float = Field(..., description="indicator value for scenario at time", examples=[23.5])
    comment: str | None = Field(None, description="comment for indicator value", examples=["--"])
