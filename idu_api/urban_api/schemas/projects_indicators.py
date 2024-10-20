from datetime import date
from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator

from idu_api.urban_api.dto import ProjectsIndicatorDTO


class ProjectsIndicator(BaseModel):
    """Schema of project's indicator for GET request."""

    scenario_id: int = Field(..., description="Scenario id")
    indicator_id: int = Field(..., description="Indicator id")
    date_type: Literal["year", "half_year", "quarter", "month", "day"] = Field(
        ..., description="Time interval", examples=["year"]
    )
    date_value: date = Field(
        ...,
        description="first day of the year for 'year' period, "
        "first of june for 'half_year', first day of jan/apr/jul/oct for quarter, "
        "first day of month for 'month', any valid day value for 'day'",
        examples=["2024-01-01"],
    )
    value: float = Field(..., description="Indicator value")
    value_type: Literal["real", "forecast", "target"] = Field(
        ..., description="Indicator value type", examples=["real"]
    )
    information_source: str = Field(
        ...,
        description="Information source",
        examples=[
            "https://data.gov.spb.ru/irsi/7832000076-Obuekty-nedvizhimogo-imushestva-i-zemelnye-uchastki/"
            "structure_version/229/"
        ],
    )

    @field_validator("date_type", mode="before")
    @staticmethod
    def date_type_to_string(date_type: Any) -> str:
        if isinstance(date_type, Enum):
            return date_type.value
        return date_type

    @field_validator("value_type", mode="before")
    @staticmethod
    def value_type_to_string(value_type: Any) -> str:
        if isinstance(value_type, Enum):
            return value_type.value
        return value_type

    @classmethod
    def from_dto(cls, dto: ProjectsIndicatorDTO) -> "ProjectsIndicator":
        """Construct from DTO"""

        return cls(
            scenario_id=dto.scenario_id,
            indicator_id=dto.indicator_id,
            date_type=dto.date_type,
            date_value=dto.date_value,
            value=dto.value,
            value_type=dto.value_type,
            information_source=dto.information_source,
        )


class ProjectsIndicatorPost(BaseModel):
    """Schema of project's indicator for POST request."""

    scenario_id: int = Field(..., description="Scenario id")
    indicator_id: int = Field(..., description="Indicator id")
    date_type: Literal["year", "half_year", "quarter", "month", "day"] = Field(
        ..., description="Time interval", examples=["year"]
    )
    date_value: date = Field(
        ...,
        description="first day of the year for 'year' period, "
        "first of june for 'half_year', first day of jan/apr/jul/oct for quarter, "
        "first day of month for 'month', any valid day value for 'day'",
        examples=["2024-01-01"],
    )
    value: float = Field(..., description="Indicator value")
    value_type: Literal["real", "forecast", "target"] = Field(
        ..., description="Indicator value type", examples=["real"]
    )
    information_source: str = Field(
        ...,
        description="Information source",
        examples=[
            "https://data.gov.spb.ru/irsi/7832000076-Obuekty-nedvizhimogo-imushestva-i-zemelnye-uchastki/"
            "structure_version/229/"
        ],
    )

    @field_validator("date_type", mode="before")
    @staticmethod
    def date_type_to_string(date_type: Any) -> str:
        if isinstance(date_type, Enum):
            return date_type.value
        return date_type
