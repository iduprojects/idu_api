"""Indicators and indicators values schemas are defined here."""

from datetime import date, datetime
from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator, model_validator

from idu_api.urban_api.dto import (
    IndicatorDTO,
    IndicatorsGroupDTO,
    IndicatorValueDTO,
    MeasurementUnitDTO,
    ProjectIndicatorValueDTO,
)
from idu_api.urban_api.schemas.short_models import ShortScenario, ShortTerritory


class MeasurementUnit(BaseModel):
    """Measurement unit with all its attributes."""

    measurement_unit_id: int = Field(..., description="measurement unit identifier", examples=[1])
    name: str = Field(..., description="measurement unit name", examples=["Количество человек"])

    @classmethod
    def from_dto(cls, dto: MeasurementUnitDTO) -> "MeasurementUnit":
        """
        Construct from DTO.
        """
        return cls(measurement_unit_id=dto.measurement_unit_id, name=dto.name)


class MeasurementUnitPost(BaseModel):
    """Measurement unit schema for POST requests."""

    name: str = Field(..., description="measurement unit name", examples=["Количество человек"])


class ShortIndicatorInfo(BaseModel):
    """Basic indicator model to encapsulate in other models."""

    indicator_id: int = Field(..., examples=[1])
    name_full: str = Field(
        ...,
        description="indicator unit full name",
        examples=["Общее количество людей, постоянно проживающих на территории"],
    )
    measurement_unit: MeasurementUnit | None
    level: int = Field(..., description="number of indicator functions above in a tree + 1", examples=[1])
    list_label: str = Field(..., description="indicator marker in lists", examples=["1.1.1"])


class IndicatorsGroup(BaseModel):
    """Indicator group with all its attributes."""

    indicators_group_id: int = Field(..., description="indicators group identifier", examples=[1])
    name: str = Field(..., description="full name of indicators group", examples=["--"])
    indicators: list[ShortIndicatorInfo] = Field(default_factory=list, description="list of indicators for the group")

    @classmethod
    def from_dto(cls, dto: IndicatorsGroupDTO) -> "IndicatorsGroup":
        return cls(
            indicators_group_id=dto.indicators_group_id,
            name=dto.name,
            indicators=[
                ShortIndicatorInfo(
                    indicator_id=indicator.indicator_id,
                    name_full=indicator.name_full,
                    measurement_unit=(
                        MeasurementUnit(
                            measurement_unit_id=indicator.measurement_unit_id,
                            name=indicator.measurement_unit_name,
                        )
                        if indicator.measurement_unit_id is not None
                        else None
                    ),
                    level=indicator.level,
                    list_label=indicator.list_label,
                )
                for indicator in dto.indicators
            ],
        )


class IndicatorsGroupPost(BaseModel):
    """Indicators group schema for POST requests."""

    name: str = Field(..., description="full name of indicators group", examples=["--"])
    indicators_ids: list[int] = Field(
        ..., description="list of indicators identifiers for the group", examples=[[1, 2]]
    )


class Indicator(BaseModel):
    """Indicator with all its attributes."""

    indicator_id: int = Field(..., description="indicator identifier", examples=[1])
    name_full: str = Field(
        ...,
        description="indicator unit full name",
        examples=["Общее количество людей, постоянно проживающих на территории"],
    )
    name_short: str = Field(..., description="indicator unit short name", examples=["Численность населения"])
    measurement_unit: MeasurementUnit | None
    level: int = Field(..., description="number of indicator functions above in a tree + 1", examples=[1])
    list_label: str = Field(..., description="indicator marker in lists", examples=["1.1.1"])
    parent_id: int | None = Field(..., description="indicator parent identifier", examples=[1])
    created_at: datetime = Field(default_factory=datetime.utcnow, description="the time when the indicator was created")
    updated_at: datetime = Field(
        default_factory=datetime.utcnow, description="the time when the indicator was last updated"
    )

    @classmethod
    def from_dto(cls, dto: IndicatorDTO) -> "Indicator":
        """
        Construct from DTO.
        """
        return cls(
            indicator_id=dto.indicator_id,
            name_full=dto.name_full,
            name_short=dto.name_short,
            measurement_unit=(
                MeasurementUnit(measurement_unit_id=dto.measurement_unit_id, name=dto.measurement_unit_name)
                if dto.measurement_unit_id is not None
                else None
            ),
            level=dto.level,
            list_label=dto.list_label,
            parent_id=dto.parent_id,
            created_at=dto.created_at,
            updated_at=dto.updated_at,
        )


class IndicatorsPost(BaseModel):
    """Indicators schema for POST requests."""

    name_full: str = Field(
        ...,
        description="indicator unit full name",
        examples=["Общее количество людей, постоянно проживающих на территории"],
    )
    name_short: str = Field(..., description="indicator unit short name", examples=["Численность населения"])
    measurement_unit_id: int | None = Field(..., description="indicator measurement unit identifier", examples=[1])
    parent_id: int | None = Field(..., description="indicator parent identifier", examples=[1])


class IndicatorsPut(BaseModel):
    """Indicators schema for PUT requests."""

    name_full: str = Field(
        ...,
        description="indicator unit full name",
        examples=["Общее количество людей, постоянно проживающих на территории"],
    )
    name_short: str = Field(..., description="indicator unit short name", examples=["Численность населения"])
    measurement_unit_id: int | None = Field(..., description="indicator measurement unit identifier", examples=[1])
    parent_id: int | None = Field(..., description="indicator parent identifier", examples=[1])


class IndicatorsPatch(BaseModel):
    """Indicators schema for PATCH requests."""

    name_full: str | None = Field(
        None,
        description="indicator unit full name",
        examples=["Общее количество людей, постоянно проживающих на территории"],
    )
    name_short: str | None = Field(None, description="indicator unit short name", examples=["Численность населения"])
    measurement_unit_id: int | None = Field(None, description="indicator measurement unit identifier", examples=[1])
    parent_id: int | None = Field(None, description="indicator parent identifier", examples=[1])

    @model_validator(mode="before")
    @classmethod
    def check_empty_request(cls, values):
        """Ensure the request body is not empty."""
        if not values:
            raise ValueError("request body cannot be empty")
        return values


class IndicatorValue(BaseModel):
    """Indicator value with all its attributes."""

    indicator: ShortIndicatorInfo
    territory: ShortTerritory
    date_type: Literal["year", "half_year", "quarter", "month", "day"] = Field(
        ..., description="time interval", examples=["year"]
    )
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
    created_at: datetime = Field(
        default_factory=datetime.utcnow, description="the time when the indicator value was created"
    )
    updated_at: datetime = Field(
        default_factory=datetime.utcnow, description="the time when the indicator value was last updated"
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
    def from_dto(cls, dto: IndicatorValueDTO) -> "IndicatorValue":
        """
        Construct from DTO.
        """
        return cls(
            indicator=ShortIndicatorInfo(
                indicator_id=dto.indicator_id,
                name_full=dto.name_full,
                level=dto.level,
                list_label=dto.list_label,
                measurement_unit=(
                    MeasurementUnit(
                        measurement_unit_id=dto.measurement_unit_id,
                        name=dto.measurement_unit_name,
                    )
                    if dto.measurement_unit_id is not None
                    else None
                ),
            ),
            territory=ShortTerritory(id=dto.territory_id, name=dto.territory_name),
            date_type=dto.date_type,
            date_value=dto.date_value,
            value=dto.value,
            value_type=dto.value_type,
            information_source=dto.information_source,
            created_at=dto.created_at,
            updated_at=dto.updated_at,
        )


class IndicatorValuePost(BaseModel):
    """Indicator value schema for POST request."""

    indicator_id: int = Field(..., description="indicator identifier", examples=[1])
    territory_id: int = Field(..., description="territory identifier", examples=[1])
    date_type: Literal["year", "half_year", "quarter", "month", "day"] = Field(
        ..., description="time interval", examples=["year"]
    )
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


class ProjectIndicatorValue(BaseModel):
    """Project indicator value with all its attributes."""

    indicator: ShortIndicatorInfo
    scenario: ShortScenario
    territory: ShortTerritory | None
    hexagon_id: int | None
    value: float = Field(..., description="indicator value for scenario at time", examples=[23.5])
    comment: str | None = Field(None, description="comment for indicator value", examples=["--"])
    information_source: str | None = Field(
        ...,
        description="information source",
        examples=[
            "https://data.gov.spb.ru/irsi/7832000076-Obuekty-nedvizhimogo-imushestva-i-zemelnye-uchastki/"
            "structure_version/229/"
        ],
    )
    properties: dict[str, Any] = Field(
        default_factory=dict,
        description="scenario additional properties",
        examples=[{"attribute_name": "attribute_value"}],
    )
    created_at: datetime = Field(
        default_factory=datetime.utcnow, description="the time when the indicator value was created"
    )
    updated_at: datetime = Field(
        default_factory=datetime.utcnow, description="the time when the indicator value was last updated"
    )

    @classmethod
    def from_dto(cls, dto: ProjectIndicatorValueDTO) -> "ProjectIndicatorValue":
        """
        Construct from DTO.
        """
        return cls(
            indicator=ShortIndicatorInfo(
                indicator_id=dto.indicator_id,
                name_full=dto.name_full,
                level=dto.level,
                list_label=dto.list_label,
                measurement_unit=(
                    MeasurementUnit(
                        measurement_unit_id=dto.measurement_unit_id,
                        name=dto.measurement_unit_name,
                    )
                    if dto.measurement_unit_id is not None
                    else None
                ),
            ),
            scenario=ShortScenario(id=dto.scenario_id, name=dto.scenario_name),
            territory=(
                ShortTerritory(
                    id=dto.territory_id,
                    name=dto.territory_name,
                )
                if dto.territory_id is not None
                else None
            ),
            hexagon_id=dto.hexagon_id,
            value=dto.value,
            comment=dto.comment,
            information_source=dto.information_source,
            created_at=dto.created_at,
            updated_at=dto.updated_at,
        )


class ProjectIndicatorValuePost(BaseModel):
    """Project indicator value schema for POST requests."""

    indicator_id: int = Field(..., description="indicator identifier", examples=[1])
    scenario_id: int = Field(..., description="scenario identifier for which indicator value was saved", examples=[1])
    territory_id: int | None = Field(
        ..., description="real territory identifier for which indicator value was saved", examples=[1]
    )
    hexagon_id: int | None = Field(
        ..., description="hexagon identifier for which indicator value was saved", examples=[1]
    )
    value: float = Field(..., description="indicator value for territory at time", examples=[23.5])
    comment: str | None = Field(..., description="comment for indicator value", examples=["--"])
    information_source: str | None = Field(
        ...,
        description="information source",
        examples=[
            "https://data.gov.spb.ru/irsi/7832000076-Obuekty-nedvizhimogo-imushestva-i-zemelnye-uchastki/"
            "structure_version/229/"
        ],
    )
    properties: dict[str, Any] = Field(
        default_factory=dict,
        description="scenario additional properties",
        examples=[{"attribute_name": "attribute_value"}],
    )


class ProjectIndicatorValuePut(BaseModel):
    """Project indicator value schema for PUT requests."""

    value: float = Field(..., description="indicator value for territory at time", examples=[23.5])
    comment: str | None = Field(..., description="comment for indicator value", examples=["--"])
    information_source: str | None = Field(
        ...,
        description="information source",
        examples=[
            "https://data.gov.spb.ru/irsi/7832000076-Obuekty-nedvizhimogo-imushestva-i-zemelnye-uchastki/"
            "structure_version/229/"
        ],
    )
    properties: dict[str, Any] = Field(
        ...,
        description="scenario additional properties",
        examples=[{"attribute_name": "attribute_value"}],
    )


class ProjectIndicatorValuePatch(BaseModel):
    """Project indicator value schema for PATCH requests."""

    value: float | None = Field(None, description="indicator value for territory at time", examples=[23.5])
    comment: str | None = Field(None, description="comment for indicator value", examples=["--"])
    information_source: str | None = Field(
        None,
        description="information source",
        examples=[
            "https://data.gov.spb.ru/irsi/7832000076-Obuekty-nedvizhimogo-imushestva-i-zemelnye-uchastki/"
            "structure_version/229/"
        ],
    )
    properties: dict[str, Any] | None = Field(
        None,
        description="scenario additional properties",
        examples=[{"attribute_name": "attribute_value"}],
    )

    @model_validator(mode="before")
    @classmethod
    def check_empty_request(cls, values):
        """Ensure the request body is not empty."""
        if not values:
            raise ValueError("request body cannot be empty")
        return values
