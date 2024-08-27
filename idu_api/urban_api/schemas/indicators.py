from datetime import date
from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator

from idu_api.urban_api.dto import IndicatorDTO, IndicatorValueDTO, MeasurementUnitDTO


class MeasurementUnit(BaseModel):
    """
    Measurement unit with all its attributes
    """

    measurement_unit_id: int = Field(..., description="Measurement unit id", examples=[1])
    name: str = Field(..., description="Measurement unit name", examples=["Количество человек"])

    @classmethod
    def from_dto(cls, dto: MeasurementUnitDTO) -> "MeasurementUnit":
        """
        Construct from DTO.
        """
        return cls(measurement_unit_id=dto.measurement_unit_id, name=dto.name)


class MeasurementUnitPost(BaseModel):
    """
    Schema of measurement unit for POST request
    """

    name: str = Field(..., description="Measurement unit name", examples=["Количество человек"])


class ShortIndicatorInfo(BaseModel):
    """
    Indicator with only name and measurement unit.
    """

    indicator_id: int = Field(..., examples=[1])
    name_full: str = Field(
        ...,
        description="Indicator unit full name",
        examples=["Общее количество людей, постоянно проживающих на территории"],
    )
    measurement_unit: MeasurementUnit | None


class Indicator(BaseModel):
    """
    Indicator with all its attributes
    """

    indicator_id: int = Field(..., examples=[1])
    name_full: str = Field(
        ...,
        description="Indicator unit full name",
        examples=["Общее количество людей, постоянно проживающих на территории"],
    )
    name_short: str = Field(..., description="Indicator unit short name", examples=["Численность населения"])
    measurement_unit: MeasurementUnit | None
    level: int = Field(..., description="Number of indicator functions above in a tree + 1", examples=[1])
    list_label: str = Field(..., description="Indicator marker in lists", examples=["1.1.1"])
    parent_id: int | None = Field(..., description="Indicator parent id", examples=[1])

    @classmethod
    def from_dto(cls, dto: IndicatorDTO) -> "Indicator":
        """
        Construct from DTO.
        """
        if dto.measurement_unit_id is not None:
            return cls(
                indicator_id=dto.indicator_id,
                name_full=dto.name_full,
                name_short=dto.name_short,
                measurement_unit=MeasurementUnit(
                    measurement_unit_id=dto.measurement_unit_id, name=dto.measurement_unit_name
                ),
                level=dto.level,
                list_label=dto.list_label,
                parent_id=dto.parent_id,
            )
        return cls(
            indicator_id=dto.indicator_id,
            name_full=dto.name_full,
            name_short=dto.name_short,
            measurement_unit=None,
            level=dto.level,
            list_label=dto.list_label,
            parent_id=dto.parent_id,
        )


class IndicatorsPost(BaseModel):
    """
    Indicator with all its attributes
    """

    name_full: str = Field(
        ...,
        description="Indicator unit full name",
        examples=["Общее количество людей, постоянно проживающих на территории"],
    )
    name_short: str = Field(..., description="Indicator unit short name", examples=["Численность населения"])
    measurement_unit_id: int = Field(..., description="Indicator measurement unit id", examples=[1])
    level: int = Field(..., description="Number of indicator functions above in a tree + 1", examples=[1])
    list_label: str = Field(..., description="Indicator marker in lists", examples=["1.1.1"])
    parent_id: int | None = Field(..., description="Indicator parent id", examples=[1])


class ShortIndicatorValueInfo(BaseModel):
    """
    Indicator value with short information
    """

    name_full: str = Field(
        ...,
        description="Indicator unit full name",
        examples=["Общее количество людей, постоянно проживающих на территории"],
    )
    measurement_unit_name: str | None = Field(..., description="Measurement unit name", examples=["Количество людей"])
    date_value: date = Field(
        ...,
        description="first day of the year for 'year' period, first of june for 'half_year',"
        " first day of jan/apr/jul/oct for quarter, first day of month for 'month', any valid day value for 'day'",
        examples=["2024-01-01"],
    )
    value: float = Field(..., description="Indicator value for territory at time", examples=[23.5])
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

    @field_validator("value_type", mode="before")
    @staticmethod
    def value_type_to_string(value_type: Any) -> str:
        if isinstance(value_type, Enum):
            return value_type.value
        return value_type


class IndicatorValue(BaseModel):
    """
    Indicator value with all its attributes
    """

    indicator: ShortIndicatorInfo
    territory_id: int = Field(..., description="Territory id", examples=[1])
    date_type: Literal["year", "half_year", "quarter", "month", "day"] = Field(
        ..., description="Time interval", examples=["year"]
    )
    date_value: date = Field(
        ...,
        description="first day of the year for 'year' period, first of june for 'half_year',"
        " first day of jan/apr/jul/oct for quarter, first day of month for 'month', any valid day value for 'day'",
        examples=["2024-01-01"],
    )
    value: float = Field(..., description="Indicator value for territory at time", examples=[23.5])
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
    def from_dto(cls, dto: IndicatorValueDTO) -> "IndicatorValue":
        """
        Construct from DTO.
        """
        if dto.measurement_unit_id is not None:
            return cls(
                indicator=ShortIndicatorInfo(
                    indicator_id=dto.indicator_id,
                    name_full=dto.name_full,
                    measurement_unit=MeasurementUnit(
                        measurement_unit_id=dto.measurement_unit_id,
                        name=dto.measurement_unit_name,
                    ),
                ),
                territory_id=dto.territory_id,
                date_type=dto.date_type,
                date_value=dto.date_value,
                value=dto.value,
                value_type=dto.value_type,
                information_source=dto.information_source,
            )
        return cls(
            indicator=ShortIndicatorInfo(
                indicator_id=dto.indicator_id,
                name_full=dto.name_full,
                measurement_unit=None,
            ),
            territory_id=dto.territory_id,
            date_type=dto.date_type,
            date_value=dto.date_value,
            value=dto.value,
            value_type=dto.value_type,
            information_source=dto.information_source,
        )


class IndicatorValuePost(BaseModel):
    """
    Indicator value schema for POST request
    """

    indicator_id: int = Field(..., description="Indicator id", examples=[1])
    territory_id: int = Field(..., description="Territory id", examples=[1])
    date_type: Literal["year", "half_year", "quarter", "month", "day"] = Field(
        ..., description="Time interval", examples=["year"]
    )
    date_value: date = Field(
        ...,
        description="first day of the year for 'year' period, first of june for 'half_year',"
        " first day of jan/apr/jul/oct for quarter, first day of month for 'month', any valid day value for 'day'",
        examples=["2024-01-01"],
    )
    value: float = Field(..., description="Indicator value for territory at time", examples=[23.5])
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
