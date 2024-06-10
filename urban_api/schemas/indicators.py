from datetime import datetime
from enum import Enum
from typing import Any, Literal, Optional

from pydantic import BaseModel, Field, field_validator

from urban_api.dto import IndicatorDTO, IndicatorValueDTO, MeasurementUnitDTO


class MeasurementUnit(BaseModel):
    """
    Measurement unit with all its attributes
    """

    measurement_unit_id: int = Field(description="Measurement unit id", example=1)
    name: str = Field(description="Measurement unit name", example="Количество людей")

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

    name: str = Field(description="Measurement unit name", example="Количество человек")


class Indicator(BaseModel):
    """
    Indicator with all its attributes
    """

    indicator_id: int = Field(example=1)
    name_full: str = Field(
        description="Indicator unit full name", example="Общее количество людей, постоянно проживающих на территории"
    )
    name_short: str = Field(description="Indicator unit short name", example="Численность населения")
    measurement_unit: Optional[MeasurementUnit] = Field(
        description="Indicator measurement unit", example={"measurement_unit_id": 1, "name": "ед"}
    )
    level: int = Field(description="Number of indicator functions above in a tree + 1", example=1)
    list_label: str = Field(description="Indicator marker in lists", example="1.1.1")
    parent_id: Optional[int] = Field(description="Indicator parent id", example=1)

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
        description="Indicator unit full name", example="Общее количество людей, постоянно проживающих на территории"
    )
    name_short: str = Field(description="Indicator unit short name", example="Численность населения")
    measurement_unit_id: int = Field(description="Indicator measurement unit id", example=1)
    level: int = Field(description="Number of indicator functions above in a tree + 1", example=1)
    list_label: str = Field(description="Indicator marker in lists", example="1.1.1")
    parent_id: Optional[int] = Field(description="Indicator parent id", example=1)


class IndicatorValue(BaseModel):
    """
    Indicator value with all its attributes
    """

    indicator_id: int = Field(description="Indicator id", example=1)
    territory_id: int = Field(description="Territory id", example=1)
    date_type: Literal["year", "half_year", "quarter", "month", "day"] = Field(
        description="Time interval", example="year"
    )
    date_value: datetime = Field(
        description="first day of the year for 'year' period, first of june for 'half_year',"
        " first day of jan/apr/jul/oct for quarter, first day of month for 'month', any valid day value for 'day'",
        example="2024-01-01",
    )
    value: float = Field(description="Indicator value for territory at time", example=23.5)
    value_type: Literal["real", "forecast", "target"] = Field(description="Indicator value type", example="real")
    information_source: Optional[str] = Field(
        description="Information source",
        example="https://data.gov.spb.ru/irsi/7832000076-Obuekty-nedvizhimogo-imushestva-i-zemelnye-uchastki/"
        "structure_version/229/",
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
            indicator_id=dto.indicator_id,
            territory_id=dto.territory_id,
            date_type=dto.date_type,
            date_value=dto.date_value,
            value=dto.value,
            value_type=dto.value_type,
            information_source=dto.information_source,
        )
