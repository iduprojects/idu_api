from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field

from urban_api.dto import IndicatorsDTO, IndicatorValueDTO


class Indicators(BaseModel):
    indicator_id: int = Field(example=1)
    name: str = Field(description="Indicator unit name", example="Численность населения")
    measurement_unit_id: int = Field(description="Indicator measurement unit id", example=1)
    level: int = Field(description="Number of indicator functions above in a tree + 1", example=1)
    list_label: str = Field(description="Indicator marker in lists", example="1.1.1")
    parent_id: int = Field(description="Indicator parent id", example=1)

    @classmethod
    def from_dto(cls, dto: IndicatorsDTO) -> "Indicators":
        """
        Construct from DTO.
        """
        return cls(
            indicator_id=dto.indicator_id,
            name=dto.name,
            measurement_unit_id=dto.measurement_unit_id,
            level=dto.level,
            list_label=dto.list_label,
            parent_id=dto.parent_id,
        )


class IndicatorValue(BaseModel):
    indicator_id: int = Field(description="Indicator id", example=1)
    territory_id: int = Field(description="Territory id", example=1)
    date_type: Literal["year", "half_year", "quarter", "month", "day"] = Field(
        description="Time interval", example="year"
    )
    date_value: datetime = Field(description="Timestamp", example="2024-03-26T16:33:24.974Z")
    value: int = Field(description="Indicator value for territory at time", example=100500)

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
        )
