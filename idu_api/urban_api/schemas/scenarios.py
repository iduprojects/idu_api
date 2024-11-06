"""Scenarios response models are defined here."""

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field, model_validator

from idu_api.urban_api.dto import ScenarioDTO
from idu_api.urban_api.schemas.functional_zones import FunctionalZoneType
from idu_api.urban_api.schemas.short_models import ShortScenario


class ScenariosData(BaseModel):
    """Scenario with all its attributes."""

    scenario_id: int = Field(..., description="scenario identifier", examples=[1])
    parent_scenario: ShortScenario | None
    project_id: int = Field(..., description="project identifier for the scenario", examples=[1])
    functional_zone_type: FunctionalZoneType | None
    name: str = Field(..., description="name of the scenario", examples=["--"])
    is_based: bool = Field(..., description="boolean parameter to determine base scenario")
    properties: dict[str, Any] = Field(
        default_factory=dict,
        description="scenario additional properties",
        examples=[{"attribute_name": "attribute_value"}],
    )
    created_at: datetime = Field(default_factory=datetime.utcnow, description="the time when the scenario was created")
    updated_at: datetime = Field(
        default_factory=datetime.utcnow, description="the time when the scenario was last updated"
    )

    @classmethod
    def from_dto(cls, dto: ScenarioDTO) -> "ScenariosData":
        return cls(
            scenario_id=dto.scenario_id,
            parent_scenario=(
                ShortScenario(id=dto.parent_id, name=dto.parent_name) if dto.parent_id is not None else None
            ),
            project_id=dto.project_id,
            functional_zone_type=(
                FunctionalZoneType(
                    functional_zone_type_id=dto.functional_zone_type_id,
                    name=dto.functional_zone_type_name,
                )
                if dto.functional_zone_type_id is not None
                else None
            ),
            name=dto.name,
            is_based=dto.is_based,
            properties=dto.properties,
            created_at=dto.created_at,
            updated_at=dto.updated_at,
        )


class ScenariosPost(BaseModel):
    """Scenario schema for POST requests."""

    project_id: int = Field(..., description="project identifier for the scenario", examples=[1])
    parent_id: int | None = Field(..., description="parent scenario identifier")
    functional_zone_type_id: int = Field(..., description="target profile identifier for the scenario", examples=[1])
    name: str = Field(..., description="name of the scenario", examples=["--"])
    is_based: bool = Field(..., description="boolean parameter to determine base scenario")
    properties: dict[str, Any] = Field(
        default_factory=dict,
        description="scenario additional properties",
        examples=[{"attribute_name": "attribute_value"}],
    )


class ScenariosPut(BaseModel):
    """Scenario schema for PUT requests."""

    project_id: int = Field(..., description="project identifier for the scenario", examples=[1])
    parent_id: int | None = Field(..., description="parent scenario identifier")
    functional_zone_type_id: int | None = Field(
        ..., description="target profile identifier for the scenario", examples=[1]
    )
    name: str = Field(..., description="name of the scenario", examples=["--"])
    is_based: bool = Field(..., description="boolean parameter to determine base scenario")
    properties: dict[str, Any] = Field(
        ...,
        description="scenario additional properties",
        examples=[{"attribute_name": "attribute_value"}],
    )


class ScenariosPatch(BaseModel):
    """Scenario schema for PATCH requests."""

    project_id: int | None = Field(None, description="project identifier for the scenario", examples=[1])
    parent_id: int | None = Field(None, description="parent scenario identifier")
    functional_zone_type_id: int | None = Field(
        None, description="target profile identifier for the scenario", examples=[1]
    )
    name: str | None = Field(None, description="name of the scenario", examples=["--"])
    is_based: bool | None = Field(None, description="boolean parameter to determine base scenario")
    properties: dict[str, Any] | None = Field(
        default_factory=None,
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
