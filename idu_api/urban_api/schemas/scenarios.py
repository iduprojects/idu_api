"""Scenarios response models are defined here."""

from datetime import datetime, timezone
from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator

from idu_api.urban_api.dto import ScenarioDTO
from idu_api.urban_api.schemas.short_models import FunctionalZoneTypeBasic, ShortProject, ShortScenario, ShortTerritory


class Scenario(BaseModel):
    """Scenario with all its attributes."""

    scenario_id: int = Field(..., description="scenario identifier", examples=[1])
    parent_scenario: ShortScenario | None
    project: ShortProject
    functional_zone_type: FunctionalZoneTypeBasic | None
    name: str = Field(..., description="name of the scenario", examples=["--"])
    is_based: bool = Field(..., description="boolean parameter to determine base scenario")
    phase: Literal["investment", "pre_design", "design", "construction", "operation", "decommission"] | None = Field(
        ..., description="phase of the scenario", examples=["pre-study"]
    )
    phase_percentage: float | None = Field(..., description="percentage of the phase", examples=[100])
    properties: dict[str, Any] = Field(
        default_factory=dict,
        description="scenario additional properties",
        examples=[{"attribute_name": "attribute_value"}],
    )
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc), description="the time when the scenario was created"
    )
    updated_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc), description="the time when the scenario was last updated"
    )

    @classmethod
    def from_dto(cls, dto: ScenarioDTO) -> "Scenario":
        return cls(
            scenario_id=dto.scenario_id,
            parent_scenario=(
                ShortScenario(id=dto.parent_id, name=dto.parent_name) if dto.parent_id is not None else None
            ),
            project=ShortProject(
                project_id=dto.project_id,
                name=dto.project_name,
                user_id=dto.project_user_id,
                region=ShortTerritory(id=dto.territory_id, name=dto.territory_name),
            ),
            functional_zone_type=(
                FunctionalZoneTypeBasic(
                    id=dto.functional_zone_type_id,
                    name=dto.functional_zone_type_name,
                    nickname=dto.functional_zone_type_nickname,
                    description=dto.functional_zone_type_description,
                )
                if dto.functional_zone_type_id is not None
                else None
            ),
            name=dto.name,
            is_based=dto.is_based,
            phase=dto.phase,
            phase_percentage=dto.phase_percentage,
            properties=dto.properties,
            created_at=dto.created_at,
            updated_at=dto.updated_at,
        )


class ScenarioPost(BaseModel):
    """Scenario schema for POST requests."""

    project_id: int = Field(..., description="project identifier for the scenario", examples=[1])
    functional_zone_type_id: int | None = Field(
        ..., description="target profile identifier for the scenario", examples=[1]
    )
    name: str = Field(..., description="name of the scenario", examples=["--"])
    phase: Literal["investment", "pre_design", "design", "construction", "operation", "decommission"] | None = Field(
        None, description="phase of the scenario", examples=["pre-study"]
    )
    phase_percentage: float | None = Field(None, description="percentage of the phase", examples=[100])
    properties: dict[str, Any] = Field(
        default_factory=dict,
        description="scenario additional properties",
        examples=[{"attribute_name": "attribute_value"}],
    )


class ScenarioPut(BaseModel):
    """Scenario schema for PUT requests."""

    functional_zone_type_id: int | None = Field(
        ..., description="target profile identifier for the scenario", examples=[1]
    )
    name: str = Field(..., description="name of the scenario", examples=["--"])
    is_based: bool = Field(..., description="boolean parameter to determine base scenario")
    phase: Literal["investment", "pre_design", "design", "construction", "operation", "decommission"] | None = Field(
        ..., description="phase of the scenario", examples=["pre-study"]
    )
    phase_percentage: float | None = Field(..., description="percentage of the phase", examples=[100])
    properties: dict[str, Any] = Field(
        ...,
        description="scenario additional properties",
        examples=[{"attribute_name": "attribute_value"}],
    )


class ScenarioPatch(BaseModel):
    """Scenario schema for PATCH requests."""

    functional_zone_type_id: int | None = Field(
        None, description="target profile identifier for the scenario", examples=[1]
    )
    name: str | None = Field(None, description="name of the scenario", examples=["--"])
    is_based: bool | None = Field(None, description="boolean parameter to determine base scenario")
    phase: Literal["investment", "pre_design", "design", "construction", "operation", "decommission"] | None = Field(
        None, description="phase of the scenario", examples=["pre-study"]
    )
    phase_percentage: float | None = Field(None, description="percentage of the phase", examples=[100])
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
