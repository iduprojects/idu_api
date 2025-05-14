"""Scenarios DTOs are defined here."""

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal


@dataclass(frozen=True)
class ScenarioDTO:  # pylint: disable=too-many-instance-attributes
    scenario_id: int
    parent_id: int | None
    parent_name: str | None
    project_id: int
    project_name: str
    project_user_id: str
    territory_id: str
    territory_name: str
    functional_zone_type_id: int | None
    functional_zone_type_name: str | None
    functional_zone_type_nickname: str | None
    functional_zone_type_description: str | None
    name: str
    is_based: bool
    phase: Literal["investment", "pre_design", "design", "construction", "operation", "decommission"]
    phase_percentage: float
    properties: dict[str, Any]
    created_at: datetime
    updated_at: datetime
