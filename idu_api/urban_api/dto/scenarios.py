from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass(frozen=True)
class ScenarioDTO:
    scenario_id: int
    parent_id: int | None
    parent_name: str | None
    project_id: int
    functional_zone_type_id: int | None
    functional_zone_type_name: str | None
    name: str
    is_based: bool
    properties: dict[str, Any]
    created_at: datetime
    updated_at: datetime
