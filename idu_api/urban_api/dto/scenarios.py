from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ScenarioDTO:
    scenario_id: int
    project_id: int
    target_profile_id: int | None
    target_profile_name: str | None
    target_profile_nickname: str | None
    name: str
    properties: dict[str, Any]
