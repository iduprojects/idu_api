from dataclasses import dataclass


@dataclass(frozen=True)
class TargetProfileDTO:
    target_profile_id: int
    name: str
