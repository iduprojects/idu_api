"""Profiles reclamations DTO are defined here."""

from dataclasses import dataclass


@dataclass
class ProfilesReclamationDataDTO:
    source_profile_id: int
    target_profile_id: int
    technical_price: float
    technical_time: float
    biological_price: float
    biological_time: float


@dataclass
class ProfilesReclamationDataMatrixDTO:
    labels: list[int]
    technical_price: list[list[float]]
    technical_time: list[list[float]]
    biological_price: list[list[float]]
    biological_time: list[list[float]]
