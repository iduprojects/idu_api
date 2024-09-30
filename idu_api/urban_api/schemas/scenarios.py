from typing import Any

from pydantic import BaseModel, Field, model_validator

from idu_api.urban_api.dto import ScenarioDTO
from idu_api.urban_api.schemas.profiles import TargetProfilesData


class ScenariosData(BaseModel):
    scenario_id: int = Field(description="scenario identifier", examples=[1])
    project_id: int = Field(description="project identifier for the scenario", examples=[1])
    target_profile: TargetProfilesData | None
    name: str = Field(description="name of the scenario", examples=["--"])
    properties: dict[str, Any] = Field(
        default_factory=dict,
        description="Scenario additional properties",
        examples=[{"attribute_name": "attribute_value"}],
    )

    @classmethod
    def from_dto(cls, dto: ScenarioDTO) -> "ScenariosData":
        if dto.target_profile_id is not None:
            return cls(
                scenario_id=dto.scenario_id,
                project_id=dto.project_id,
                target_profile=TargetProfilesData(
                    target_profile_id=dto.target_profile_id,
                    name=dto.target_profile_name,
                ),
                name=dto.name,
                properties=dto.properties,
            )
        return cls(
            scenario_id=dto.scenario_id,
            project_id=dto.project_id,
            target_profile=None,
            name=dto.name,
            properties=dto.properties,
        )


class ScenariosPost(BaseModel):
    project_id: int = Field(..., description="project identifier for the scenario", examples=[1])
    target_profile_id: int = Field(..., description="target profile identifier for the scenario", examples=[1])
    name: str = Field(..., description="name of the scenario", examples=["--"])
    properties: dict[str, Any] = Field(
        default_factory=dict,
        description="Scenario additional properties",
        examples=[{"attribute_name": "attribute_value"}],
    )


class ScenariosPut(BaseModel):
    target_profile_id: int = Field(..., description="target profile identifier for the scenario", examples=[1])
    name: str = Field(..., description="name of the scenario", examples=["--"])
    properties: dict[str, Any] = Field(
        ...,
        description="Scenario additional properties",
        examples=[{"attribute_name": "attribute_value"}],
    )


class ScenariosPatch(BaseModel):
    target_profile_id: int | None = Field(None, description="target profile identifier for the scenario", examples=[1])
    name: str | None = Field(None, description="name of the scenario", examples=["--"])
    properties: dict[str, Any] | None = Field(
        None,
        description="Scenario additional properties",
        examples=[{"attribute_name": "attribute_value"}],
    )

    @model_validator(mode="before")
    @classmethod
    def check_empty_request(cls, values):
        """Ensure the request body is not empty."""
        if not values:
            raise ValueError("request body cannot be empty")
        return values
