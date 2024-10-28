from pydantic import BaseModel, Field, model_validator

from idu_api.urban_api.dto import ProjectsFunctionalZoneDTO
from idu_api.urban_api.schemas.geometries import Geometry


class ProjectsFunctionalZone(BaseModel):
    """Schema of project's functional zone for GET request."""

    scenario_id: int = Field(description="Scenario id", examples=[1])
    functional_zone_type_id: int = Field(description="Functional zone type id", examples=[1])
    type_name: str = Field(description="Type name")
    zone_nickname: str | None = Field(None, description="Zone nickname")
    description: str | None = Field(None, description="Description")
    name: str = Field(description="Functional zone name", examples=["--"])
    geometry: Geometry

    @classmethod
    def from_dto(cls, dto: ProjectsFunctionalZoneDTO) -> "ProjectsFunctionalZone":
        return cls(
            scenario_id=dto.scenario_id,
            functional_zone_type_id=dto.functional_zone_type_id,
            type_name=dto.type_name,
            zone_nickname=dto.zone_nickname,
            description=dto.description,
            name=dto.name,
            geometry=dto.geometry,
            # geometry=Geometry.from_shapely_geometry(dto.geometry),
        )


class ProjectsFunctionalZonePost(BaseModel):
    """Schema of project's functional zone for POST request."""

    scenario_id: int = Field(description="Scenario id", examples=[1])
    functional_zone_type_id: int = Field(description="Functional zone type id", examples=[1])
    type_name: str = Field(description="Type name")
    zone_nickname: str | None = Field(None, description="Zone nickname")
    description: str | None = Field(None, description="Description")
    name: str = Field(description="Functional zone name", examples=["--"])
    geometry: Geometry = Field(description="Functional zone geometry")


class ProjectsFunctionalZonePut(BaseModel):
    """Schema of project's functional zone for PUT request."""

    scenario_id: int = Field(description="Scenario id", examples=[1])
    functional_zone_type_id: int = Field(description="Functional zone type id", examples=[1])
    type_name: str = Field(description="Type name")
    zone_nickname: str | None = Field(None, description="Zone nickname")
    description: str | None = Field(None, description="Description")
    name: str = Field(description="Functional zone name", examples=["--"])
    geometry: Geometry = Field(description="Functional zone geometry")


class ProjectsFunctionalZonePatch(BaseModel):
    """Schema of project's functional zone for PATCH request."""

    scenario_id: int | None = Field(None, description="Scenario id", examples=[1])
    functional_zone_type_id: int | None = Field(None, description="Functional zone type id", examples=[1])
    type_name: str | None = Field(None, description="Type name")
    zone_nickname: str | None = Field(None, description="Zone nickname")
    description: str | None = Field(None, description="Description")
    name: str | None = Field(None, description="Functional zone name", examples=["--"])
    geometry: Geometry | None = Field(None, description="Functional zone geometry")

    @model_validator(mode="before")
    @classmethod
    def check_empty_request(cls, values):
        """
        Ensure the request body is not empty.
        """
        if not values:
            raise ValueError("request body cannot be empty")
        return values
