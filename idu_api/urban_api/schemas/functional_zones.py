from pydantic import BaseModel, Field, model_validator

from idu_api.urban_api.dto import FunctionalZoneDataDTO, FunctionalZoneTypeDTO
from idu_api.urban_api.schemas.geometries import Geometry


class FunctionalZoneType(BaseModel):
    functional_zone_type_id: int = Field(..., description="functional zone type identifier", examples=[1])
    name: str = Field(..., description="functional zone type name", examples=["ИЖС"])
    zone_nickname: str | None = Field(..., description="functional zone type nickname", examples=["residential"])
    description: str | None = Field(None, description="description of functional zone type", examples=["--"])

    @classmethod
    def from_dto(cls, dto: FunctionalZoneTypeDTO) -> "FunctionalZoneType":
        return cls(
            functional_zone_type_id=dto.functional_zone_type_id,
            name=dto.name,
            zone_nickname=dto.zone_nickname,
            description=dto.description,
        )


class FunctionalZoneTypePost(BaseModel):
    name: str = Field(..., description="target profile name", examples=["ИЖС"])
    zone_nickname: str | None = Field(..., description="target profile name", examples=["residential"])
    description: str | None = Field(None, description="description of functional zone type", examples=["--"])


class FunctionalZoneData(BaseModel):
    functional_zone_id: int = Field(..., examples=[1])
    territory_id: int = Field(..., examples=[1])
    functional_zone_type_id: int = Field(..., examples=[1])
    geometry: Geometry

    @classmethod
    def from_dto(cls, dto: FunctionalZoneDataDTO) -> "FunctionalZoneData":
        return cls(
            functional_zone_id=dto.functional_zone_id,
            territory_id=dto.territory_id,
            functional_zone_type_id=dto.functional_zone_type_id,
            geometry=Geometry.from_shapely_geometry(dto.geometry),
        )


class FunctionalZoneDataPost(BaseModel):
    functional_zone_type_id: int = Field(..., examples=[1])
    geometry: Geometry = Field(..., description="Functional zone geometry")


class FunctionalZoneDataPut(BaseModel):
    functional_zone_type_id: int = Field(..., examples=[1])
    geometry: Geometry = Field(..., description="Functional zone geometry")


class FunctionalZoneDataPatch(BaseModel):
    functional_zone_type_id: int | None = Field(None, examples=[1])
    geometry: Geometry | None = Field(None, description="Functional zone geometry")

    @model_validator(mode="before")
    @classmethod
    def check_empty_request(cls, values):
        """Ensure the request body is not empty."""
        if not values:
            raise ValueError("request body cannot be empty")
        return values
