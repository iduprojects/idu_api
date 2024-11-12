"""Object geometries schemas are defined here."""

from datetime import datetime

from pydantic import BaseModel, Field, model_validator

from idu_api.urban_api.dto import ObjectGeometryDTO
from idu_api.urban_api.schemas.geometries import Geometry, GeometryValidationModel
from idu_api.urban_api.schemas.short_models import ShortScenarioPhysicalObject, ShortScenarioService, ShortTerritory


class ObjectGeometries(BaseModel):
    """Object geometry with all its attributes."""

    object_geometry_id: int = Field(..., examples=[1])
    territory_id: int = Field(..., examples=[1])
    address: str | None = Field(..., description="physical object address", examples=["--"])
    osm_id: str | None = Field(..., description="open street map identifier", examples=["1"])
    geometry: Geometry
    centre_point: Geometry
    created_at: datetime = Field(default_factory=datetime.utcnow, description="the time when the geometry was created")
    updated_at: datetime = Field(
        default_factory=datetime.utcnow, description="the time when the geometry was last updated"
    )

    @classmethod
    def from_dto(cls, dto: ObjectGeometryDTO) -> "ObjectGeometries":
        """
        Construct from DTO.
        """
        return cls(
            object_geometry_id=dto.object_geometry_id,
            territory_id=dto.territory_id,
            address=dto.address,
            osm_id=dto.osm_id,
            geometry=Geometry.from_shapely_geometry(dto.geometry),
            centre_point=Geometry.from_shapely_geometry(dto.centre_point),
            created_at=dto.created_at,
            updated_at=dto.updated_at,
        )


class ObjectGeometriesPost(GeometryValidationModel):
    """Object geometry schema for POST requests."""

    territory_id: int = Field(..., examples=[1])
    geometry: Geometry
    centre_point: Geometry | None = None
    address: str | None = Field(None, description="physical object address", examples=["--"])
    osm_id: str | None = Field(None, description="open street map identifier", examples=["1"])


class ObjectGeometriesPut(GeometryValidationModel):
    """Object geometry schema for PUT requests."""

    territory_id: int = Field(..., examples=[1])
    geometry: Geometry
    centre_point: Geometry
    address: str | None = Field(..., description="physical object address", examples=["--"])
    osm_id: str | None = Field(..., description="open street map identifier", examples=["1"])


class ObjectGeometriesPatch(GeometryValidationModel):
    """Object geometry schema for PATCH requests."""

    territory_id: int | None = Field(None, examples=[1])
    geometry: Geometry | None = None
    centre_point: Geometry | None = None
    address: str | None = Field(None, description="physical object address", examples=["--"])
    osm_id: str | None = Field(None, description="open street map identifier", examples=["1"])

    @model_validator(mode="before")
    @classmethod
    def check_empty_request(cls, values):
        """
        Ensure the request body is not empty.
        """
        if not values:
            raise ValueError("request body cannot be empty")
        return values


class ScenarioGeometry(BaseModel):
    """Scenario object geometry schema (but without geometry columns)."""

    object_geometry_id: int = Field(..., examples=[1])
    territory: ShortTerritory
    address: str | None = Field(..., description="physical object address", examples=["--"])
    osm_id: str | None = Field(..., description="open street map identifier", examples=["1"])
    is_scenario_object: bool = Field(..., description="boolean parameter to determine scenario object")


class ScenarioAllObjects(BaseModel):
    """Scenario object geometry with all its physical objects and services (but without geometry columns...)."""

    object_geometry_id: int = Field(..., examples=[1])
    territory_id: int = Field(..., examples=[1])
    address: str | None = Field(..., description="physical object address", examples=["--"])
    osm_id: str | None = Field(..., description="open street map identifier", examples=["1"])
    is_scenario_object: bool = Field(..., description="boolean parameter to determine scenario object")
    physical_objects: list[ShortScenarioPhysicalObject]
    services: list[ShortScenarioService]
