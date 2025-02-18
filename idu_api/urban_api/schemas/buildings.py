"""Living buildings schemas are defined here."""

from typing import Any

from pydantic import BaseModel, Field, model_validator

from idu_api.urban_api.dto import BuildingDTO, BuildingWithGeometryDTO
from idu_api.urban_api.schemas.geometries import Geometry
from idu_api.urban_api.schemas.short_models import PhysicalObjectTypeBasic, ShortPhysicalObjectWithoutLivingBuilding


class BuildingWithGeometry(BaseModel):
    """Building with all its attributes and geometry."""

    building_id: int = Field(..., examples=[1])
    physical_object: ShortPhysicalObjectWithoutLivingBuilding
    properties: dict[str, Any] = Field(
        default_factory=dict,
        description="additional properties",
        examples=[{"additional_attribute_name": "additional_attribute_value"}],
    )
    floors: int | None = Field(..., examples=[1])
    building_area_official: float | None = Field(..., examples=[1.0])
    building_area_modeled: float | None = Field(..., examples=[1.0])
    project_type: str | None = Field(..., examples=["example"])
    floor_type: str | None = Field(..., examples=["example"])
    wall_material: str | None = Field(..., examples=["example"])
    built_year: int | None = Field(..., examples=[1])
    exploitation_start_year: int | None = Field(..., examples=[1])
    object_geometry_id: int = Field(..., description="object geometry identifier", examples=[1])
    address: str | None = Field(None, description="geometry address", examples=["--"])
    osm_id: str | None = Field(None, description="open street map identifier", examples=["1"])
    geometry: Geometry
    centre_point: Geometry

    @classmethod
    def from_dto(cls, dto: BuildingWithGeometryDTO) -> "BuildingWithGeometry":
        """
        Construct from DTO.
        """
        return cls(
            building_id=dto.building_id,
            physical_object=ShortPhysicalObjectWithoutLivingBuilding(
                physical_object_id=dto.physical_object_id,
                physical_object_type=PhysicalObjectTypeBasic(
                    id=dto.physical_object_type_id,
                    name=dto.physical_object_type_name,
                ),
                name=dto.physical_object_name,
                properties=dto.physical_object_properties,
            ),
            properties=dto.properties,
            floors=dto.floors,
            building_area_official=dto.building_area_official,
            building_area_modeled=dto.building_area_modeled,
            project_type=dto.project_type,
            floor_type=dto.floor_type,
            wall_material=dto.wall_material,
            built_year=dto.built_year,
            exploitation_start_year=dto.exploitation_start_year,
            object_geometry_id=dto.object_geometry_id,
            address=dto.address,
            osm_id=dto.osm_id,
            geometry=Geometry.from_shapely_geometry(dto.geometry),
            centre_point=Geometry.from_shapely_geometry(dto.centre_point),
        )


class Building(BaseModel):
    """Building with all its attributes."""

    building_id: int = Field(..., examples=[1])
    physical_object: ShortPhysicalObjectWithoutLivingBuilding
    properties: dict[str, Any] = Field(
        default_factory=dict,
        description="additional properties",
        examples=[{"additional_attribute_name": "additional_attribute_value"}],
    )
    floors: int | None = Field(..., examples=[1])
    building_area_official: float | None = Field(..., examples=[1.0])
    building_area_modeled: float | None = Field(..., examples=[1.0])
    project_type: str | None = Field(..., examples=["example"])
    floor_type: str | None = Field(..., examples=["example"])
    wall_material: str | None = Field(..., examples=["example"])
    built_year: int | None = Field(..., examples=[1])
    exploitation_start_year: int | None = Field(..., examples=[1])

    @classmethod
    def from_dto(cls, dto: BuildingDTO) -> "Building":
        """
        Construct from DTO.
        """
        return cls(
            building_id=dto.building_id,
            physical_object=ShortPhysicalObjectWithoutLivingBuilding(
                physical_object_id=dto.physical_object_id,
                physical_object_type=PhysicalObjectTypeBasic(
                    id=dto.physical_object_type_id,
                    name=dto.physical_object_type_name,
                ),
                name=dto.physical_object_name,
                properties=dto.physical_object_properties,
            ),
            properties=dto.properties,
            floors=dto.floors,
            building_area_official=dto.building_area_official,
            building_area_modeled=dto.building_area_modeled,
            project_type=dto.project_type,
            floor_type=dto.floor_type,
            wall_material=dto.wall_material,
            built_year=dto.built_year,
            exploitation_start_year=dto.exploitation_start_year,
        )


class BuildingPost(BaseModel):
    """Building schema for POST requests."""

    physical_object_id: int = Field(..., examples=[1])
    properties: dict[str, Any] = Field(
        default_factory=dict,
        description="additional properties",
        examples=[{"additional_attribute_name": "additional_attribute_value"}],
    )
    floors: int | None = Field(None, examples=[1])
    building_area_official: float | None = Field(None, examples=[1.0])
    building_area_modeled: float | None = Field(None, examples=[1.0])
    project_type: str | None = Field(None, examples=["example"])
    floor_type: str | None = Field(None, examples=["example"])
    wall_material: str | None = Field(None, examples=["example"])
    built_year: int | None = Field(None, examples=[1])
    exploitation_start_year: int | None = Field(None, examples=[1])


class BuildingPut(BaseModel):
    """Building schema for PUT requests."""

    physical_object_id: int = Field(..., examples=[1])
    properties: dict[str, Any] = Field(
        ..., description="additional properties", examples=[{"additional_attribute_name": "additional_attribute_value"}]
    )
    floors: int | None = Field(..., examples=[1])
    building_area_official: float | None = Field(..., examples=[1.0])
    building_area_modeled: float | None = Field(..., examples=[1.0])
    project_type: str | None = Field(..., examples=["example"])
    floor_type: str | None = Field(..., examples=["example"])
    wall_material: str | None = Field(..., examples=["example"])
    built_year: int | None = Field(..., examples=[1])
    exploitation_start_year: int | None = Field(..., examples=[1])


class BuildingPatch(BaseModel):
    """Building schema for PATCH requests."""

    physical_object_id: int | None = Field(None, examples=[1])
    properties: dict[str, Any] | None = Field(
        None,
        description="additional properties",
        examples=[{"additional_attribute_name": "additional_attribute_value"}],
    )
    floors: int | None = Field(None, examples=[1])
    building_area_official: float | None = Field(None, examples=[1.0])
    building_area_modeled: float | None = Field(None, examples=[1.0])
    project_type: str | None = Field(None, examples=["example"])
    floor_type: str | None = Field(None, examples=["example"])
    wall_material: str | None = Field(None, examples=["example"])
    built_year: int | None = Field(None, examples=[1])
    exploitation_start_year: int | None = Field(None, examples=[1])

    @model_validator(mode="before")
    @classmethod
    def check_empty_request(cls, values):
        """
        Ensure the request body is not empty.
        """
        if not values:
            raise ValueError("request body cannot be empty")
        return values
