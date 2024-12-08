"""Services schemas are defined here."""

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field, model_validator

from idu_api.urban_api.dto import (
    ScenarioServiceDTO,
    ServiceDTO,
    ServicesCountCapacityDTO,
    ServiceWithGeometryDTO,
    ServiceWithTerritoriesDTO,
)
from idu_api.urban_api.schemas.geometries import Geometry
from idu_api.urban_api.schemas.service_types import ServiceTypes, UrbanFunctionBasic
from idu_api.urban_api.schemas.territories import ShortTerritory, TerritoryType


class ServicesOrderByField(str, Enum):
    CREATED_AT = "created_at"
    UPDATED_AT = "updated_at"


class ServicesData(BaseModel):
    """Service with all its attributes."""

    service_id: int = Field(..., examples=[1])
    service_type: ServiceTypes
    territory_type: TerritoryType | None
    name: str | None = Field(None, description="service name", examples=["--"])
    capacity_real: int | None = Field(None, examples=[1])
    properties: dict[str, Any] = Field(
        default_factory=dict,
        description="service additional properties",
        examples=[{"additional_attribute_name": "additional_attribute_value"}],
    )
    created_at: datetime = Field(default_factory=datetime.utcnow, description="the time when the service was created")
    updated_at: datetime = Field(
        default_factory=datetime.utcnow, description="the time when the service was last updated"
    )

    @classmethod
    def from_dto(cls, dto: ServiceDTO) -> "ServicesData":
        """
        Construct from DTO.
        """
        return cls(
            service_id=dto.service_id,
            service_type=ServiceTypes(
                service_type_id=dto.service_type_id,
                urban_function=UrbanFunctionBasic(id=dto.urban_function_id, name=dto.urban_function_name),
                name=dto.service_type_name,
                capacity_modeled=dto.service_type_capacity_modeled,
                code=dto.service_type_code,
                infrastructure_type=dto.infrastructure_type,
                properties=dto.service_type_properties,
            ),
            territory_type=(
                TerritoryType(territory_type_id=dto.territory_type_id, name=dto.territory_type_name)
                if dto.territory_type_id is not None
                else None
            ),
            name=dto.name,
            capacity_real=dto.capacity_real,
            properties=dto.properties,
            created_at=dto.created_at,
            updated_at=dto.updated_at,
        )


class ServiceWithTerritories(BaseModel):
    """Service with all its attributes and parent territory."""

    service_id: int = Field(..., examples=[1])
    service_type: ServiceTypes
    territory_type: TerritoryType | None
    name: str | None = Field(..., description="service name", examples=["--"])
    capacity_real: int | None = Field(..., examples=[1])
    properties: dict[str, Any] = Field(
        default_factory=dict,
        description="service additional properties",
        examples=[{"additional_attribute_name": "additional_attribute_value"}],
    )
    territories: list[ShortTerritory]
    created_at: datetime = Field(default_factory=datetime.utcnow, description="the time when the service was created")
    updated_at: datetime = Field(
        default_factory=datetime.utcnow, description="the time when the service was last updated"
    )

    @classmethod
    def from_dto(cls, dto: ServiceWithTerritoriesDTO) -> "ServiceWithTerritories":
        """
        Construct from DTO.
        """
        return cls(
            service_id=dto.service_id,
            service_type=ServiceTypes(
                service_type_id=dto.service_type_id,
                urban_function=UrbanFunctionBasic(id=dto.urban_function_id, name=dto.urban_function_name),
                name=dto.service_type_name,
                capacity_modeled=dto.service_type_capacity_modeled,
                code=dto.service_type_code,
                infrastructure_type=dto.infrastructure_type,
                properties=dto.service_type_properties,
            ),
            territory_type=(
                TerritoryType(territory_type_id=dto.territory_type_id, name=dto.territory_type_name)
                if dto.territory_type_id is not None
                else None
            ),
            name=dto.name,
            capacity_real=dto.capacity_real,
            properties=dto.properties,
            territories=[
                ShortTerritory(id=territory["territory_id"], name=territory["name"]) for territory in dto.territories
            ],
            created_at=dto.created_at,
            updated_at=dto.updated_at,
        )


class ServicesDataPost(BaseModel):
    physical_object_id: int = Field(..., examples=[1])
    object_geometry_id: int = Field(..., examples=[1])
    service_type_id: int = Field(..., examples=[1])
    territory_type_id: int | None = Field(None, examples=[1])
    name: str | None = Field(None, description="service name", examples=["--"])
    capacity_real: int | None = Field(None, examples=[1])
    properties: dict[str, Any] = Field(
        default_factory=dict,
        description="service additional properties",
        examples=[{"additional_attribute_name": "additional_attribute_value"}],
    )


class ScenarioServicePost(BaseModel):
    physical_object_id: int = Field(..., examples=[1])
    is_scenario_physical_object: bool = Field(..., description="to determine scenario object")
    object_geometry_id: int = Field(..., examples=[1])
    is_scenario_geometry: bool = Field(..., description="to determine scenario object")
    service_type_id: int = Field(..., examples=[1])
    territory_type_id: int | None = Field(None, examples=[1])
    name: str | None = Field(None, description="service name", examples=["--"])
    capacity_real: int | None = Field(None, examples=[1])
    properties: dict[str, Any] = Field(
        default_factory=dict,
        description="service additional properties",
        examples=[{"additional_attribute_name": "additional_attribute_value"}],
    )


class ServicesDataPut(BaseModel):
    service_type_id: int = Field(..., examples=[1])
    territory_type_id: int | None = Field(..., examples=[1])
    name: str | None = Field(..., description="service name", examples=["--"])
    capacity_real: int | None = Field(..., examples=[1])
    properties: dict[str, Any] = Field(
        ...,
        description="service additional properties",
        examples=[{"additional_attribute_name": "additional_attribute_value"}],
    )


class ServicesDataPatch(BaseModel):
    service_type_id: int | None = Field(None, examples=[1])
    territory_type_id: int | None = Field(None, examples=[1])
    name: str | None = Field(None, description="service name", examples=["--"])
    capacity_real: int | None = Field(None, examples=[1])
    properties: dict[str, Any] | None = Field(
        None,
        description="service additional properties",
        examples=[{"additional_attribute_name": "additional_attribute_value"}],
    )

    @model_validator(mode="before")
    @classmethod
    def check_empty_request(cls, values):
        """
        Ensure the request body is not empty.
        """
        if not values:
            raise ValueError("request body cannot be empty")
        return values


class ServicesDataWithGeometry(BaseModel):
    service_id: int = Field(..., examples=[1])
    service_type: ServiceTypes
    territory_type: TerritoryType | None = None
    name: str | None = Field(..., description="service name", examples=["--"])
    capacity_real: int | None = Field(..., examples=[1])
    properties: dict[str, Any] = Field(
        default_factory=dict,
        description="service additional properties",
        examples=[{"additional_attribute_name": "additional_attribute_value"}],
    )
    object_geometry_id: int = Field(..., description="object geometry identifier", examples=[1])
    address: str | None = Field(None, description="physical object address", examples=["--"])
    osm_id: str | None = Field(None, description="open street map identifier", examples=["1"])
    geometry: Geometry
    centre_point: Geometry
    created_at: datetime = Field(default_factory=datetime.utcnow, description="the time when the service was created")
    updated_at: datetime = Field(
        default_factory=datetime.utcnow, description="the time when the service was last updated"
    )

    @classmethod
    def from_dto(cls, dto: ServiceWithGeometryDTO) -> "ServicesDataWithGeometry":
        """
        Construct from DTO.
        """
        service = cls(
            service_id=dto.service_id,
            service_type=ServiceTypes(
                service_type_id=dto.service_type_id,
                urban_function=UrbanFunctionBasic(id=dto.urban_function_id, name=dto.urban_function_name),
                name=dto.service_type_name,
                capacity_modeled=dto.service_type_capacity_modeled,
                code=dto.service_type_code,
                infrastructure_type=dto.infrastructure_type,
                properties=dto.service_type_properties,
            ),
            territory_type=(
                TerritoryType(territory_type_id=dto.territory_type_id, name=dto.territory_type_name)
                if dto.territory_type_id is not None
                else None
            ),
            name=dto.name,
            capacity_real=dto.capacity_real,
            properties=dto.properties,
            object_geometry_id=dto.object_geometry_id,
            address=dto.address,
            osm_id=dto.osm_id,
            geometry=Geometry.from_shapely_geometry(dto.geometry),
            centre_point=Geometry.from_shapely_geometry(dto.centre_point),
            created_at=dto.created_at,
            updated_at=dto.updated_at,
        )
        return service


class ServicesCountCapacity(BaseModel):
    territory_id: int = Field(..., description="territory identifier", examples=[1])
    count: int = Field(..., description="total count of services that are located in the territory")
    capacity: int = Field(..., description="summary capacity of services that are located in the territory")

    @classmethod
    def from_dto(cls, dto: ServicesCountCapacityDTO) -> "ServicesCountCapacity":
        return cls(territory_id=dto.territory_id, count=dto.count, capacity=dto.capacity)


class ScenarioService(ServicesData):
    """Service with all its attributes."""

    is_scenario_object: bool = Field(..., description="boolean parameter to determine scenario object")

    @classmethod
    def from_dto(cls, dto: ScenarioServiceDTO) -> "ScenarioService":
        """
        Construct from DTO.
        """
        return cls(
            service_id=dto.service_id,
            service_type=ServiceTypes(
                service_type_id=dto.service_type_id,
                urban_function=UrbanFunctionBasic(id=dto.urban_function_id, name=dto.urban_function_name),
                name=dto.service_type_name,
                capacity_modeled=dto.service_type_capacity_modeled,
                code=dto.service_type_code,
                infrastructure_type=dto.infrastructure_type,
                properties=dto.service_type_properties,
            ),
            territory_type=(
                TerritoryType(territory_type_id=dto.territory_type_id, name=dto.territory_type_name)
                if dto.territory_type_id is not None
                else None
            ),
            name=dto.name,
            capacity_real=dto.capacity_real,
            properties=dto.properties,
            created_at=dto.created_at,
            updated_at=dto.updated_at,
            is_scenario_object=dto.is_scenario_object,
        )
