from typing import Any, Dict

from pydantic import BaseModel, Field

from urban_api.dto import ServiceDTO, ServiceWithGeometryDTO
from urban_api.schemas.geometries import Geometry
from urban_api.schemas.service_types import ServiceTypes
from urban_api.schemas.territories import TerritoryTypes


class ServicesData(BaseModel):
    service_id: int = Field(example=1)
    service_type: ServiceTypes = Field(
        example={"service_type_id": 1, "urban_function_id": 1, "name": "Школа", "capacity_modeled": 1, "code": "1"}
    )
    territory_type: TerritoryTypes = Field(example={"territory_type_id": 1, "name": "Город"})
    name: str = Field(description="Service name", example="--")
    capacity_real: int = Field(example=1)
    properties: Dict[str, Any] = Field(
        description="Service additional properties",
        example={"additional_attribute_name": "additional_attribute_value"},
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
                urban_function_id=dto.urban_function_id,
                name=dto.service_type_name,
                capacity_modeled=dto.service_type_capacity_modeled,
                code=dto.service_type_code,
            ),
            territory_type=TerritoryTypes(territory_type_id=dto.territory_type_id, name=dto.territory_type_name),
            name=dto.name,
            capacity_real=dto.capacity_real,
            properties=dto.properties,
        )


class ServicesDataPost(BaseModel):
    physical_object_id: int = Field(example=1)
    object_geometry_id: int = Field(example=1)
    service_type_id: int = Field(example=1)
    territory_type_id: int = Field(example=1)
    name: str = Field(description="Service name", example="--")
    capacity_real: int = Field(example=1)
    properties: Dict[str, Any] = Field(
        description="Service additional properties",
        example={"additional_attribute_name": "additional_attribute_value"},
    )


class ServicesDataWithGeometry(BaseModel):
    service_id: int = Field(example=1)
    service_type: ServiceTypes = Field(
        example={"service_type_id": 1, "urban_function_id": 1, "name": "Школа", "capacity_modeled": 1, "code": "1"}
    )
    territory_type: TerritoryTypes = Field(example={"territory_type_id": 1, "name": "Город"})
    name: str = Field(description="Service name", example="--")
    capacity_real: int = Field(example=1)
    properties: Dict[str, Any] = Field(
        description="Service additional properties",
        example={"additional_attribute_name": "additional_attribute_value"},
    )
    geometry: Geometry = Field(description="Object geometry")
    centre_point: Geometry = Field(description="Centre coordinates")

    @classmethod
    def from_dto(cls, dto: ServiceWithGeometryDTO) -> "ServicesDataWithGeometry":
        """
        Construct from DTO.
        """
        return cls(
            service_id=dto.service_id,
            service_type=ServiceTypes(
                service_type_id=dto.service_type_id,
                urban_function_id=dto.urban_function_id,
                name=dto.service_type_name,
                capacity_modeled=dto.service_type_capacity_modeled,
                code=dto.service_type_code,
            ),
            territory_type=TerritoryTypes(territory_type_id=dto.territory_type_id, name=dto.territory_type_name),
            name=dto.name,
            capacity_real=dto.capacity_real,
            properties=dto.properties,
            geometry=Geometry.from_shapely_geometry(dto.geometry),
            centre_point=Geometry.from_shapely_geometry(dto.centre_point),
        )
