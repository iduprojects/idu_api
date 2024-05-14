from pydantic import BaseModel, Field

from urban_api.dto import ServiceDTO, ServiceWithGeometryDTO
from urban_api.schemas.geometries import Geometry


class ServicesData(BaseModel):
    service_id: int = Field(example=1)
    service_type_id: int = Field(example=1)
    territory_type_id: int = Field(example=1)
    name: str = Field(description="Service name", example="--")
    list_label: str = Field(description="Indicator marker in lists", example="1.1.1")
    capacity_real: int = Field(example=1)
    properties: dict[str, str] = Field(
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
            service_type_id=dto.service_type_id,
            territory_type_id=dto.territory_type_id,
            name=dto.name,
            list_label=dto.list_label,
            capacity_real=dto.capacity_real,
            properties=dto.properties,
        )


class ServicesDataWithGeometry(BaseModel):
    service_id: int = Field(example=1)
    service_type_id: int = Field(example=1)
    territory_type_id: int = Field(example=1)
    name: str = Field(description="Service name", example="--")
    list_label: str = Field(description="Indicator marker in lists", example="1.1.1")
    capacity_real: int = Field(example=1)
    properties: dict[str, str] = Field(
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
            service_type_id=dto.service_type_id,
            territory_type_id=dto.territory_type_id,
            name=dto.name,
            list_label=dto.list_label,
            capacity_real=dto.capacity_real,
            properties=dto.properties,
            geometry=dto.geometry,
            centre_point=dto.centre_point,
        )
