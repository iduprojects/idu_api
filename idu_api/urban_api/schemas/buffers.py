"""Buffers schemas are defined here."""

from pydantic import BaseModel, Field, model_validator

from idu_api.urban_api.dto import BufferDTO, BufferTypeDTO, DefaultBufferValueDTO, ScenarioBufferDTO
from idu_api.urban_api.schemas.geometries import Geometry, NotPointGeometryValidationModel
from idu_api.urban_api.schemas.short_models import (
    BufferTypeBasic,
    ObjectGeometryBasic,
    PhysicalObjectBasic,
    PhysicalObjectTypeBasic,
    ServiceBasic,
    ServiceTypeBasic,
    ShortTerritory,
    ShortUrbanObject,
)


class BufferType(BaseModel):
    """Buffer type schema with all attributes."""

    buffer_type_id: int = Field(..., description="buffer type identifier", examples=[1])
    name: str = Field(..., description="buffer type name", examples=["--"])

    @classmethod
    def from_dto(cls, dto: BufferTypeDTO) -> "BufferType":
        return cls(buffer_type_id=dto.buffer_type_id, name=dto.name)


class BufferTypePost(BaseModel):
    """Buffer type schema for POST request."""

    name: str = Field(..., description="buffer type name", examples=["--"])


class DefaultBufferValue(BaseModel):
    """Default buffer value schema."""

    buffer_type: BufferTypeBasic
    physical_object_type: PhysicalObjectTypeBasic | None
    service_type: ServiceTypeBasic | None
    buffer_value: float = Field(..., description="default buffer radius in meters", examples=[3000])

    @classmethod
    def from_dto(cls, dto: DefaultBufferValueDTO) -> "DefaultBufferValue":
        return cls(
            buffer_type=BufferTypeBasic(id=dto.buffer_type_id, name=dto.buffer_type_name),
            physical_object_type=(
                PhysicalObjectTypeBasic(
                    id=dto.physical_object_type_id,
                    name=dto.physical_object_type_name,
                )
                if dto.physical_object_type_id is not None
                else None
            ),
            service_type=(
                ServiceTypeBasic(
                    id=dto.service_type_id,
                    name=dto.service_type_name,
                )
                if dto.service_type_id is not None
                else None
            ),
            buffer_value=dto.buffer_value,
        )


class DefaultBufferValuePost(BaseModel):
    """Default buffer value schema for POST request."""

    buffer_type_id: int = Field(..., description="buffer type identifier", examples=[1])
    physical_object_type_id: int | None = Field(None, description="physical object type identifier", examples=[1])
    service_type_id: int | None = Field(None, description="service type identifier", examples=[1])
    buffer_value: float = Field(..., description="default buffer radius in meters", examples=[3000])

    @model_validator(mode="after")
    def validate_service_type_or_urban_function(self):
        if self.service_type_id is None and self.physical_object_type_id is None:
            raise ValueError("service_type_id and physical_object_type_id cannot both be unset")
        if self.service_type_id is not None and self.physical_object_type_id is not None:
            raise ValueError("service_type_id and physical_object_type_id cannot both be set")
        return self


class DefaultBufferValuePut(BaseModel):
    """Default buffer value schema for PUT request."""

    buffer_type_id: int = Field(..., description="buffer type identifier", examples=[1])
    physical_object_type_id: int | None = Field(..., description="physical object type identifier", examples=[1])
    service_type_id: int | None = Field(..., description="service type identifier", examples=[1])
    buffer_value: float = Field(..., description="default buffer radius in meters", examples=[3000])

    @model_validator(mode="after")
    def validate_service_type_or_urban_function(self):
        if self.service_type_id is None and self.physical_object_type_id is None:
            raise ValueError("service_type_id and physical_object_type_id cannot both be unset")
        if self.service_type_id is not None and self.physical_object_type_id is not None:
            raise ValueError("service_type_id and physical_object_type_id cannot both be set")
        return self


class Buffer(BaseModel):
    """Buffer schema with all its attributes."""

    buffer_type: BufferTypeBasic
    urban_object: ShortUrbanObject
    geometry: Geometry
    is_custom: bool = Field(..., description="boolean parameter to determine if buffer is custom or default")

    @classmethod
    def from_dto(cls, dto: BufferDTO) -> "Buffer":
        return cls(
            buffer_type=BufferTypeBasic(id=dto.buffer_type_id, name=dto.buffer_type_name),
            urban_object=ShortUrbanObject(
                id=dto.urban_object_id,
                physical_object=PhysicalObjectBasic(
                    id=dto.physical_object_id,
                    name=dto.physical_object_name,
                    type=PhysicalObjectTypeBasic(
                        id=dto.physical_object_type_id,
                        name=dto.physical_object_type_name,
                    ),
                ),
                object_geometry=ObjectGeometryBasic(
                    id=dto.object_geometry_id,
                    territory=ShortTerritory(
                        id=dto.territory_id,
                        name=dto.territory_name,
                    ),
                ),
                service=(
                    ServiceBasic(
                        id=dto.service_id,
                        name=dto.service_name,
                        type=ServiceTypeBasic(
                            id=dto.service_type_id,
                            name=dto.service_type_name,
                        ),
                    )
                    if dto.service_id is not None
                    else None
                ),
            ),
            geometry=Geometry.from_shapely_geometry(dto.geometry),
            is_custom=dto.is_custom,
        )


class BufferAttributes(BaseModel):
    buffer_type: BufferTypeBasic
    urban_object: ShortUrbanObject
    is_custom: bool = Field(..., description="boolean parameter to determine if buffer is custom or default")


class BufferPut(NotPointGeometryValidationModel):
    """Buffer schema for PUT request."""

    buffer_type_id: int = Field(..., description="buffer type identifier", examples=[1])
    urban_object_id: int = Field(..., description="urban object identifier", examples=[1])
    geometry: Geometry | None


class ScenarioBuffer(BaseModel):
    """Scenario buffer schema with all its attributes."""

    buffer_type: BufferTypeBasic
    urban_object: ShortUrbanObject
    geometry: Geometry
    is_custom: bool = Field(..., description="boolean parameter to determine if buffer is custom or default")
    is_scenario_object: bool = Field(..., description="boolean parameter to determine scenario object")
    is_locked: bool = Field(..., description="boolean parameter to determine locked (to edit) object")

    @classmethod
    def from_dto(cls, dto: ScenarioBufferDTO) -> "ScenarioBuffer":
        return cls(
            buffer_type=BufferTypeBasic(id=dto.buffer_type_id, name=dto.buffer_type_name),
            urban_object=ShortUrbanObject(
                id=dto.urban_object_id,
                physical_object=PhysicalObjectBasic(
                    id=dto.physical_object_id,
                    name=dto.physical_object_name,
                    type=PhysicalObjectTypeBasic(
                        id=dto.physical_object_type_id,
                        name=dto.physical_object_type_name,
                    ),
                ),
                object_geometry=ObjectGeometryBasic(
                    id=dto.object_geometry_id,
                    territory=ShortTerritory(
                        id=dto.territory_id,
                        name=dto.territory_name,
                    ),
                ),
                service=(
                    ServiceBasic(
                        id=dto.service_id,
                        name=dto.service_name,
                        type=ServiceTypeBasic(
                            id=dto.service_type_id,
                            name=dto.service_type_name,
                        ),
                    )
                    if dto.service_id is not None
                    else None
                ),
            ),
            geometry=Geometry.from_shapely_geometry(dto.geometry),
            is_custom=dto.is_custom,
            is_scenario_object=dto.is_scenario_object,
            is_locked=dto.is_locked,
        )


class ScenarioBufferPut(NotPointGeometryValidationModel):
    """Scenario buffer schema for PUT request."""

    buffer_type_id: int = Field(..., description="buffer type identifier", examples=[1])
    physical_object_id: int = Field(..., description="physical object identifier", examples=[1])
    is_scenario_physical_object: bool = Field(..., description="true if it is scenario object", examples=[True])
    object_geometry_id: int = Field(..., description="object geometry identifier", examples=[1])
    is_scenario_geometry: bool = Field(..., description="true if it is scenario geometry", examples=[True])
    service_id: int | None = Field(..., description="service identifier", examples=[1])
    is_scenario_service: bool = Field(..., description="true if it is scenario service", examples=[True])
    geometry: Geometry | None


class ScenarioBufferDelete(BaseModel):
    """Scenario buffer schema for DELETE request."""

    buffer_type_id: int = Field(..., description="buffer type identifier", examples=[1])
    physical_object_id: int = Field(..., description="physical object identifier", examples=[1])
    is_scenario_physical_object: bool = Field(..., description="true if it is scenario object", examples=[True])
    object_geometry_id: int = Field(..., description="object geometry identifier", examples=[1])
    is_scenario_geometry: bool = Field(..., description="true if it is scenario geometry", examples=[True])
    service_id: int | None = Field(..., description="service identifier", examples=[1])
    is_scenario_service: bool = Field(..., description="true if it is scenario service", examples=[True])


class ScenarioBufferAttributes(BaseModel):
    """Scenario buffer schema without geometry."""

    buffer_type: BufferTypeBasic
    urban_object: ShortUrbanObject
    is_scenario_object: bool = Field(..., description="boolean parameter to determine scenario object")
    is_locked: bool = Field(..., description="boolean parameter to determine locked (to edit) object")
