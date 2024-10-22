from pydantic import BaseModel, Field

from idu_api.urban_api.dto import UrbanObjectDTO
from idu_api.urban_api.schemas.geometries import Geometry
from idu_api.urban_api.schemas.object_geometries import ObjectGeometries
from idu_api.urban_api.schemas.physical_object_types import PhysicalObjectFunctionBasic
from idu_api.urban_api.schemas.physical_objects import PhysicalObjectsData, PhysicalObjectsTypes
from idu_api.urban_api.schemas.service_types import ServiceTypes, UrbanFunctionBasic
from idu_api.urban_api.schemas.services import ServicesData
from idu_api.urban_api.schemas.territories import TerritoryType


class UrbanObject(BaseModel):
    """Schema for urban object with all its attribute."""

    urban_object_id: int = Field(..., description="Urban object id", examples=[1])
    physical_object: PhysicalObjectsData
    object_geometry: ObjectGeometries
    service: ServicesData | None

    @classmethod
    def from_dto(cls, dto: UrbanObjectDTO) -> "UrbanObject":
        urban_object = cls(
            urban_object_id=dto.urban_object_id,
            physical_object=PhysicalObjectsData(
                physical_object_id=dto.physical_object_id,
                physical_object_type=PhysicalObjectsTypes(
                    physical_object_type_id=dto.physical_object_type_id,
                    name=dto.physical_object_type_name,
                    physical_object_function=(
                        PhysicalObjectFunctionBasic(
                            id=dto.physical_object_function_id, name=dto.physical_object_function_name
                        )
                        if dto.physical_object_function_id is not None
                        else None
                    ),
                ),
                name=dto.physical_object_name,
                properties=dto.physical_object_properties,
                created_at=dto.physical_object_created_at,
                updated_at=dto.physical_object_updated_at,
            ),
            object_geometry=ObjectGeometries(
                object_geometry_id=dto.object_geometry_id,
                territory_id=dto.territory_id,
                address=dto.address,
                osm_id=dto.osm_id,
                geometry=Geometry.from_shapely_geometry(dto.geometry),
                centre_point=Geometry.from_shapely_geometry(dto.centre_point),
                created_at=dto.object_geometry_created_at,
                updated_at=dto.object_geometry_updated_at,
            ),
            service=(
                ServicesData(
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
                    name=dto.service_name,
                    capacity_real=dto.capacity_real,
                    properties=dto.service_properties,
                    created_at=dto.service_created_at,
                    updated_at=dto.service_updated_at,
                )
                if dto.service_id is not None
                else None
            ),
        )
        return urban_object
