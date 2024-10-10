from pydantic import BaseModel, Field

from idu_api.urban_api.dto import ScenarioUrbanObjectDTO
from idu_api.urban_api.schemas.geometries import Geometry
from idu_api.urban_api.schemas.object_geometries import ObjectGeometries
from idu_api.urban_api.schemas.physical_objects import PhysicalObjectsData, PhysicalObjectsTypes
from idu_api.urban_api.schemas.service_types import ServiceTypes
from idu_api.urban_api.schemas.services import ServicesData
from idu_api.urban_api.schemas.territories import TerritoryType


class ScenariosUrbanObject(BaseModel):
    """Schema for scenarios urban object with all its attribute."""

    urban_object_id: int = Field(..., description="Urban object id", examples=[1])
    physical_object: PhysicalObjectsData
    object_geometry: ObjectGeometries
    service: ServicesData | None
    scenario_id: int = Field(description="scenario identifier", examples=[1])

    @classmethod
    def from_dto(cls, dto: ScenarioUrbanObjectDTO) -> "ScenariosUrbanObject":
        if dto.service_id is not None:
            urban_object = cls(
                urban_object_id=dto.urban_object_id,
                physical_object=PhysicalObjectsData(
                    physical_object_id=dto.physical_object_id,
                    physical_object_type=PhysicalObjectsTypes(
                        physical_object_type_id=dto.physical_object_type_id, name=dto.physical_object_type_name
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
                    geometry=Geometry.from_shapely_geometry(dto.geometry),
                    centre_point=Geometry.from_shapely_geometry(dto.centre_point),
                ),
                service=ServicesData(
                    service_id=dto.service_id,
                    service_type=ServiceTypes(
                        service_type_id=dto.service_type_id,
                        urban_function_id=dto.urban_function_id,
                        name=dto.service_type_name,
                        capacity_modeled=dto.service_type_capacity_modeled,
                        code=dto.service_type_code,
                    ),
                    name=dto.service_name,
                    capacity_real=dto.capacity_real,
                    properties=dto.service_properties,
                    created_at=dto.service_created_at,
                    updated_at=dto.service_updated_at,
                ),
                scenario_id=dto.scenario_id,
            )
            if dto.territory_type_id is not None:
                urban_object.service.territory_type = TerritoryType(
                    territory_type_id=dto.territory_type_id, name=dto.territory_type_name
                )
            return urban_object
        return cls(
            urban_object_id=dto.urban_object_id,
            physical_object=PhysicalObjectsData(
                physical_object_id=dto.physical_object_id,
                physical_object_type=PhysicalObjectsTypes(
                    physical_object_type_id=dto.physical_object_type_id, name=dto.physical_object_type_name
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
                geometry=Geometry.from_shapely_geometry(dto.geometry),
                centre_point=Geometry.from_shapely_geometry(dto.centre_point),
            ),
            service=None,
            scenario_id=dto.scenario_id,
        )
