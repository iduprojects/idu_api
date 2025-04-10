from pydantic import BaseModel, Field

from idu_api.urban_api.dto import ScenarioUrbanObjectDTO, UrbanObjectDTO
from idu_api.urban_api.schemas.geometries import Geometry
from idu_api.urban_api.schemas.object_geometries import ObjectGeometry, ScenarioObjectGeometry
from idu_api.urban_api.schemas.physical_object_types import PhysicalObjectFunctionBasic
from idu_api.urban_api.schemas.physical_objects import PhysicalObject, PhysicalObjectType, ScenarioPhysicalObject
from idu_api.urban_api.schemas.service_types import ServiceType, UrbanFunctionBasic
from idu_api.urban_api.schemas.services import ScenarioService, Service
from idu_api.urban_api.schemas.short_models import ShortBuilding, ShortTerritory
from idu_api.urban_api.schemas.territories import TerritoryType


class UrbanObject(BaseModel):
    """Urban object with all its attributes."""

    urban_object_id: int = Field(..., description="urban object id", examples=[1])
    physical_object: PhysicalObject
    object_geometry: ObjectGeometry
    service: Service | None

    @classmethod
    def from_dto(cls, dto: UrbanObjectDTO) -> "UrbanObject":
        urban_object = cls(
            urban_object_id=dto.urban_object_id,
            physical_object=PhysicalObject(
                physical_object_id=dto.physical_object_id,
                physical_object_type=PhysicalObjectType(
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
                building=(
                    ShortBuilding(
                        id=dto.building_id,
                        properties=dto.building_properties,
                        floors=dto.floors,
                        building_area_official=dto.building_area_official,
                        building_area_modeled=dto.building_area_modeled,
                        project_type=dto.project_type,
                        floor_type=dto.floor_type,
                        wall_material=dto.wall_material,
                        built_year=dto.built_year,
                        exploitation_start_year=dto.exploitation_start_year,
                    )
                    if dto.building_id is not None
                    else None
                ),
                properties=dto.physical_object_properties,
                created_at=dto.physical_object_created_at,
                updated_at=dto.physical_object_updated_at,
            ),
            object_geometry=ObjectGeometry(
                object_geometry_id=dto.object_geometry_id,
                territory=ShortTerritory(id=dto.territory_id, name=dto.territory_name),
                address=dto.address,
                osm_id=dto.osm_id,
                geometry=Geometry.from_shapely_geometry(dto.geometry),
                centre_point=Geometry.from_shapely_geometry(dto.centre_point),
                created_at=dto.object_geometry_created_at,
                updated_at=dto.object_geometry_updated_at,
            ),
            service=(
                Service(
                    service_id=dto.service_id,
                    service_type=ServiceType(
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
                    capacity=dto.capacity,
                    is_capacity_real=dto.is_capacity_real,
                    properties=dto.service_properties,
                    created_at=dto.service_created_at,
                    updated_at=dto.service_updated_at,
                )
                if dto.service_id is not None
                else None
            ),
        )
        return urban_object


class ScenarioUrbanObject(BaseModel):
    """Scenario urban object with all its attributes."""

    urban_object_id: int = Field(..., description="urban object identifier", examples=[1])
    scenario_id: int = Field(..., description="scenario identifier", examples=[1])
    public_urban_object_id: int | None = Field(
        ..., description="urban object identifier in public schema", examples=[1]
    )
    physical_object: ScenarioPhysicalObject
    object_geometry: ScenarioObjectGeometry
    service: ScenarioService | None

    @classmethod
    def from_dto(cls, dto: ScenarioUrbanObjectDTO) -> "ScenarioUrbanObject":
        return cls(
            urban_object_id=dto.urban_object_id,
            scenario_id=dto.scenario_id,
            public_urban_object_id=dto.public_urban_object_id,
            physical_object=ScenarioPhysicalObject(
                physical_object_id=dto.physical_object_id,
                physical_object_type=PhysicalObjectType(
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
                building=(
                    ShortBuilding(
                        id=dto.building_id,
                        properties=dto.building_properties,
                        floors=dto.floors,
                        building_area_official=dto.building_area_official,
                        building_area_modeled=dto.building_area_modeled,
                        project_type=dto.project_type,
                        floor_type=dto.floor_type,
                        wall_material=dto.wall_material,
                        built_year=dto.built_year,
                        exploitation_start_year=dto.exploitation_start_year,
                    )
                    if dto.building_id is not None
                    else None
                ),
                name=dto.physical_object_name,
                properties=dto.physical_object_properties,
                created_at=dto.physical_object_created_at,
                updated_at=dto.physical_object_updated_at,
                is_scenario_object=dto.is_scenario_physical_object,
            ),
            object_geometry=ScenarioObjectGeometry(
                object_geometry_id=dto.object_geometry_id,
                territory=ShortTerritory(id=dto.territory_id, name=dto.territory_name),
                address=dto.address,
                osm_id=dto.osm_id,
                geometry=Geometry.from_shapely_geometry(dto.geometry),
                centre_point=Geometry.from_shapely_geometry(dto.centre_point),
                created_at=dto.object_geometry_created_at,
                updated_at=dto.object_geometry_updated_at,
                is_scenario_object=dto.is_scenario_geometry,
            ),
            service=(
                ScenarioService(
                    service_id=dto.service_id,
                    service_type=ServiceType(
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
                    capacity=dto.capacity,
                    is_capacity_real=dto.is_capacity_real,
                    properties=dto.service_properties,
                    created_at=dto.service_created_at,
                    updated_at=dto.service_updated_at,
                    is_scenario_object=dto.is_scenario_service,
                )
                if dto.service_id is not None
                else None
            ),
        )


class UrbanObjectPatch(BaseModel):
    """Urban object schema for PATCH requests."""

    physical_object_id: int | None = Field(None, description="physical object identifier", examples=[1])
    object_geometry_id: int | None = Field(None, description="object geometry identifier", examples=[1])
    service_id: int | None = Field(None, description="service identifier", examples=[1])
