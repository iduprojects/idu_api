from fastapi import FastAPI

from idu_api.city_api.services.objects.physical_objects import PhysicalObjectsService
from idu_api.city_api.services.objects.services import ServicesService
from idu_api.city_api.services.territories.blocks import BlocksService
from idu_api.city_api.services.territories.cities import CitiesService
from idu_api.urban_api.middlewares.dependency_injection import PassServicesDependencies
from idu_api.city_api.common import connection_manager

app = FastAPI(
    title="CityAPI",
    root_path="/api"
)

app.add_middleware(
    PassServicesDependencies,
    connection_manager=connection_manager,
    cities_service=CitiesService,
    services_service=ServicesService,
    physical_objects_service=PhysicalObjectsService,
    blocks_service=BlocksService,
)
