from fastapi import FastAPI

from idu_api.city_api.services.territories.administrative_units import AdministrativeUnitsService
from idu_api.urban_api.middlewares.dependency_injection import PassServicesDependencies
from idu_api.city_api.common import connection_manager

app = FastAPI(
    title="CityAPI",
    root_path="/api"
)

app.add_middleware(
    PassServicesDependencies,
    connection_manager=connection_manager,
    administrative_units_service=AdministrativeUnitsService
)
