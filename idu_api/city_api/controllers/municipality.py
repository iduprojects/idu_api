from typing import Annotated

from idu_api.city_api import app
from fastapi import Request, Path

from idu_api.city_api.dto.munipalities import MunicipalitiesDTO
from idu_api.city_api.schemas.municipalities import MunicipalitiesData
from idu_api.city_api.services.territories.municipalities import MunicipalitiesService
