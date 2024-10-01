"""Functional zones handlers logic of getting entities from the database is defined here."""

import abc
from typing import Protocol

from idu_api.urban_api.dto import FunctionalZoneTypeDTO
from idu_api.urban_api.schemas import FunctionalZoneTypePost


class FunctionalZonesService(Protocol):
    """Service to manipulate functional zone objects."""

    @abc.abstractmethod
    async def get_functional_zone_types(self) -> list[FunctionalZoneTypeDTO]:
        """Get all functional zone type objects."""

    @abc.abstractmethod
    async def add_functional_zone_type(self, functional_zone_type: FunctionalZoneTypePost) -> FunctionalZoneTypeDTO:
        """Create functional zone type object."""
