"""Functional zones handlers logic of getting entities from the database is defined here."""

import abc
from typing import Protocol

from idu_api.urban_api.dto import (
    FunctionalZoneDataDTO,
    FunctionalZoneTypeDTO,
    ProfilesReclamationDataDTO,
    ProfilesReclamationDataMatrixDTO,
)
from idu_api.urban_api.schemas import (
    FunctionalZoneDataPatch,
    FunctionalZoneDataPost,
    FunctionalZoneDataPut,
    FunctionalZoneTypePost,
    ProfilesReclamationDataPost,
    ProfilesReclamationDataPut,
)


class FunctionalZonesService(Protocol):
    """Service to manipulate functional zone objects."""

    @abc.abstractmethod
    async def get_functional_zone_types(self) -> list[FunctionalZoneTypeDTO]:
        """Get all functional zone type objects."""

    @abc.abstractmethod
    async def add_functional_zone_type(self, functional_zone_type: FunctionalZoneTypePost) -> FunctionalZoneTypeDTO:
        """Create functional zone type object."""

    @abc.abstractmethod
    async def get_all_sources(self, territory_id: int | None) -> list[int]:
        """Get a list of all profiles reclamation sources."""

    @abc.abstractmethod
    async def get_profiles_reclamation_data_matrix(
        self, labels: list[int], territory_id: int | None
    ) -> ProfilesReclamationDataMatrixDTO:
        """Get a matrix of profiles reclamation data for specific labels and territory."""

    @abc.abstractmethod
    async def add_profiles_reclamation_data(
        self, profiles_reclamation: ProfilesReclamationDataPost
    ) -> ProfilesReclamationDataDTO:
        """Add a new profiles reclamation data."""

    @abc.abstractmethod
    async def put_profiles_reclamation_data(
        self, profiles_reclamation: ProfilesReclamationDataPut
    ) -> ProfilesReclamationDataDTO:
        """Update profiles reclamation data if exists else create new profiles reclamation data."""

    @abc.abstractmethod
    async def delete_profiles_reclamation_data(
        self, source_id: int, target_id: int, territory_id: int | None
    ) -> dict[str, str]:
        """Delete profiles reclamation data by source and target profile identifier and territory identifier."""

    @abc.abstractmethod
    async def add_functional_zone(self, functional_zone: FunctionalZoneDataPost) -> FunctionalZoneDataDTO:
        """Add a functional zone."""

    @abc.abstractmethod
    async def put_functional_zone(
        self, functional_zone_id: int, functional_zone: FunctionalZoneDataPut
    ) -> FunctionalZoneDataDTO:
        """Update functional zone by all its attributes."""

    @abc.abstractmethod
    async def patch_functional_zone(
        self, functional_zone_id: int, functional_zone: FunctionalZoneDataPatch
    ) -> FunctionalZoneDataDTO:
        """Update functional zone by only given attributes."""

    @abc.abstractmethod
    async def delete_functional_zone(self, functional_zone_id: int) -> dict:
        """Delete functional zone by identifier."""
