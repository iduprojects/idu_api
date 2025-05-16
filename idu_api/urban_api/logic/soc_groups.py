"""Social groups and values handlers logic of getting entities from the database is defined here."""

import abc
from typing import Literal, Protocol

from idu_api.urban_api.dto import (
    ServiceTypeDTO,
    SocGroupDTO,
    SocGroupWithServiceTypesDTO,
    SocValueDTO,
    SocValueIndicatorValueDTO,
    SocValueWithServiceTypesDTO,
)
from idu_api.urban_api.schemas import (
    SocGroupPost,
    SocServiceTypePost,
    SocValueIndicatorValuePost,
    SocValueIndicatorValuePut,
    SocValuePost,
)


class SocGroupsService(Protocol):
    """Service to manipulate social groups and its values objects."""

    @abc.abstractmethod
    async def get_social_groups(self) -> list[SocGroupDTO]:
        """Get a list of all social groups."""

    @abc.abstractmethod
    async def get_social_group_by_id(self, soc_group_id: int) -> SocGroupWithServiceTypesDTO:
        """Get social group by identifier."""

    @abc.abstractmethod
    async def add_social_group(self, soc_group: SocGroupPost) -> SocGroupWithServiceTypesDTO:
        """Create a new social group."""

    @abc.abstractmethod
    async def add_service_type_to_social_value(
        self, soc_value_id: int, service_type_id: int
    ) -> SocValueWithServiceTypesDTO:
        """Add service type to social value."""

    @abc.abstractmethod
    async def add_service_type_to_social_group(
        self, soc_group_id: int, service_type: SocServiceTypePost
    ) -> SocGroupWithServiceTypesDTO:
        """Add service type to social group."""

    @abc.abstractmethod
    async def delete_social_group(self, soc_group_id: int) -> dict[str, str]:
        """Delete social group by identifier."""

    @abc.abstractmethod
    async def get_social_values(self) -> list[SocValueDTO]:
        """Get a list of all social values."""

    @abc.abstractmethod
    async def get_social_value_by_id(self, soc_value_id: int) -> SocValueWithServiceTypesDTO:
        """Get social value by identifier."""

    @abc.abstractmethod
    async def add_social_value(self, soc_value: SocValuePost) -> SocValueWithServiceTypesDTO:
        """Create a new social value."""

    @abc.abstractmethod
    async def add_value_to_social_group(
        self, soc_group_id: int, service_type_id: int, soc_value_id: int
    ) -> SocValueWithServiceTypesDTO:
        """Add value to social group."""

    @abc.abstractmethod
    async def delete_social_value(self, soc_value_id: int) -> dict[str, str]:
        """Delete social value by identifier."""

    @abc.abstractmethod
    async def get_social_value_indicator_values(
        self,
        soc_value_id: int,
        territory_id: int | None,
        year: int | None,
        last_only: bool,
    ) -> list[SocValueIndicatorValueDTO]:
        """Get social group's indicator values by social group identifier."""

    @abc.abstractmethod
    async def add_social_value_indicator_value(
        self, soc_group_indicator: SocValueIndicatorValuePost
    ) -> SocValueIndicatorValueDTO:
        """Create a new social value indicator value."""

    @abc.abstractmethod
    async def put_social_value_indicator_value(
        self, soc_value_indicator: SocValueIndicatorValuePut
    ) -> SocValueIndicatorValueDTO:
        """Update or create a social value indicator value."""

    @abc.abstractmethod
    async def delete_social_value_indicator_value_from_db(
        self,
        soc_value_id: int | None,
        territory_id: int | None,
        year: int | None,
    ) -> dict[str, str]:
        """Delete social value's indicator value."""

    @abc.abstractmethod
    async def get_service_types_by_social_value_id(
        self, social_value_id: int, ordering: Literal["asc", "desc"] | None
    ) -> list[ServiceTypeDTO]:
        """Get all service types by social value id"""
