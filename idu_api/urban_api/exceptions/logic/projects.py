"""
Exceptions connected with entities in urban_db are defined here.
"""

from fastapi import status

from idu_api.urban_api.exceptions import IduApiError


class NotAllowedInRegionalScenario(IduApiError):
    """
    Exception to raise when attempting to access entities that can only be retrieved in a project scenario only.
    """

    def __init__(self, entity: str):
        """
        Construct from entity (table) name.
        """
        self.entity = entity
        super().__init__()

    def __str__(self) -> str:
        return f"{self.entity} cannot be accessed in a REGIONAL scenario. Pass the identifier of a PROJECT scenario."

    def get_status_code(self) -> int:
        """
        Return '403 Forbidden' status code.
        """
        return status.HTTP_403_FORBIDDEN


class NotAllowedInProjectScenario(IduApiError):
    """
    Exception to raise when attempting to access entities that can only be retrieved in a regional scenario only.
    """

    def __init__(self, entity: str):
        """
        Construct from entity (table) name.
        """
        self.entity = entity
        super().__init__()

    def __str__(self) -> str:
        return f"{self.entity} cannot be accessed in a PROJECT scenario. Pass the identifier of a REGIONAL scenario."

    def get_status_code(self) -> int:
        """
        Return '403 Forbidden' status code.
        """
        return status.HTTP_403_FORBIDDEN
