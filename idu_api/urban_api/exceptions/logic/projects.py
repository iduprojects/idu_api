"""
Exceptions connected with entities in urban_db are defined here.
"""

from fastapi import status

from idu_api.urban_api.exceptions import IduApiError


class NotAllowedInRegionalScenario(IduApiError):
    """
    Exception to raise when attempting to access entities that can only be retrieved in a project scenario only.
    """

    def __str__(self) -> str:
        return "This method cannot be accessed in a REGIONAL scenario. Pass the identifier of a PROJECT scenario."

    def get_status_code(self) -> int:
        """
        Return '400 Bad Request' status code.
        """
        return status.HTTP_400_BAD_REQUEST


class NotAllowedInProjectScenario(IduApiError):
    """
    Exception to raise when attempting to access entities that can only be retrieved in a regional scenario only.
    """

    def __str__(self) -> str:
        return "This method cannot be accessed in a PROJECT scenario. Pass the identifier of a REGIONAL scenario."

    def get_status_code(self) -> int:
        """
        Return '400 Bad Request' status code.
        """
        return status.HTTP_400_BAD_REQUEST


class NotAllowedInRegionalProject(IduApiError):
    """
    Exception to raise when attempting to access entities that can only be retrieved in a non-regional project only.
    """

    def __str__(self) -> str:
        return "This method cannot be accessed in a REGIONAL project. Pass the identifier of a common PROJECT."

    def get_status_code(self) -> int:
        """
        Return '400 Bad Request' status code.
        """
        return status.HTTP_400_BAD_REQUEST
