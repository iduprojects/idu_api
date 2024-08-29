"""
Exceptions connected with geometry are defined here.
"""

from fastapi import status

from idu_api.urban_api.exceptions import IduApiError


class EntityNotFoundById(IduApiError):
    """
    Exception to raise when requested entity was not found in the database by the identifier.
    """

    def __init__(self, requested_id: int, entity: str):
        """
        Construct from requested identifier and entity (table) name.
        """
        self.requested_id = requested_id
        self.entity = entity
        super().__init__()

    def __str__(self) -> str:
        return f"Entity '{self.entity}' with id={self.requested_id} is not found"

    def get_status_code(self) -> int:
        """
        Return '404 Not found' status code.
        """
        return status.HTTP_404_NOT_FOUND


class EntitiesNotFoundByIds(IduApiError):
    """
    Exception to raise when requested entity was not found in the database by the list of identifiers.
    """

    def __init__(self, entity: str):
        """
        Construct from requested identifier and entity (table) name.
        """
        self.entity = entity
        super().__init__()

    def __str__(self) -> str:
        return f"At least one '{self.entity}' of given ids is not found"

    def get_status_code(self) -> int:
        """
        Return '404 Not found' status code.
        """
        return status.HTTP_404_NOT_FOUND


class EntityNotFoundByParams(IduApiError):
    """
    Exception to raise when requested entity was not found in the database by the identifier.
    """

    def __init__(self, entity: str, *args):
        """
        Construct from requested identifier and entity (table) name.
        """
        self.entity = entity
        self.params = tuple(args)
        super().__init__()

    def __str__(self) -> str:
        return f"Entity '{self.entity}' with such parameters={self.params} is not found"

    def get_status_code(self) -> int:
        """
        Return '404 Not found' status code.
        """
        return status.HTTP_404_NOT_FOUND


class EntityAlreadyExists(IduApiError):
    """
    Exception to raise when requested entity with the same parameters was found in the database.
    """

    def __init__(self, entity: str, *args):
        """
        Construct from requested identifier and entity (table) name.
        """
        self.entity = entity
        self.params = tuple(args)
        super().__init__()

    def __str__(self) -> str:
        return f"Invalid input! '{self.entity}' with the same parameters={self.params} already exists)"

    def get_status_code(self) -> int:
        """
        Return '404 Not found' status code.
        """
        return status.HTTP_404_NOT_FOUND


class AccessDeniedError(IduApiError):
    """
    Exception to raise when you do not have access rights to a resource.
    """

    def __init__(self, requested_id: int, entity: str):
        """
        Construct from requested identifier and entity (table) name.
        """
        self.requested_id = requested_id
        self.entity = entity
        super().__init__()

    def __str__(self) -> str:
        return f"Access for entity '{self.entity}' with id={self.requested_id} is denied"

    def get_status_code(self) -> int:
        """
        Return '403 Forbidden' status code.
        """
        return status.HTTP_403_FORBIDDEN
