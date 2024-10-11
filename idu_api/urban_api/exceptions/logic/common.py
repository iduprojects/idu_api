"""
Exceptions connected with entities in urban_db are defined here.
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
        return f"Invalid input! '{self.entity}' with the same parameters={self.params} already exists"

    def get_status_code(self) -> int:
        """
        Return '404 Not found' status code.
        """
        return status.HTTP_400_BAD_REQUEST


class TooManyObjectsError(IduApiError):
    """Exception to raise when number of objects to be returned is too big."""

    def __init__(self, number_of_objects: int, limit: int | None = None):
        """Construct from actual number of objects and set limit."""
        self.objects = number_of_objects
        self.limit = limit
        super().__init__()

    def __str__(self) -> str:
        return (
            f"Too many objects to be returned. Value is {self.objects}" + f" , but configured  limit is {self.limit}"
            if self.limit is not None
            else ""
        )

    def get_status_code(self) -> int:
        """Return '400 Bad Request' status code."""
        return status.HTTP_400_BAD_REQUEST
