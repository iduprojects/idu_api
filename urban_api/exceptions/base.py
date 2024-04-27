"""
Head API exception class is defined here.
"""
from fastapi import HTTPException, status


class NotesApiError(HTTPException):
    """
    Base Notes API exception to inherit from.
    User can redefine `status_code` method and `__str__` as its value will be used in HTTPException.
    """

    def __init__(self):
        super().__init__(self.get_status_code(), str(self))

    def get_status_code(self) -> int:
        """
        Return FastApi response status for an HTTPException. Descestors should override this method,
            but it defaults to 500 - Internal Server Error.
        """
        return status.HTTP_500_INTERNAL_SERVER_ERROR
