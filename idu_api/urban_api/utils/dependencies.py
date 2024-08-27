"""FastAPI keycloak are defined here."""

import json
from base64 import b64decode
from typing import Annotated

from cachetools import TTLCache
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from idu_api.urban_api.dto.users import UserDTO

cache = TTLCache(maxsize=100, ttl=1800)


async def access_token_dependency(
    access_token: Annotated[HTTPAuthorizationCredentials, Depends(HTTPBearer())],
) -> dict:

    result = cache.get(access_token.credentials)
    if result is not None:
        return result

    try:
        return json.loads(b64decode(access_token.credentials.split('.')[1]))
    except Exception as e:
        raise HTTPException(  # pylint: disable=raise-missing-from
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=str(e),  # "Invalid authentication credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )


async def user_dependency(
    payload: dict = Depends(access_token_dependency),
) -> UserDTO:
    """
    Return user fetched from the database by email from a validated access token.

    Ensures that User is approved to log in and valid.
    """

    try:
        return UserDTO(
            id=payload.get("sub"),
            is_active=payload.get("active")
        )
    except Exception as e:
        raise HTTPException(  # pylint: disable=raise-missing-from
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),  # "Invalid authentication credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )
