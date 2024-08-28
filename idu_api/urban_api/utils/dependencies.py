"""FastAPI authentication client is defined here."""

import json
from base64 import b64decode
from datetime import datetime
from typing import Annotated

import httpx
from cachetools import TTLCache
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from idu_api.urban_api.config import UrbanAPIConfig
from idu_api.urban_api.dto.users import UserDTO

config = UrbanAPIConfig.try_from_env()
cache = TTLCache(maxsize=config.cache_size, ttl=config.cache_ttl)


async def access_token_dependency(
    access_token: Annotated[HTTPAuthorizationCredentials, Depends(HTTPBearer())],
) -> UserDTO:
    """Decode the JWT token, extract user information, and validate the token if necessary."""

    cached_result = cache.get(access_token.credentials)
    if cached_result is not None:
        return cached_result

    try:
        payload = json.loads(b64decode(access_token.credentials.split(".")[1]))
        UserDTO(id=payload.get("sub"), is_active=payload.get("active"))
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication token",
            headers={"WWW-Authenticate": "Bearer"},
        )

    cache[access_token.credentials] = payload

    if not config.validate:
        return payload

    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                config.authentication_url, headers={"Authorization": f"Bearer {access_token.credentials}"}
            )
        if response.status_code != 200:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid token signature",
                headers={"WWW-Authenticate": "Bearer"},
            )
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Error verifying token signature",
            headers={"WWW-Authenticate": "Bearer"},
        )

    try:
        if "exp" in payload and datetime.utcfromtimestamp(payload["exp"]) < datetime.utcnow():
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Token has expired, please refresh",
                headers={"WWW-Authenticate": "Bearer"},
            )
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid expiration format",
            headers={"WWW-Authenticate": "Bearer"},
        )

    return payload


async def user_dependency(
    user: dict = Depends(access_token_dependency),
) -> UserDTO:
    """Return UserDTO created by access_token_dependency."""
    return UserDTO(id=user.get("sub"), is_active=user.get("active"))
