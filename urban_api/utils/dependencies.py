"""
FastApi dependencies are defined here.
"""
import asyncio
from fastapi import HTTPException, status, Depends
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy.ext.asyncio import AsyncConnection
from sqlalchemy import select, insert
from keycloak import KeycloakOpenID
from typing import Annotated
from cachetools import TTLCache

from urban_api.db.connection import get_connection
from urban_api.db.entities.users import users
from urban_api.dto.users import UserDTO
from urban_api.config.app_settings_global import app_settings


keycloak_openid = KeycloakOpenID(
    server_url=app_settings.keycloak_server_url,
    client_id=app_settings.client_id,
    realm_name=app_settings.realm,
    client_secret_key=app_settings.client_secret,
    verify=True,
)

cache = TTLCache(maxsize=100, ttl=60)


async def get_idp_public_key():
    return (
        "-----BEGIN PUBLIC KEY-----\n"
        f"{keycloak_openid.public_key()}"
        "\n-----END PUBLIC KEY-----"
    )


async def access_token_dependency(
        access_token: Annotated[HTTPAuthorizationCredentials, Depends(HTTPBearer())],
) -> dict:

    result = cache.get(access_token.credentials)
    if result is not None:
        print(f"Found it in cache for token {access_token.credentials}")
        return result

    try:
        result = keycloak_openid.decode_token(
            access_token.credentials,
            key=await get_idp_public_key(),
            options={
                "verify_signature": True,
                "verify_aud": False,
                "exp": True
            }
        )
        await asyncio.sleep(5)

        # Store the result in the cache
        cache[access_token.credentials] = result

        return result

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=str(e),  # "Invalid authentication credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )


async def user_dependency(
    payload: dict = Depends(access_token_dependency),
    conn: AsyncConnection = Depends(get_connection),
) -> UserDTO:
    """
    Return user fetched from the database by email from a validated access token.

    Ensures that User is approved to log in and valid.
    """

    statement = (select(users.c.is_banned).
                 where(users.c.id == payload.get('sub')))
    is_banned = (await conn.execute(statement)).fetchone()

    if is_banned is None:
        statement = insert(users).values(id=payload.get('sub')).returning(users.c.is_banned)
        is_banned = list(await conn.execute(statement))[0]
        await conn.commit()

    try:
        return UserDTO(
            id=payload.get('sub'),
            username=payload.get('username'),
            email=payload.get('email'),
            roles=list(payload.get("realm_access", {}).get("roles", [])),
            is_banned=bool(*is_banned)
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),  # "Invalid authentication credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )
