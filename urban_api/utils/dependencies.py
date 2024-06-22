"""FastApi keycloak are defined here."""

import asyncio
from typing import Annotated

from cachetools import TTLCache
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from keycloak import KeycloakOpenID

from urban_api.dto.users import UserDTO

# TODO: remove or refactor as dependency class
_keycloak_openid: KeycloakOpenID

cache = TTLCache(maxsize=100, ttl=60)


def configure_keycloak(keycloak_server_url: str, client_id: str, realm: str, client_secret: str):
    global _keycloak_openid  # pylint: disable=global-statement
    _keycloak_openid = KeycloakOpenID(
        server_url=keycloak_server_url,
        client_id=client_id,
        realm_name=realm,
        client_secret_key=client_secret,
        verify=True,
    )


async def get_idp_public_key():
    return "-----BEGIN PUBLIC KEY-----\n" f"{_keycloak_openid.public_key()}" "\n-----END PUBLIC KEY-----"


async def access_token_dependency(
    access_token: Annotated[HTTPAuthorizationCredentials, Depends(HTTPBearer())],
) -> dict:

    result = cache.get(access_token.credentials)
    if result is not None:
        print(f"Found it in cache for token {access_token.credentials}")
        return result

    try:
        result = _keycloak_openid.decode_token(
            access_token.credentials,
            key=await get_idp_public_key(),
            options={"verify_signature": True, "verify_aud": False, "exp": True},
        )
        await asyncio.sleep(5)

        # Store the result in the cache
        cache[access_token.credentials] = result

        return result

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
            username=payload.get("username"),
            email=payload.get("email"),
            roles=list(payload.get("realm_access", {}).get("roles", [])),
        )
    except Exception as e:
        raise HTTPException(  # pylint: disable=raise-missing-from
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),  # "Invalid authentication credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )
