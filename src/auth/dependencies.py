import os
from typing import Optional

import requests
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError, jwt

from src.auth.jwks import get_key_for_kid
from src.utils.logger import logger

# auto_error=False pour renvoyer 401 (non authentifié) plutôt que 403 (interdit)
_bearer = HTTPBearer(auto_error=False)


def _issuer() -> str:
    return os.getenv("ZITADEL_ISSUER", "").rstrip("/")


def _jwks_uri() -> str:
    return f"{_issuer()}/oauth/v2/keys"


def _audience() -> str:
    return os.getenv("JWT_AUDIENCE", "")


async def require_auth(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(_bearer),
) -> dict:
    if credentials is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token manquant",
            headers={"WWW-Authenticate": "Bearer"},
        )

    issuer = _issuer()
    audience = _audience()
    if not issuer or not audience:
        logger.error("Configuration auth manquante | ZITADEL_ISSUER ou JWT_AUDIENCE non défini")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Configuration auth manquante",
        )

    token = credentials.credentials
    try:
        header = jwt.get_unverified_header(token)
        kid = header.get("kid")
        if not kid:
            raise JWTError("Aucun kid dans le header du token")

        jwk = get_key_for_kid(_jwks_uri(), kid)
        payload = jwt.decode(
            token,
            jwk,
            algorithms=["RS256"],
            issuer=issuer,
            audience=audience,
        )
        logger.info("Token valide | sub={}", payload.get("sub"))
        return payload

    except requests.RequestException as exc:
        logger.error("Erreur lors de la récupération JWKS | {}", exc)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Service d'authentification indisponible",
        )
    except (JWTError, ValueError) as exc:
        logger.warning("Token invalide | {}", exc)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=str(exc),
            headers={"WWW-Authenticate": "Bearer"},
        )
