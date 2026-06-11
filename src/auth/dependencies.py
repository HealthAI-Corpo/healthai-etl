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
        logger.error(
            "Configuration auth manquante | ZITADEL_ISSUER ou JWT_AUDIENCE non défini"
        )
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


# Claim Zitadel contenant les rôles du projet. Deux formats selon le
# contexte : générique (tokens humains via le front) ou scopé par projet
# « urn:zitadel:iam:org:project:<id>:roles » (userinfo des service users).
ZITADEL_ROLES_CLAIM = "urn:zitadel:iam:org:project:roles"
_ROLES_PREFIX = "urn:zitadel:iam:org:project:"


def _extract_roles(claims: dict) -> dict:
    for key, value in claims.items():
        is_roles_claim = key == ZITADEL_ROLES_CLAIM or (
            key.startswith(_ROLES_PREFIX) and key.endswith(":roles")
        )
        if is_roles_claim and isinstance(value, dict) and value:
            return value
    return {}


def _fetch_userinfo_roles(token: str) -> dict:
    """Zitadel n'asserte pas les rôles dans l'access token des service
    users M2M — ils ne sont disponibles que via le userinfo endpoint."""
    try:
        resp = requests.get(
            f"{_issuer()}/oidc/v1/userinfo",
            headers={"Authorization": f"Bearer {token}"},
            timeout=10,
        )
        if not resp.ok:
            return {}
        return _extract_roles(resp.json())
    except requests.RequestException as exc:
        logger.warning("Userinfo injoignable pour les rôles | {}", exc)
        return {}


async def require_admin(
    payload: dict = Depends(require_auth),
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(_bearer),
) -> dict:
    """Réservé au rôle admin — humains (rôles dans le token) et service
    users M2M (rôles récupérés via userinfo avec le même token)."""
    roles = _extract_roles(payload)
    if not roles and credentials is not None:
        roles = _fetch_userinfo_roles(credentials.credentials)
    if "admin" not in roles:
        logger.warning(
            "Accès refusé, rôle admin requis | sub={} | roles={}",
            payload.get("sub"),
            list(roles.keys()),
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Rôle admin requis",
        )
    return payload
