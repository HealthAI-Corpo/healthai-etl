"""Client M2M Zitadel — flux JWT Profile (clé machine JSON).

Le service user prouve son identité en signant une assertion JWT avec la
clé privée du fichier JSON téléchargé depuis Zitadel (Users → Keys), puis
l'échange contre un access token portant l'audience du projet et le rôle
admin.

Usage (module) :
    from src.auth.m2m import get_m2m_token
    token = get_m2m_token()  # mis en cache jusqu'à expiration

Usage (CLI, pratique pour un cron) :
    TOKEN=$(uv run python -m src.auth.m2m)
    curl -X POST https://etl/run-all -H "Authorization: Bearer $TOKEN"
"""

import json
import os
import time

import requests
from dotenv import load_dotenv
from jose import jwt

from src.utils.logger import logger

load_dotenv()

# Renouvelle le token quand il reste moins de 60 s de validité
_SAFETY_MARGIN = 60
_ASSERTION_TTL = 300

_token_cache: dict = {}


def _issuer() -> str:
    return os.getenv("ZITADEL_ISSUER", "").rstrip("/")


def _project_id() -> str:
    return os.getenv("ZITADEL_PROJECT_ID", "")


def _key_file() -> str:
    return os.getenv("ZITADEL_M2M_KEY_FILE", "")


def build_assertion(key: dict, issuer: str) -> str:
    """Assertion JWT signée avec la clé privée du service user."""
    now = int(time.time())
    claims = {
        "iss": key["userId"],
        "sub": key["userId"],
        "aud": issuer,
        "iat": now,
        "exp": now + _ASSERTION_TTL,
    }
    return jwt.encode(
        claims, key["key"], algorithm="RS256", headers={"kid": key["keyId"]}
    )


def get_m2m_token() -> str:
    """Access token JWT du service user — avec cache jusqu'à expiration."""
    cached = _token_cache.get("token")
    if cached and time.time() < cached["exp"] - _SAFETY_MARGIN:
        return cached["value"]

    issuer = _issuer()
    project_id = _project_id()
    key_file = _key_file()
    if not issuer or not project_id or not key_file:
        raise RuntimeError(
            "Config M2M manquante : ZITADEL_ISSUER, ZITADEL_PROJECT_ID "
            "et ZITADEL_M2M_KEY_FILE sont requis"
        )

    with open(key_file) as fh:
        key = json.load(fh)

    scope = (
        "openid "
        f"urn:zitadel:iam:org:project:id:{project_id}:aud "
        "urn:zitadel:iam:org:projects:roles"
    )
    resp = requests.post(
        f"{issuer}/oauth/v2/token",
        data={
            "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
            "assertion": build_assertion(key, issuer),
            "scope": scope,
        },
        timeout=10,
    )
    resp.raise_for_status()
    body = resp.json()

    token = body["access_token"]
    expires_in = int(body.get("expires_in", 3600))
    _token_cache["token"] = {"value": token, "exp": time.time() + expires_in}
    logger.info("Token M2M obtenu | expire dans {}s", expires_in)
    return token


if __name__ == "__main__":
    print(get_m2m_token())
