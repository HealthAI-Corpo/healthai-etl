import time
import requests
from src.utils.logger import logger

_CACHE_TTL = 600  # 10 minutes, même durée que jwks-rsa côté NestJS
_cache: dict[str, dict] = {}


def fetch_jwks(jwks_uri: str) -> list[dict]:
    now = time.monotonic()
    cached = _cache.get(jwks_uri)
    if cached and now - cached["ts"] < _CACHE_TTL:
        return cached["keys"]

    logger.info("Récupération JWKS | URI : {}", jwks_uri)
    resp = requests.get(jwks_uri, timeout=10)
    resp.raise_for_status()
    keys = resp.json().get("keys", [])
    _cache[jwks_uri] = {"ts": now, "keys": keys}
    return keys


def get_key_for_kid(jwks_uri: str, kid: str) -> dict:
    keys = fetch_jwks(jwks_uri)
    for k in keys:
        if k.get("kid") == kid:
            return k
    # Invalide le cache et réessaie une fois (rotation de clé ZITADEL)
    _cache.pop(jwks_uri, None)
    keys = fetch_jwks(jwks_uri)
    for k in keys:
        if k.get("kid") == kid:
            return k
    raise ValueError(f"Clé introuvable pour kid={kid!r}")
