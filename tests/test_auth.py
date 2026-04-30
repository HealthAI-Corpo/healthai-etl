"""
Tests de l'authentification ZITADEL JWT sur les endpoints FastAPI du service ETL.

Stratégie :
- Génération d'une paire RSA réelle (2048 bits) pour signer les tokens de test
- Mock de fetch_jwks pour retourner la clé publique sans appel réseau
- Mock des fonctions pipeline pour éviter tout accès BDD pendant les tests
"""

import base64
import time
from unittest.mock import MagicMock, patch

import pytest
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.rsa import generate_private_key
from fastapi.testclient import TestClient
from jose import jwt as jose_jwt

# ── Constantes de test ───────────────────────────────────────────────────────

TEST_ISSUER = "http://localhost:8080"
TEST_AUDIENCE = "test-audience"
TEST_KID = "test-key-1"

# ── Fixtures RSA ─────────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def rsa_private_key():
    return generate_private_key(
        public_exponent=65537,
        key_size=2048,
        backend=default_backend(),
    )


@pytest.fixture(scope="module")
def jwk_public(rsa_private_key):
    """JWK dict représentant la clé publique RSA (format ZITADEL JWKS)."""
    pub_numbers = rsa_private_key.public_key().public_numbers()

    def _b64url(n: int) -> str:
        length = (n.bit_length() + 7) // 8
        return base64.urlsafe_b64encode(n.to_bytes(length, "big")).rstrip(b"=").decode()

    return {
        "kty": "RSA",
        "use": "sig",
        "alg": "RS256",
        "kid": TEST_KID,
        "n": _b64url(pub_numbers.n),
        "e": _b64url(pub_numbers.e),
    }


# ── Helpers ───────────────────────────────────────────────────────────────────


def _sign_token(
    rsa_private_key,
    *,
    kid: str = TEST_KID,
    issuer: str = TEST_ISSUER,
    audience: str = TEST_AUDIENCE,
    exp_delta: int = 3600,
    extra_claims: dict | None = None,
) -> str:
    priv_pem = rsa_private_key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.TraditionalOpenSSL,
        serialization.NoEncryption(),
    ).decode()
    claims = {
        "sub": "user-test-123",
        "iss": issuer,
        "aud": audience,
        "exp": int(time.time()) + exp_delta,
        "iat": int(time.time()),
        **(extra_claims or {}),
    }
    return jose_jwt.encode(claims, priv_pem, algorithm="RS256", headers={"kid": kid})


# ── Fixture client avec mocks ─────────────────────────────────────────────────

# Mocks partagés pour toutes les pipelines (évite les appels BDD dans les tests)
_PIPELINE_MOCKS = {
    "src.server.execute_pipeline_daily_food": MagicMock(),
    "src.server.execute_pipeline_diet_recommendations_dataset": MagicMock(),
    "src.server.execute_pipeline_exercisedb_hobby": MagicMock(),
    "src.server.execute_pipeline_dataset_historique_seance_exercice": MagicMock(),
    "src.server.execute_pipeline_dataset_historique_seance_exercice_synthetic_data": MagicMock(),
    "src.server.run_all_pipelines": MagicMock(),
    "src.server.run_downloader": MagicMock(),
}


@pytest.fixture
def client(rsa_private_key, jwk_public, monkeypatch):
    monkeypatch.setenv("ZITADEL_ISSUER", TEST_ISSUER)
    monkeypatch.setenv("JWT_AUDIENCE", TEST_AUDIENCE)

    # Vide le cache JWKS entre les tests
    import src.auth.jwks as jwks_module
    jwks_module._cache.clear()

    with patch("src.auth.jwks.fetch_jwks", return_value=[jwk_public]):
        patchers = [patch(target, mock) for target, mock in _PIPELINE_MOCKS.items()]
        for p in patchers:
            p.start()

        from src.server import app
        with TestClient(app, raise_server_exceptions=False) as c:
            yield c

        for p in patchers:
            p.stop()


@pytest.fixture
def valid_token(rsa_private_key):
    return _sign_token(rsa_private_key)


@pytest.fixture
def expired_token(rsa_private_key):
    return _sign_token(rsa_private_key, exp_delta=-60)


# ── Tests : endpoint public /health ──────────────────────────────────────────


def test_health_no_auth(client):
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


def test_health_with_token(client, valid_token):
    resp = client.get("/health", headers={"Authorization": f"Bearer {valid_token}"})
    assert resp.status_code == 200


# ── Tests : absence de token (401) ────────────────────────────────────────────


@pytest.mark.parametrize(
    "method,url",
    [
        ("POST", "/run-all"),
        ("POST", "/run/exercices"),
        ("POST", "/run-download"),
    ],
)
def test_protected_route_no_token_returns_401(client, method, url):
    resp = client.request(method, url)
    assert resp.status_code == 401


# ── Tests : token valide (202) ────────────────────────────────────────────────


def test_run_all_valid_token(client, valid_token):
    resp = client.post("/run-all", headers={"Authorization": f"Bearer {valid_token}"})
    assert resp.status_code == 202


def test_run_pipeline_valid_token(client, valid_token):
    resp = client.post(
        "/run/exercices", headers={"Authorization": f"Bearer {valid_token}"}
    )
    assert resp.status_code == 202


def test_run_download_valid_token(client, valid_token):
    resp = client.post(
        "/run-download", headers={"Authorization": f"Bearer {valid_token}"}
    )
    assert resp.status_code == 202


# ── Tests : token invalide (401) ──────────────────────────────────────────────


def test_run_all_invalid_token_returns_401(client):
    resp = client.post(
        "/run-all", headers={"Authorization": "Bearer this.is.not.a.valid.jwt"}
    )
    assert resp.status_code == 401


def test_run_all_expired_token_returns_401(client, expired_token):
    resp = client.post(
        "/run-all", headers={"Authorization": f"Bearer {expired_token}"}
    )
    assert resp.status_code == 401


def test_run_all_wrong_audience_returns_401(client, rsa_private_key):
    token = _sign_token(rsa_private_key, audience="wrong-audience")
    resp = client.post("/run-all", headers={"Authorization": f"Bearer {token}"})
    assert resp.status_code == 401


def test_run_all_wrong_issuer_returns_401(client, rsa_private_key):
    token = _sign_token(rsa_private_key, issuer="http://evil.example.com")
    resp = client.post("/run-all", headers={"Authorization": f"Bearer {token}"})
    assert resp.status_code == 401


def test_run_all_unknown_kid_returns_401(client, rsa_private_key):
    token = _sign_token(rsa_private_key, kid="unknown-kid")
    resp = client.post("/run-all", headers={"Authorization": f"Bearer {token}"})
    assert resp.status_code == 401


# ── Tests : ZITADEL indisponible (503) ────────────────────────────────────────


def test_jwks_unavailable_returns_503(monkeypatch, rsa_private_key):
    import requests as req_lib

    monkeypatch.setenv("ZITADEL_ISSUER", TEST_ISSUER)
    monkeypatch.setenv("JWT_AUDIENCE", TEST_AUDIENCE)

    import src.auth.jwks as jwks_module
    jwks_module._cache.clear()

    with patch(
        "src.auth.jwks.fetch_jwks",
        side_effect=req_lib.ConnectionError("ZITADEL unreachable"),
    ):
        from src.server import app
        with TestClient(app, raise_server_exceptions=False) as c:
            token = _sign_token(rsa_private_key)
            resp = c.post("/run-all", headers={"Authorization": f"Bearer {token}"})
    assert resp.status_code == 503


# ── Tests : upload (route avec fichier) ──────────────────────────────────────


def test_upload_no_token_returns_401(client):
    import io
    resp = client.post(
        "/upload/aliments",
        files={"file": ("test.csv", io.BytesIO(b"a,b\n1,2"), "text/csv")},
    )
    assert resp.status_code == 401


def test_upload_valid_token_accepted(client, valid_token):
    import io
    resp = client.post(
        "/upload/aliments",
        files={"file": ("test.csv", io.BytesIO(b"a,b\n1,2"), "text/csv")},
        headers={"Authorization": f"Bearer {valid_token}"},
    )
    assert resp.status_code == 202
