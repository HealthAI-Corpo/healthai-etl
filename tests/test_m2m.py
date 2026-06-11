"""Tests du client M2M Zitadel (flux JWT Profile, clé machine JSON)."""

import json
import time
from unittest.mock import MagicMock, patch

import pytest
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.rsa import generate_private_key
from jose import jwt as jose_jwt

from src.auth import m2m

TEST_ISSUER = "https://zitadel.test"
TEST_PROJECT_ID = "375235925845737475"


@pytest.fixture(scope="module")
def machine_key():
    """Clé machine JSON telle que téléchargée depuis Zitadel."""
    private_key = generate_private_key(
        public_exponent=65537, key_size=2048, backend=default_backend()
    )
    pem = private_key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.TraditionalOpenSSL,
        serialization.NoEncryption(),
    ).decode()
    public_pem = (
        private_key.public_key()
        .public_bytes(
            serialization.Encoding.PEM,
            serialization.PublicFormat.SubjectPublicKeyInfo,
        )
        .decode()
    )
    return {
        "key": {
            "type": "serviceaccount",
            "keyId": "key-123",
            "key": pem,
            "userId": "user-456",
        },
        "public_pem": public_pem,
    }


@pytest.fixture
def key_file(tmp_path, machine_key, monkeypatch):
    path = tmp_path / "etl-service-key.json"
    path.write_text(json.dumps(machine_key["key"]))
    monkeypatch.setenv("ZITADEL_ISSUER", TEST_ISSUER)
    monkeypatch.setenv("ZITADEL_PROJECT_ID", TEST_PROJECT_ID)
    monkeypatch.setenv("ZITADEL_M2M_KEY_FILE", str(path))
    m2m._token_cache.clear()
    return path


# ── build_assertion ───────────────────────────────────────────────────────────


def test_assertion_claims_and_signature(machine_key):
    assertion = m2m.build_assertion(machine_key["key"], TEST_ISSUER)

    header = jose_jwt.get_unverified_header(assertion)
    assert header["kid"] == "key-123"
    assert header["alg"] == "RS256"

    payload = jose_jwt.decode(
        assertion,
        machine_key["public_pem"],
        algorithms=["RS256"],
        audience=TEST_ISSUER,
        options={"verify_at_hash": False},
    )
    assert payload["iss"] == "user-456"
    assert payload["sub"] == "user-456"
    assert payload["exp"] > int(time.time())


# ── get_m2m_token ─────────────────────────────────────────────────────────────


def _token_response(token="m2m-token", expires_in=3600):
    resp = MagicMock()
    resp.json.return_value = {"access_token": token, "expires_in": expires_in}
    resp.raise_for_status.return_value = None
    return resp


def test_get_token_posts_jwt_bearer_grant(key_file):
    with patch.object(m2m.requests, "post", return_value=_token_response()) as post:
        token = m2m.get_m2m_token()

    assert token == "m2m-token"
    url = post.call_args.args[0]
    data = post.call_args.kwargs["data"]
    assert url == f"{TEST_ISSUER}/oauth/v2/token"
    assert data["grant_type"] == "urn:ietf:params:oauth:grant-type:jwt-bearer"
    assert f"urn:zitadel:iam:org:project:id:{TEST_PROJECT_ID}:aud" in data["scope"]
    assert "urn:zitadel:iam:org:projects:roles" in data["scope"]


def test_get_token_is_cached_until_expiry(key_file):
    with patch.object(m2m.requests, "post", return_value=_token_response()) as post:
        first = m2m.get_m2m_token()
        second = m2m.get_m2m_token()

    assert first == second
    assert post.call_count == 1  # 2e appel servi par le cache


def test_missing_config_raises(monkeypatch):
    monkeypatch.delenv("ZITADEL_ISSUER", raising=False)
    monkeypatch.delenv("ZITADEL_PROJECT_ID", raising=False)
    monkeypatch.delenv("ZITADEL_M2M_KEY_FILE", raising=False)
    m2m._token_cache.clear()

    with pytest.raises(RuntimeError, match="Config M2M manquante"):
        m2m.get_m2m_token()
