"""Tests unitaires — server.py (endpoints FastAPI)

On utilise le TestClient de Starlette — pas de vraie connexion réseau.
Le pipeline ETL est mocké pour éviter toute écriture disque ou DB.
"""

import io
from unittest.mock import patch

from fastapi.testclient import TestClient

from src.auth.dependencies import require_auth
from src.server import app


def setup_module():
    app.dependency_overrides[require_auth] = lambda: {"sub": "test-user"}


def teardown_module():
    app.dependency_overrides.pop(require_auth, None)


client = TestClient(app)

PIPELINE_TARGET = "src.server.execute_pipeline_exercisedb_hobby"


# ---------------------------------------------------------------------------
# Validation extension fichier
# ---------------------------------------------------------------------------


def test_upload_extension_invalide_retourne_400():
    data = {"file": ("data.txt", io.BytesIO(b"content"), "text/plain")}
    response = client.post("/upload/exercices", files=data)
    assert response.status_code == 400
    assert "Format non supporté" in response.json()["detail"]


def test_upload_extension_xml_retourne_400():
    data = {"file": ("data.xml", io.BytesIO(b"<root/>"), "application/xml")}
    response = client.post("/upload/exercices", files=data)
    assert response.status_code == 400


# ---------------------------------------------------------------------------
# Type de pipeline inconnu
# ---------------------------------------------------------------------------


def test_upload_pipeline_type_inconnu_retourne_400():
    data = {"file": ("data.csv", io.BytesIO(b"a,b\n1,2"), "text/csv")}
    response = client.post("/upload/pipeline_inexistant", files=data)
    assert response.status_code == 400
    assert "pipeline inconnu" in response.json()["detail"].lower()


# ---------------------------------------------------------------------------
# Upload valide — exercises
# ---------------------------------------------------------------------------


def test_upload_exercises_json_retourne_202():
    with patch(PIPELINE_TARGET, return_value=[]):
        data = {"file": ("exercises.json", io.BytesIO(b"[]"), "application/json")}
        response = client.post("/upload/exercices", files=data)
    assert response.status_code == 202


def test_upload_exercises_reponse_contient_nom_fichier():
    with patch(PIPELINE_TARGET, return_value=[]):
        data = {"file": ("exercises.json", io.BytesIO(b"[]"), "application/json")}
        response = client.post("/upload/exercices", files=data)
    body = response.json()
    assert body["file"] == "exercises.json"
    assert "message" in body


def test_upload_exercises_csv_accepte():
    with patch(PIPELINE_TARGET, return_value=[]):
        data = {
            "file": (
                "exercises.csv",
                io.BytesIO(b"name,type\nPush-up,strength"),
                "text/csv",
            )
        }
        response = client.post("/upload/exercices", files=data)
    assert response.status_code == 202
