"""Tests placeholder — serveur FastAPI ETL."""

from fastapi.testclient import TestClient

from src.server import app

client = TestClient(app)


def test_health() -> None:
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"
