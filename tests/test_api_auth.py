"""Shared-secret auth on /api/* (src/api/auth.py). No database needed."""
import pytest
from flask import Flask

from src.api.server import api as api_blueprint
from src.api.tools import tools as tools_blueprint


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setenv("KG_API_SECRET", "api-secret")
    monkeypatch.setenv("KG_TOOLS_TOKEN", "tools-token")
    app = Flask(__name__)
    app.register_blueprint(api_blueprint)
    app.register_blueprint(tools_blueprint)
    return app.test_client()


def _auth(token):
    return {"Authorization": f"Bearer {token}"}


@pytest.mark.parametrize("path", [
    "/api/matters",
    "/api/graph?matter_id=x",
    "/api/export?matter_id=x",
    "/api/tools/overview",
])
def test_rejects_missing_or_wrong_token(client, path):
    assert client.get(path).status_code == 401
    assert client.get(path, headers=_auth("wrong")).status_code == 401


def test_write_and_llm_routes_rejected_without_token(client):
    assert client.post("/api/query?matter_id=x", json={"query": "q"}).status_code == 401
    assert client.post("/api/entity?matter_id=x", json={}).status_code == 401
    assert client.delete("/api/document/abc?matter_id=x").status_code == 401


def test_tools_token_only_works_on_tools_routes(client):
    assert client.get("/api/matters", headers=_auth("tools-token")).status_code == 401
    # Passes auth; then fails the tools' own X-Matter-Id check.
    resp = client.get("/api/tools/overview", headers=_auth("tools-token"))
    assert resp.status_code == 400


def test_api_secret_passes_auth_on_tools(client):
    resp = client.get("/api/tools/overview", headers=_auth("api-secret"))
    assert resp.status_code == 400  # past auth, missing X-Matter-Id


def test_health_stays_open(client):
    assert client.get("/api/health").status_code != 401


def test_options_preflight_not_blocked(client):
    assert client.options("/api/matters").status_code != 401


def test_fails_closed_without_secret(client, monkeypatch):
    monkeypatch.delenv("KG_API_SECRET")
    assert client.get("/api/matters", headers=_auth("api-secret")).status_code == 503
