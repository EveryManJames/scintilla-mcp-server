import os

os.environ["MCP_BEARER_TOKEN"] = "test-token"
from starlette.testclient import TestClient  # noqa: E402
from scintilla_mcp import server  # noqa: E402

client = TestClient(server.app)
INIT = {"jsonrpc": "2.0", "id": 0, "method": "initialize",
        "params": {"protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "t", "version": "1"}}}
HDR = {"content-type": "application/json", "accept": "application/json, text/event-stream"}


def test_health_is_open():
    assert client.get("/health").status_code == 200


def test_oauth_discovery_is_404_not_401():
    r = client.get("/.well-known/oauth-protected-resource")
    assert r.status_code == 404
    assert "www-authenticate" not in {k.lower() for k in r.headers}


def test_missing_and_wrong_token_401():
    assert client.post("/mcp", json=INIT, headers=HDR).status_code == 401
    assert client.post("/mcp/nope", json=INIT, headers=HDR).status_code == 401
    assert client.post("/mcp", json=INIT, headers={**HDR, "authorization": "Bearer wrong"}).status_code == 401


def test_token_in_path_and_header_both_work():
    # The MCP session manager lives in the app lifespan, so run the client as a context manager.
    with TestClient(server.app) as c:
        assert c.post("/mcp/test-token", json=INIT, headers=HDR).status_code == 200
        assert c.post("/mcp", json=INIT, headers={**HDR, "authorization": "Bearer test-token"}).status_code == 200
