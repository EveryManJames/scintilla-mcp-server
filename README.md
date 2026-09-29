# Scintilla MCP

A small, self-hosted [Model Context Protocol](https://modelcontextprotocol.io) server that gives
AI assistants (Claude, and any other MCP client) **read-only** access to your organization's
**Walmart Scintilla BI Link** data.

BI Link is a Google BigQuery share: Walmart Data Ventures gives each supplier a service-account
key and a dataset (`wmt-dv-bi-link-prod.dv_supplier`) of ~54 views covering store sales,
inventory and in-stock, demand and order forecasts, out-of-stock root cause, returns, modular
placement, item and store dimensions. This server sits in front of that share so your team can
ask questions in plain English while the key stays on the server.

> Not affiliated with, endorsed by, or supported by Walmart Inc. or Walmart Data Ventures.
> You need your own BI Link subscription and service-account key.

## What you get

| Tool | Purpose |
|---|---|
| `scintilla_list_views` | Every view in the share, with a one-line description and last refresh time |
| `scintilla_describe_view` | Columns, types and 3 sample rows for one view |
| `scintilla_query` | Read-only SQL: `SELECT`/`WITH` only, single statement, `dv_supplier` only, automatic `LIMIT` |
| `scintilla_find_items` | Look up Walmart item numbers, UPCs, names, brand and category |
| `scintilla_sales_summary` | This year vs last year sales, units, AUR and store counts by item, category, brand, channel, week, region or total |
| `scintilla_inventory_snapshot` | Latest on-hand, on-order, in-transit, replenishment in-stock % and zero-on-hand stores by item |
| `scintilla_oos_root_cause` | Walmart's out-of-stock attribution (Supplier, Store Ops, Forecast, Modular, Other) by reason, item or month |
| `scintilla_demand_forecast` | Store demand forecast summed by item and week |
| `scintilla_item_store_detail` | Store-level sales and in-stock for one item, worst in-stock first |

Guardrails: SQL is limited to `SELECT` against the BI Link dataset (DDL/DML keywords and other
projects are rejected), results are capped (`MAX_ROWS`, default 500), queries time out
(`QUERY_TIMEOUT_S`, default 90), and every request needs the bearer token. The service account
Walmart issues is itself read-only, so there is no write path at any layer.

## How it works

```
Claude / MCP client  --HTTPS, bearer token-->  Scintilla MCP (this repo)  --service account-->  BigQuery (Walmart's share)
```

The server speaks MCP over Streamable HTTP (stateless, JSON responses) and runs anywhere a
container runs. Query cost is billed to Walmart's project under the BI Link terms; the views scan
the full shared tables even for small aggregates, so per-query byte caps are off by default.

## Quick start (local)

```bash
git clone https://github.com/<you>/scintilla-mcp-server.git
cd scintilla-mcp-server
cp .env.example .env            # fill in GCP_SERVICE_ACCOUNT_JSON and MCP_BEARER_TOKEN
docker compose up --build
```

Then:

```bash
curl -s localhost:8080/health          # -> ok
curl -s -X POST localhost:8080/mcp/$MCP_BEARER_TOKEN \
  -H 'content-type: application/json' -H 'accept: application/json, text/event-stream' \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}'
```

Without Docker: `pip install -r requirements.txt`, export the env vars, `python -m scintilla_mcp.server`.

## Configuration

| Variable | Required | Default | Notes |
|---|---|---|---|
| `GCP_SERVICE_ACCOUNT_JSON` | yes | | The full JSON key Walmart Data Ventures issued (`svc-wmt-dv-...@wmt-dv-bi-link-prod.iam.gserviceaccount.com`). Paste the whole file as one value. |
| `MCP_BEARER_TOKEN` | yes | | Long random secret. `openssl rand -hex 32`. |
| `BQ_PROJECT` | no | `wmt-dv-bi-link-prod` | Only change if Walmart tells you to. |
| `BQ_DATASET` | no | `dv_supplier` | Only change if Walmart tells you to. |
| `VENDOR_NBRS` | no | | Comma-separated Walmart vendor numbers to restrict item lookups to (trims legacy vendor records). |
| `MAX_ROWS` | no | `500` | Row cap per tool call. |
| `QUERY_TIMEOUT_S` | no | `90` | BigQuery job timeout. |
| `MAX_BYTES_BILLED` | no | `0` (off) | Per-query byte cap. Leave off unless you pay for the queries. |
| `ALLOWED_HOSTS` | no | | Comma-separated hostnames to re-enable the SDK's Host-header check. Off by default because the server sits behind a reverse proxy and is gated by the token. |
| `HC_PING_URL` | no | | Healthchecks.io ping URL. Enables a heartbeat every `HEARTBEAT_INTERVAL_S` (600) that probes BigQuery and pings ok or `/fail`. |
| `AXIOM_TOKEN`, `AXIOM_DATASET`, `AXIOM_AGENT_ID` | no | `agent-runs`, `scintilla-mcp` | Axiom ingest. Heartbeats and each tool call are written as `started` / `finished` / `failed` events. |
| `PORT` | no | `8080` | |

## Authentication

Every MCP request must carry the shared token, in one of two forms:

- `Authorization: Bearer <token>` (Claude Desktop, Claude Code, Cursor, any client that can set headers)
- The last path segment: `https://<host>/mcp/<token>` (claude.ai organization and custom connectors, which cannot set headers)

Requests without a valid token get `401`. OAuth discovery paths return `404` on purpose so
claude.ai does not attempt OAuth client registration. If you need per-user identity, put an
OAuth-aware proxy in front (Cloudflare Access, Entra, etc.) and keep this token as the
service-to-service secret.

## Deploying

Any platform that runs a Docker image and terminates TLS works. Included:

- **DigitalOcean App Platform**: `.do/app.yaml` (edit the repo name, set the two secrets). About $5/month.
- **Docker Compose**: `docker-compose.yml` for a VM or on-prem box behind your own reverse proxy.

Also tested patterns: Fly.io (`fly launch` on the Dockerfile), Google Cloud Run (`gcloud run deploy --source .`),
Azure Container Apps, Render. The container listens on `PORT` and exposes `GET /health` for probes.

## Connecting a client

**claude.ai (organization or personal custom connector)**: Settings → Connectors → Add custom
connector. Name it (for example `Scintilla MCP`), URL `https://<host>/mcp/<MCP_BEARER_TOKEN>`,
no OAuth. Members then enable it under their own Connectors settings.

**Claude Desktop / Claude Code / other MCP clients** (`mcp.json`):

```json
{
  "mcpServers": {
    "scintilla": {
      "type": "http",
      "url": "https://<host>/mcp",
      "headers": { "Authorization": "Bearer <MCP_BEARER_TOKEN>" }
    }
  }
}
```

First things to ask: *"List the Scintilla views."* · *"Sales by category, last 4 weeks vs last year."* ·
*"Which items have the worst in-stock right now?"* · *"What is Walmart attributing our out-of-stocks to this month?"*

## Notes on the data

- `ty_*` columns are this year, `ly_*` the same period last year as Walmart aligns it. Dollars are retail.
- `store_sales` covers Walmart US stores plus store-fulfilled online (`svc_chnl_nm`: `BIS` in-store, `PICKUP`, `DELIVERY`, `SFS`, `S2H`). Marketplace and Sam's Club are not in BI Link.
- The share is a rolling 52 weeks. If you need history, land the views in your own warehouse on a schedule.
- `store_invt` and `hourly_store_inventory` are tens of millions of rows; aggregate in SQL, never pull raw store-item-days through the model.

## Development

```bash
pip install -r requirements.txt -r requirements-dev.txt
pytest
```

Tests cover the SQL guard and auth middleware without touching BigQuery.

## License

MIT. See `LICENSE`.
