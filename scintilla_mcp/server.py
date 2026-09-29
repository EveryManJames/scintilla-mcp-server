"""
Scintilla MCP: a remote Model Context Protocol server for Walmart Scintilla BI Link.

Exposes read-only, guard-railed access to the BigQuery share that Walmart Data
Ventures provisions for a supplier (project wmt-dv-bi-link-prod, dataset
dv_supplier) so AI assistants such as Claude can answer questions about Walmart
sales, inventory, forecast and out-of-stock data without anyone handling the
Google service-account key.

Auth: a shared bearer token, accepted either as `Authorization: Bearer <token>`
or as the last path segment (`/mcp/<token>`) for clients that cannot set
headers (for example claude.ai organization connectors).

Not affiliated with or endorsed by Walmart Inc. or Walmart Data Ventures.
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import re
import secrets
import time
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Callable, Optional

import httpx

from google.cloud import bigquery
from google.oauth2 import service_account
from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from pydantic import BaseModel, Field
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, PlainTextResponse

PROJECT = os.environ.get("BQ_PROJECT", "wmt-dv-bi-link-prod")
DATASET = os.environ.get("BQ_DATASET", "dv_supplier")
FQ = f"`{PROJECT}.{DATASET}."
MAX_ROWS = int(os.environ.get("MAX_ROWS", "500"))
MAX_BYTES_BILLED = int(os.environ.get("MAX_BYTES_BILLED", "0"))  # 0 = no cap. Walmart's authorized views scan
# the full shared base tables (~TBs) even for small aggregates, and billing is on Walmart's project.
QUERY_TIMEOUT_S = int(os.environ.get("QUERY_TIMEOUT_S", "90"))
# Optional: restrict item lookups to your own Walmart vendor number(s), comma-separated.
# BI Link already scopes the share to your items; this only trims legacy vendor records.
VENDOR_NBRS = [int(v) for v in os.environ.get("VENDOR_NBRS", "").replace(" ", "").split(",") if v]

# ---------------------------------------------------------------- observability (optional)
# Healthchecks.io: HC_PING_URL is the full ping URL for this service's check, e.g.
#   https://hc-ping.com/<ping-key>/scintilla-mcp   (the heartbeat appends /fail on failure)
# Axiom: one event per heartbeat run and per tool call into an Axiom dataset (AXIOM_DATASET),
#   shaped as {agent_id, event: started|finished|failed, duration_ms, items, error}.
# Both are optional; with the env vars unset they are silent no-ops.
HC_PING_URL = os.environ.get("HC_PING_URL", "").rstrip("/")
AXIOM_TOKEN = os.environ.get("AXIOM_TOKEN", "")
AXIOM_DATASET = os.environ.get("AXIOM_DATASET", "agent-runs")
AXIOM_AGENT_ID = os.environ.get("AXIOM_AGENT_ID", "scintilla-mcp")
HEARTBEAT_INTERVAL_S = int(os.environ.get("HEARTBEAT_INTERVAL_S", "600"))
log = logging.getLogger("scintilla-mcp")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")


async def axiom_event(event: str, **fields: Any) -> None:
    if not AXIOM_TOKEN:
        return
    body = [{"agent_id": AXIOM_AGENT_ID, "event": event, "service": "scintilla-mcp", **fields}]
    try:
        async with httpx.AsyncClient(timeout=8) as c:
            r = await c.post(f"https://api.axiom.co/v1/datasets/{AXIOM_DATASET}/ingest",
                             headers={"Authorization": f"Bearer {AXIOM_TOKEN}", "Content-Type": "application/json"}, json=body)
            if r.status_code >= 300:
                log.warning("axiom ingest %s: %s", r.status_code, r.text[:200])
    except Exception as e:  # never let telemetry break a request
        log.warning("axiom ingest failed: %s", e)


async def hc_ping(ok: bool, body: str = "") -> None:
    if not HC_PING_URL:
        return
    try:
        async with httpx.AsyncClient(timeout=8) as c:
            await c.post(HC_PING_URL + ("" if ok else "/fail"), content=body[:10_000], headers={"Content-Type": "text/plain"})
    except Exception as e:
        log.warning("healthchecks ping failed: %s", e)


def probe() -> dict[str, Any]:
    """Cheap end-to-end check: auth + BigQuery + the share still answering."""
    r = run_sql(f"SELECT COUNT(*) AS views, MAX(upd_ts) AS latest_refresh FROM {FQ}dv_table_upd_status`", max_rows=1)
    return r["rows"][0] if r["rows"] else {}


async def heartbeat_once() -> None:
    t0 = time.monotonic()
    await axiom_event("started", run="heartbeat")
    try:
        row = await asyncio.to_thread(probe)
        ms = int((time.monotonic() - t0) * 1000)
        await axiom_event("finished", run="heartbeat", duration_ms=ms, items=int(row.get("views") or 0), latest_refresh=str(row.get("latest_refresh")))
        await hc_ping(True, f"ok {ms}ms views={row.get('views')} latest_refresh={row.get('latest_refresh')}")
        log.info("heartbeat ok %dms views=%s", ms, row.get("views"))
    except Exception as e:
        ms = int((time.monotonic() - t0) * 1000)
        err = f"{type(e).__name__}: {str(e)[:500]}"
        await axiom_event("failed", run="heartbeat", duration_ms=ms, error=err)
        await hc_ping(False, err)
        log.error("heartbeat failed: %s", err)


async def heartbeat_loop() -> None:
    await asyncio.sleep(15)  # let the container settle
    while True:
        await heartbeat_once()
        await asyncio.sleep(HEARTBEAT_INTERVAL_S)


def instrumented(name: str, fn: Callable[..., str]) -> Callable[..., str]:
    """Wrap a tool so each call lands in Axiom as one finished/failed event. Sync tools
    run in FastMCP's worker thread, so telemetry is scheduled back onto the event loop."""
    import functools

    @functools.wraps(fn)
    def wrapper(*a: Any, **kw: Any) -> str:
        t0 = time.monotonic()
        try:
            out = fn(*a, **kw)
            ms = int((time.monotonic() - t0) * 1000)
            m = re.search(r"^(\d+) rows", out, re.M)
            rows = int(m.group(1)) if m else None
            _schedule(axiom_event("finished", run="tool_call", tool=name, duration_ms=ms, items=rows))
            return out
        except Exception as e:
            ms = int((time.monotonic() - t0) * 1000)
            _schedule(axiom_event("failed", run="tool_call", tool=name, duration_ms=ms, error=f"{type(e).__name__}: {str(e)[:500]}"))
            raise

    return wrapper


_loop: Optional[asyncio.AbstractEventLoop] = None


def _schedule(coro: Any) -> None:
    try:
        if _loop and _loop.is_running():
            asyncio.run_coroutine_threadsafe(coro, _loop)
        else:
            coro.close()
    except Exception:
        with contextlib.suppress(Exception):
            coro.close()

# ---------------------------------------------------------------- BigQuery client
_client: Optional[bigquery.Client] = None


def bq() -> bigquery.Client:
    global _client
    if _client is None:
        raw = os.environ.get("GCP_SERVICE_ACCOUNT_JSON")
        if not raw:
            raise RuntimeError("GCP_SERVICE_ACCOUNT_JSON env var is not set")
        info = json.loads(raw)
        creds = service_account.Credentials.from_service_account_info(info)
        _client = bigquery.Client(credentials=creds, project=info.get("project_id", PROJECT))
    return _client


def _jsonable(v: Any) -> Any:
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, (date, datetime)):
        return v.isoformat()
    return v


def run_sql(sql: str, params: Optional[dict[str, Any]] = None, max_rows: int = MAX_ROWS) -> dict[str, Any]:
    cfg = bigquery.QueryJobConfig(use_query_cache=True)
    if MAX_BYTES_BILLED > 0:
        cfg.maximum_bytes_billed = MAX_BYTES_BILLED
    if params:
        qp = []
        for k, v in params.items():
            if isinstance(v, bool):
                t = "BOOL"
            elif isinstance(v, int):
                t = "INT64"
            elif isinstance(v, float):
                t = "FLOAT64"
            elif isinstance(v, (date, datetime)):
                t = "DATE"
            else:
                t = "STRING"
            qp.append(bigquery.ScalarQueryParameter(k, t, v))
        cfg.query_parameters = qp
    job = bq().query(sql, job_config=cfg)
    it = job.result(timeout=QUERY_TIMEOUT_S, max_results=max_rows)
    cols = [f.name for f in it.schema]
    rows = [{c: _jsonable(r[c]) for c in cols} for r in it]
    return {
        "columns": cols,
        "rows": rows,
        "row_count": len(rows),
        "truncated": len(rows) >= max_rows,
        "bytes_processed": job.total_bytes_processed,
        "cache_hit": job.cache_hit,
    }


def fmt(result: dict[str, Any], note: str = "") -> str:
    """Compact text rendering for the model, plus the structured payload."""
    head = f"{result['row_count']} rows" + (" (truncated, add filters or LIMIT)" if result["truncated"] else "")
    if note:
        head = note + "\n" + head
    return head + "\n" + json.dumps(result["rows"], separators=(",", ":"), default=_jsonable)


# ---------------------------------------------------------------- SQL guard
_FORBIDDEN = re.compile(
    r"\b(INSERT|UPDATE|DELETE|MERGE|CREATE|DROP|ALTER|TRUNCATE|GRANT|REVOKE|EXPORT|LOAD|CALL|EXECUTE|BEGIN|COMMIT|ROLLBACK|DECLARE|SET)\b",
    re.I,
)
_TABLE_REF = re.compile(r"`?([\w-]+)`?\.`?(\w+)`?\.`?(\w+)`?")


def guard_sql(sql: str) -> str:
    s = sql.strip().rstrip(";")
    if ";" in s:
        raise ValueError("Only a single statement is allowed.")
    if not re.match(r"^\s*(SELECT|WITH)\b", s, re.I):
        raise ValueError("Only SELECT (or WITH ... SELECT) queries are allowed.")
    if _FORBIDDEN.search(s):
        raise ValueError("Query contains a disallowed keyword. This connector is read-only.")
    for proj, ds, _tbl in _TABLE_REF.findall(s):
        if proj != PROJECT or ds != DATASET:
            raise ValueError(f"Only tables in {PROJECT}.{DATASET} may be queried (found {proj}.{ds}).")
    if not re.search(r"\bLIMIT\s+\d+", s, re.I):
        s = f"{s}\nLIMIT {MAX_ROWS}"
    return s


# ---------------------------------------------------------------- MCP server
mcp = FastMCP(
    "Scintilla (Walmart BI Link)",
    instructions=(
        "Read-only access to this organization's Walmart Scintilla BI Link data (BigQuery dataset "
        f"{PROJECT}.{DATASET}). Data covers Walmart US stores plus store-fulfilled online orders, "
        "rolling 52 weeks, refreshed daily. Money columns are retail dollars. 'ty_' columns are this year, "
        "'ly_' the same period last year as Walmart aligns it. Start with scintilla_list_views or a curated tool; "
        "use scintilla_query for anything else. Always state the date range you used."
    ),
    stateless_http=True,
    json_response=True,
    # Public hostname behind DO ingress; access is gated by the bearer token, so the SDK's
    # localhost-only Host check is disabled. Set ALLOWED_HOSTS to re-enable with an allow-list.
    transport_security=TransportSecuritySettings(
        enable_dns_rebinding_protection=bool(os.environ.get("ALLOWED_HOSTS")),
        allowed_hosts=[h.strip() for h in os.environ.get("ALLOWED_HOSTS", "").split(",") if h.strip()],
    ),
)

VIEW_NOTES = {
    "store_sales": "Daily store x item sales. Key cols: bus_dt, wm_yr_wk_nbr, store_nbr, wm_item_nbr, svc_chnl_nm (BIS=in-store, PICKUP, DELIVERY, SFS, S2H), ty_/ly_ sales_amt, qty, aur, scan_cnt.",
    "store_invt": "Daily store x item inventory: on_hand, on_order, in_trnst, in_whse qty and retail; repl_instock numerator/denominator; traited_cnt; receipts.",
    "item_dim": "Item master: wm_item_nbr, item_name, wm_upc_nbr, gtin, brand_nm, omni_catg_desc, item_status_desc, base_unit_rtl_amt, vnpk/whpk, buyer.",
    "store_dim": "Store master: store_nbr, region_nbr, market, state, store type/format.",
    "calendar_dim": "Walmart fiscal calendar: wm_yr_wk_nbr to dates.",
    "oosrootcause": "Walmart's out-of-stock root cause per store-item-day: hi/med/low level reason (Supplier, Store Ops, Forecast, Modular, Other), frac_instk_pct.",
    "daily_demand_forecast": "Store x item daily demand forecast ~4 weeks forward: fcst_dt, fcst_tot_dmand_each_qty.",
    "order_frcst": "Suggested store orders: order_place_dt, sugg_order_dt, dc_nbr, order_each_qty, need/sched arrival dates.",
    "omni_sales": "Daily item sales by order channel (Store vs Ecomm) with GMV, net sales, qty, cost and margin.",
    "upc_sales": "Weekly store x UPC sales with basket_cnt (wm_yr_wk_nbr grain).",
    "store_returns": "Store returns by rtn_dt, item, reason code, qty and amount.",
    "ecom_returns": "Ecom returns.",
    "modular_upc_loc": "Planogram placement of UPCs by store modular.",
    "store_modular": "Store to modular assignment.",
    "item_trait": "Item traits (store assortment eligibility).",
    "store_trait": "Store traits.",
    "dc_item": "DC-level item inventory/alignment.",
    "dv_table_upd_status": "Last refresh timestamp per view.",
}


def tool(name: str) -> Callable[[Callable[..., str]], Callable[..., str]]:
    """Register a read-only tool with Axiom call telemetry."""
    def deco(fn: Callable[..., str]) -> Callable[..., str]:
        return mcp.tool(name=name, annotations={"readOnlyHint": True})(instrumented(name, fn))
    return deco


class ViewName(BaseModel):
    view: str = Field(description="View name inside dv_supplier, e.g. store_sales, store_invt, item_dim.")


@tool("scintilla_list_views")
def list_views() -> str:
    """List the Walmart BI Link views available to query, with a one-line description of each and its last refresh time."""
    upd = {r["table_name"]: r["upd_ts"] for r in run_sql(f"SELECT table_name, upd_ts FROM {FQ}dv_table_upd_status`")["rows"]}
    tables = [t.table_id for t in bq().list_tables(DATASET)]
    lines = [f"{PROJECT}.{DATASET} ({len(tables)} views). Use scintilla_describe_view for columns."]
    for t in sorted(tables):
        lines.append(f"- {t}: {VIEW_NOTES.get(t, '')} (refreshed {upd.get(t, 'n/a')})")
    return "\n".join(lines)


@tool("scintilla_describe_view")
def describe_view(view: str) -> str:
    """Return the columns and data types of one dv_supplier view, plus 3 sample rows."""
    if not re.fullmatch(r"\w+", view):
        raise ValueError("Invalid view name.")
    cols = run_sql(
        f"SELECT column_name, data_type FROM `{PROJECT}.{DATASET}.INFORMATION_SCHEMA.COLUMNS` WHERE table_name=@v ORDER BY ordinal_position",
        {"v": view},
        max_rows=400,
    )
    if not cols["rows"]:
        raise ValueError(f"View '{view}' not found. Call scintilla_list_views.")
    sample = run_sql(f"SELECT * FROM {FQ}{view}` LIMIT 3", max_rows=3)
    out = [f"{view}: {VIEW_NOTES.get(view, '')}", "Columns:"]
    out += [f"  {r['column_name']} {r['data_type']}" for r in cols["rows"]]
    out.append("Sample rows:")
    out.append(json.dumps(sample["rows"], default=_jsonable))
    return "\n".join(out)


@tool("scintilla_query")
def query(sql: str = Field(description=f"A single BigQuery Standard SQL SELECT against {PROJECT}.{DATASET}. Fully qualify tables as `{PROJECT}.{DATASET}.view`. A LIMIT of {MAX_ROWS} is applied if none is given.")) -> str:
    """Run a read-only SQL SELECT against the Walmart BI Link dataset. Use this when no curated tool fits. Aggregate in SQL; do not pull raw store-item-day rows."""
    return fmt(run_sql(guard_sql(sql)))


@tool("scintilla_find_items")
def find_items(search: str = Field(description="Text to match against item name or UPC, e.g. 'sandalwood body wash' or '87863901873'. Space-separated words are ANDed."), active_only: bool = True) -> str:
    """Look up Walmart item numbers, UPCs, names, brand and category for items matching a search string."""
    words = [w for w in re.split(r"\s+", search.strip()) if w]
    if not words:
        raise ValueError("Provide a search string.")
    conds, params = [], {}
    for i, w in enumerate(words):
        params[f"w{i}"] = f"%{w.upper()}%"
        conds.append(f"(UPPER(item_name) LIKE @w{i} OR CAST(wm_upc_nbr AS STRING) LIKE @w{i} OR CAST(wm_item_nbr AS STRING) LIKE @w{i})")
    where = " AND ".join(conds) + (" AND item_status_desc='ACTIVE'" if active_only else "")
    if VENDOR_NBRS:
        where += " AND vendor_nbr IN (" + ",".join(str(v) for v in VENDOR_NBRS) + ")"
    sql = f"""SELECT wm_item_nbr, item_name, wm_upc_nbr, brand_nm, omni_catg_desc, item_status_desc, base_unit_rtl_amt, item_create_dt, vendor_nbr
             FROM {FQ}item_dim` WHERE {where} ORDER BY item_status_desc, item_name LIMIT 100"""
    return fmt(run_sql(sql, params))


@tool("scintilla_sales_summary")
def sales_summary(
    weeks: int = Field(13, ge=1, le=52, description="Number of most recent complete Walmart fiscal weeks to include."),
    group_by: str = Field("item", description="One of: item, category, brand, channel, week, region, total."),
    item_filter: Optional[str] = Field(None, description="Optional text to restrict to items whose name contains this (case-insensitive)."),
) -> str:
    """Walmart US store sales for your items, this year vs last year, over the last N fiscal weeks, grouped by item, category, brand, channel, week, region or total. Returns dollars, units, AUR and YoY %."""
    dims = {
        "item": "i.wm_item_nbr, i.item_name",
        "category": "i.omni_catg_desc",
        "brand": "i.brand_nm",
        "channel": "IFNULL(s.svc_chnl_nm,'IN-STORE') AS channel",
        "week": "s.wm_yr_wk_nbr",
        "region": "d.region_nbr",
        "total": "'total' AS grp",
    }
    if group_by not in dims:
        raise ValueError(f"group_by must be one of {list(dims)}")
    sel = dims[group_by]
    grp = re.sub(r"\s+AS\s+\w+", "", sel)
    params: dict[str, Any] = {"weeks": weeks}
    flt = ""
    if item_filter:
        flt = " AND UPPER(i.item_name) LIKE @flt"
        params["flt"] = f"%{item_filter.upper()}%"
    join_d = f"JOIN {FQ}store_dim` d USING(store_nbr)" if group_by == "region" else ""
    group_clause = "" if group_by == "total" else f"GROUP BY {grp}"
    sql = f"""
      WITH wk AS (SELECT DISTINCT wm_yr_wk_nbr FROM {FQ}store_sales` WHERE bus_dt < CURRENT_DATE() ORDER BY 1 DESC LIMIT @weeks)
      SELECT {sel},
             ROUND(SUM(s.ty_sales_amt)) AS ty_sales, ROUND(SUM(s.ly_sales_amt)) AS ly_sales,
             ROUND(SAFE_DIVIDE(SUM(s.ty_sales_amt),SUM(s.ly_sales_amt))-1,3) AS yoy_pct,
             SUM(s.ty_qty) AS ty_units, SUM(s.ly_qty) AS ly_units,
             ROUND(SAFE_DIVIDE(SUM(s.ty_sales_amt),SUM(s.ty_qty)),2) AS ty_aur,
             ROUND(SAFE_DIVIDE(SUM(s.ly_sales_amt),SUM(s.ly_qty)),2) AS ly_aur,
             COUNT(DISTINCT IF(s.ty_qty>0, s.store_nbr, NULL)) AS stores_selling_ty
      FROM {FQ}store_sales` s JOIN {FQ}item_dim` i USING(wm_item_nbr) {join_d}
      WHERE s.wm_yr_wk_nbr IN (SELECT wm_yr_wk_nbr FROM wk) {flt}
      {group_clause} ORDER BY ty_sales DESC NULLS LAST LIMIT {MAX_ROWS}"""
    return fmt(run_sql(sql, params), note=f"Last {weeks} complete fiscal weeks, grouped by {group_by}.")


@tool("scintilla_inventory_snapshot")
def inventory_snapshot(item_filter: Optional[str] = Field(None, description="Optional item name filter (case-insensitive contains).")) -> str:
    """Latest-day store inventory by item: traited stores, on-hand units and retail, on-order, in-transit, replenishment in-stock %, and stores with zero on hand."""
    params: dict[str, Any] = {}
    flt = ""
    if item_filter:
        flt = " AND UPPER(i.item_name) LIKE @flt"
        params["flt"] = f"%{item_filter.upper()}%"
    sql = f"""
      SELECT v.bus_dt, i.wm_item_nbr, i.item_name,
             COUNT(DISTINCT IF(v.ty_traited_cnt>0, v.store_nbr, NULL)) AS traited_stores,
             SUM(v.ty_on_hand_qty) AS on_hand_units, ROUND(SUM(v.ty_on_hand_rtl_amt)) AS on_hand_retail,
             SUM(v.ty_on_order_qty) AS on_order_units, SUM(v.ty_in_trnst_qty) AS in_transit_units,
             ROUND(SAFE_DIVIDE(SUM(v.ty_repl_instock_numerator),SUM(v.ty_repl_instock_denominator))*100,1) AS repl_instock_pct,
             COUNT(DISTINCT IF(v.ty_traited_cnt>0 AND IFNULL(v.ty_on_hand_qty,0)<=0, v.store_nbr, NULL)) AS traited_stores_zero_oh
      FROM {FQ}store_invt` v JOIN {FQ}item_dim` i USING(wm_item_nbr)
      WHERE v.bus_dt = (SELECT MAX(bus_dt) FROM {FQ}store_invt`) {flt}
      GROUP BY 1,2,3 HAVING traited_stores>0 ORDER BY on_hand_retail DESC LIMIT {MAX_ROWS}"""
    return fmt(run_sql(sql, params))


@tool("scintilla_oos_root_cause")
def oos_root_cause(
    days: int = Field(28, ge=1, le=365, description="Look-back window in days."),
    group_by: str = Field("reason", description="One of: reason, item, item_reason, month."),
) -> str:
    """Walmart's out-of-stock root-cause attribution for your items over the last N days: store-item-days by cause (Supplier, Store Ops, Forecast, Modular, Other), optionally by item or month."""
    dims = {
        "reason": "o.hi_lvl_oos_rsn_desc, o.med_lvl_oos_rsn_desc, o.low_lvl_oos_rsn_desc",
        "item": "i.wm_item_nbr, i.item_name",
        "item_reason": "i.item_name, o.hi_lvl_oos_rsn_desc",
        "month": "FORMAT_DATE('%Y-%m', o.bus_dt) AS month, o.hi_lvl_oos_rsn_desc",
    }
    if group_by not in dims:
        raise ValueError(f"group_by must be one of {list(dims)}")
    sel = dims[group_by]
    grp = re.sub(r"\s+AS\s+\w+", "", sel)
    sql = f"""
      SELECT {sel}, COUNT(*) AS store_item_days, COUNT(DISTINCT o.store_nbr) AS stores, COUNT(DISTINCT o.wm_item_nbr) AS items
      FROM {FQ}oosrootcause` o JOIN {FQ}item_dim` i USING(wm_item_nbr)
      WHERE o.bus_dt >= DATE_SUB(CURRENT_DATE(), INTERVAL @days DAY)
      GROUP BY {grp} ORDER BY store_item_days DESC LIMIT {MAX_ROWS}"""
    return fmt(run_sql(sql, {"days": days}), note=f"Last {days} days.")


@tool("scintilla_demand_forecast")
def demand_forecast(
    weeks_ahead: int = Field(4, ge=1, le=6, description="Forecast horizon in weeks from today."),
    item_filter: Optional[str] = Field(None, description="Optional item name filter."),
) -> str:
    """Walmart's store demand forecast for your items, summed across stores by item and week for the next N weeks."""
    params: dict[str, Any] = {"days": weeks_ahead * 7}
    flt = ""
    if item_filter:
        flt = " AND UPPER(i.item_name) LIKE @flt"
        params["flt"] = f"%{item_filter.upper()}%"
    sql = f"""
      SELECT i.wm_item_nbr, i.item_name, DATE_TRUNC(f.fcst_dt, WEEK(SATURDAY)) AS week_start,
             ROUND(SUM(f.fcst_tot_dmand_each_qty)) AS forecast_units, COUNT(DISTINCT f.store_nbr) AS stores
      FROM {FQ}daily_demand_forecast` f JOIN {FQ}item_dim` i USING(wm_item_nbr)
      WHERE f.fcst_dt BETWEEN CURRENT_DATE() AND DATE_ADD(CURRENT_DATE(), INTERVAL @days DAY) {flt}
      GROUP BY 1,2,3 ORDER BY item_name, week_start LIMIT {MAX_ROWS}"""
    return fmt(run_sql(sql, params))


@tool("scintilla_item_store_detail")
def item_store_detail(
    wm_item_nbr: int = Field(description="Walmart item number (from scintilla_find_items)."),
    weeks: int = Field(4, ge=1, le=13),
    limit: int = Field(100, ge=1, le=500),
) -> str:
    """Store-level detail for one item over the last N weeks: sales, units, latest on-hand, in-stock %. Sorted by lowest in-stock first to surface problem stores."""
    sql = f"""
      WITH wk AS (SELECT DISTINCT wm_yr_wk_nbr FROM {FQ}store_sales` WHERE bus_dt < CURRENT_DATE() ORDER BY 1 DESC LIMIT @weeks),
      s AS (SELECT store_nbr, ROUND(SUM(ty_sales_amt)) ty_sales, SUM(ty_qty) ty_units, ROUND(SUM(ly_sales_amt)) ly_sales
            FROM {FQ}store_sales` WHERE wm_item_nbr=@item AND wm_yr_wk_nbr IN (SELECT wm_yr_wk_nbr FROM wk) GROUP BY 1),
      v AS (SELECT store_nbr, ty_on_hand_qty on_hand, ROUND(SAFE_DIVIDE(ty_repl_instock_numerator, ty_repl_instock_denominator)*100,1) instock_pct
            FROM {FQ}store_invt` WHERE wm_item_nbr=@item AND bus_dt=(SELECT MAX(bus_dt) FROM {FQ}store_invt`))
      SELECT d.store_nbr, d.region_nbr, d.state_prov_cd, s.ty_sales, s.ly_sales, s.ty_units, v.on_hand, v.instock_pct
      FROM {FQ}store_dim` d LEFT JOIN s USING(store_nbr) LEFT JOIN v USING(store_nbr)
      WHERE s.store_nbr IS NOT NULL OR v.store_nbr IS NOT NULL
      ORDER BY v.instock_pct NULLS LAST, s.ty_sales DESC LIMIT @lim"""
    return fmt(run_sql(sql, {"item": wm_item_nbr, "weeks": weeks, "lim": limit}, max_rows=limit))


# ---------------------------------------------------------------- HTTP app + auth
TOKEN = os.environ.get("MCP_BEARER_TOKEN", "")


class AuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        path = request.url.path
        if path in ("/", "/health", "/healthz"):
            return await call_next(request)
        # claude.ai probes /.well-known/oauth-* when adding a connector. Answering 401 with a
        # WWW-Authenticate header makes it assume OAuth and attempt dynamic client registration,
        # which this server does not offer. Be explicit: no OAuth metadata here, and only the
        # MCP endpoint exists.
        if path.startswith("/.well-known/") or not (path == "/mcp" or path.startswith("/mcp/")):
            return JSONResponse({"error": "not found"}, status_code=404)
        if not TOKEN:
            return JSONResponse({"error": "server misconfigured: MCP_BEARER_TOKEN not set"}, status_code=500)
        supplied = ""
        auth = request.headers.get("authorization", "")
        if auth.lower().startswith("bearer "):
            supplied = auth[7:].strip()
        else:
            m = re.match(r"^/mcp/([^/]+)/?$", request.url.path)
            if m:
                supplied = m.group(1)
                request.scope["path"] = "/mcp"
                request.scope["raw_path"] = b"/mcp"
        if not supplied or not secrets.compare_digest(supplied, TOKEN):
            return JSONResponse({"error": "unauthorized"}, status_code=401)
        return await call_next(request)


app = mcp.streamable_http_app()
app.add_middleware(AuthMiddleware)

_inner_lifespan = app.router.lifespan_context


@contextlib.asynccontextmanager
async def lifespan(a):
    global _loop
    _loop = asyncio.get_running_loop()
    task = asyncio.create_task(heartbeat_loop()) if (HC_PING_URL or AXIOM_TOKEN) else None
    async with _inner_lifespan(a):
        yield
    if task:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


app.router.lifespan_context = lifespan


async def health(request: Request):
    return PlainTextResponse("ok")

app.add_route("/health", health, methods=["GET"])
app.add_route("/", health, methods=["GET"])

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", "8080")))
