import os
import pytest

os.environ.setdefault("MCP_BEARER_TOKEN", "test-token")
from scintilla_mcp import server  # noqa: E402


@pytest.mark.parametrize("sql", [
    "SELECT 1 FROM `wmt-dv-bi-link-prod.dv_supplier.store_sales`",
    "WITH x AS (SELECT 1) SELECT * FROM x",
    "select wm_item_nbr from `wmt-dv-bi-link-prod.dv_supplier.item_dim` limit 5",
])
def test_guard_accepts_selects(sql):
    out = server.guard_sql(sql)
    assert out.lower().lstrip().startswith(("select", "with"))
    assert "LIMIT" in out.upper()


@pytest.mark.parametrize("sql", [
    "DELETE FROM `wmt-dv-bi-link-prod.dv_supplier.store_sales` WHERE 1=1",
    "SELECT 1; DROP TABLE x",
    "INSERT INTO t VALUES (1)",
    "SELECT * FROM `bigquery-public-data.samples.shakespeare`",
    "SELECT * FROM `wmt-dv-bi-link-prod.other_dataset.t`",
    "CREATE TABLE t AS SELECT 1",
])
def test_guard_rejects(sql):
    with pytest.raises(ValueError):
        server.guard_sql(sql)


def test_guard_preserves_explicit_limit():
    out = server.guard_sql("SELECT 1 FROM `wmt-dv-bi-link-prod.dv_supplier.store_sales` LIMIT 7")
    assert out.count("LIMIT") == 1
