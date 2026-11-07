"""Batch transform: bronze -> silver (incremental MERGE) -> gold (aggregates).

Silver is incremental. lake.ops.watermarks remembers, per model and Kafka partition, the
highest bronze offset already merged. Each run:
  1. finds the current high mark per partition among bronze rows above the watermark
  2. MERGEs exactly the range (watermark, high mark] into silver
  3. advances the watermark in the SAME transaction, so a crash can't skip or double-apply
Rows that land in bronze while this runs sit above the high mark and wait for the next run.
Replayed duplicates (at-least-once ingest) fall at or below the watermark, or are deduped
within the range.
"""

import logging
import time

import duckdb

from lakehouse.connection import SQL_DIR

logger = logging.getLogger(__name__)

# Order matters: silver before gold, and gold reads only silver.
SILVER = ["users", "orders", "order_items"]  # each reads lake.bronze.<same name>
GOLD = ["daily_revenue", "customer_summary", "product_sales", "order_status_funnel"]

WATERMARKS_DDL = """
CREATE TABLE IF NOT EXISTS lake.ops.watermarks (
    model            VARCHAR,
    kafka_partition  INTEGER,
    max_offset       BIGINT,
    updated_at       TIMESTAMPTZ
)"""


def _above_watermark(wm: dict[int, int]) -> str:
    """Predicate: bronze rows not merged yet (also matches partitions we've never seen)."""
    if not wm:
        return "TRUE"
    parts = [f"(kafka_partition = {p} AND kafka_offset > {o})" for p, o in sorted(wm.items())]
    parts.append(f"kafka_partition NOT IN ({', '.join(str(p) for p in sorted(wm))})")
    return "(" + " OR ".join(parts) + ")"


def _offset_range(wm: dict[int, int], hi: dict[int, int]) -> str:
    """Predicate: exactly the rows (watermark, high mark] per partition."""
    return "(" + " OR ".join(
        f"(kafka_partition = {p} AND kafka_offset > {wm.get(p, -1)} AND kafka_offset <= {h})"
        for p, h in sorted(hi.items())
    ) + ")"


def run_silver_model(con: duckdb.DuckDBPyConnection, model: str) -> int:
    """Incrementally merge new bronze events into silver. Returns events processed."""
    ddl, merge = (SQL_DIR / "silver" / f"{model}.sql").read_text().split("-- ==== MERGE ====")
    con.execute(ddl)

    wm = dict(con.execute(
        "SELECT kafka_partition, max_offset FROM lake.ops.watermarks WHERE model = ?", [model]
    ).fetchall())
    new = con.execute(
        f"SELECT kafka_partition, max(kafka_offset), count(*) FROM lake.bronze.{model} "
        f"WHERE {_above_watermark(wm)} GROUP BY 1"
    ).fetchall()
    if not new:
        return 0
    hi = {p: h for p, h, _ in new}
    n_events = sum(n for _, _, n in new)

    con.execute("BEGIN")
    try:
        con.execute(merge.replace("{new_events}", _offset_range(wm, hi)))
        con.execute("DELETE FROM lake.ops.watermarks WHERE model = ?", [model])
        merged = {**wm, **hi}
        con.executemany(
            "INSERT INTO lake.ops.watermarks VALUES (?, ?, ?, now())",
            [[model, p, o] for p, o in sorted(merged.items())],
        )
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    return n_events


def full_refresh(con: duckdb.DuckDBPyConnection) -> None:
    """Throw silver away and rebuild it from bronze. This is why bronze keeps every raw event."""
    for model in SILVER:
        con.execute(f"DROP TABLE IF EXISTS lake.silver.{model}")
    con.execute("DROP TABLE IF EXISTS lake.ops.watermarks")
    logger.info("Full refresh: dropped silver tables and watermarks; rebuilding from bronze.")


def run_transform(con: duckdb.DuckDBPyConnection, refresh: bool = False) -> dict[str, float]:
    if refresh:
        full_refresh(con)
    con.execute(WATERMARKS_DDL)
    timings = {}
    for model in SILVER:
        start = time.monotonic()
        n_new = run_silver_model(con, model)
        timings[f"silver.{model}"] = dt = time.monotonic() - start
        rows = con.execute(f"SELECT count(*) FROM lake.silver.{model}").fetchone()[0]
        logger.info("silver %-20s %9d rows  %8d new events  (%.2fs)", model, rows, n_new, dt)
    for model in GOLD:
        start = time.monotonic()
        con.execute((SQL_DIR / "gold" / f"{model}.sql").read_text())
        timings[f"gold.{model}"] = dt = time.monotonic() - start
        rows = con.execute(f"SELECT count(*) FROM lake.gold.{model}").fetchone()[0]
        logger.info("gold   %-20s %9d rows  (%.2fs)", model, rows, dt)
    return timings
