"""DuckDB connection wired to the Iceberg lakehouse (MinIO + REST catalog)."""

import fcntl
from contextlib import contextmanager
from pathlib import Path

import duckdb

SQL_DIR = Path(__file__).parent / "sql"
LOCK_FILE = Path(__file__).resolve().parent.parent / ".lakerun" / "commit.lock"

# Source tables captured by Debezium (see docker-compose/connector.json)
SOURCE_TABLES = ["users", "orders", "order_items"]
# ops holds pipeline bookkeeping (watermarks), not business data
LAYERS = ["bronze", "silver", "gold", "ops"]


def run_sql_file(con: duckdb.DuckDBPyConnection, path: Path) -> None:
    con.execute(path.read_text())


def connect() -> duckdb.DuckDBPyConnection:
    """In-memory DuckDB that has the Iceberg catalog attached as `lake`."""
    con = duckdb.connect()
    run_sql_file(con, SQL_DIR / "00_setup.sql")
    for layer in LAYERS:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS lake.{layer}")
    return con


@contextmanager
def commit_lock():
    """Serialize Iceberg commits across local processes.

    Measured on DuckDB 2.0 with the Iceberg REST fixture: when several processes append
    to the same Iceberg table concurrently, some commits report success but their
    snapshot is lost (last writer wins). Holding this lock only around the short
    write+commit keeps parallel ingestors safe, while the expensive work (Kafka fetch,
    JSON parsing, Arrow building) still runs in parallel.

    A file lock only works on one machine. Across machines you'd use an engine whose
    Iceberg commits do proper optimistic concurrency with retries (Spark, Flink, the Kafka
    Connect Iceberg sink), or a distributed lock such as a Postgres advisory lock.
    """
    LOCK_FILE.parent.mkdir(exist_ok=True)
    with open(LOCK_FILE, "w") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(f, fcntl.LOCK_UN)
