"""LakeRun CLI.

    python main.py simulate        # generate inserts/updates/deletes in Postgres
    python main.py ingest          # Kafka -> bronze, runs forever (Ctrl+C to stop)
    python main.py ingest --once   # Kafka -> bronze, stop when the topics are drained
    python main.py ingest --workers 6 --batch-size 10000   # parallel consumers (<= partitions)
    python main.py transform       # bronze -> silver (incremental) -> gold
    python main.py transform --full-refresh   # rebuild silver from all of bronze
    python main.py run             # ingest --once + transform (one full batch cycle)
    python main.py sql "SELECT * FROM lake.gold.daily_revenue"
    python main.py shell           # interactive SQL prompt over the lake
    python main.py status          # row counts for every table in every layer
"""

import argparse
import logging
import subprocess
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logging.getLogger("kafka").setLevel(logging.ERROR)
logger = logging.getLogger("lakerun")

ROOT = Path(__file__).parent


def cmd_ingest(once: bool, batch_size: int = 1000, workers: int = 1) -> None:
    from ingestor.ingestor import run_workers

    try:
        run_workers(workers=workers, once=once, batch_size=batch_size)
    except KeyboardInterrupt:
        logger.info("Stopped.")


def cmd_transform(full_refresh: bool = False) -> None:
    from lakehouse.connection import connect
    from lakehouse.transform import run_transform

    run_transform(connect(), refresh=full_refresh)


def cmd_sql(query: str) -> None:
    from lakehouse.connection import connect

    connect().sql(query).show(max_rows=100, max_width=200)


def cmd_shell() -> None:
    from lakehouse.connection import connect

    con = connect()
    print("LakeRun SQL shell. Tables live in lake.bronze.*, lake.silver.*, lake.gold.*")
    print("End statements with ';'. Type 'exit' to quit.\n")
    buf = ""
    while True:
        try:
            line = input("lake> " if not buf else "  ... ")
        except (EOFError, KeyboardInterrupt):
            print()
            return
        if not buf and line.strip().lower() in ("exit", "quit", ".quit"):
            return
        buf += line + "\n"
        if buf.strip().endswith(";"):
            try:
                rel = con.sql(buf)
                if rel is not None:
                    rel.show(max_rows=100, max_width=200)
            except Exception as e:  # keep the shell alive on bad SQL
                print(f"Error: {e}")
            buf = ""


def cmd_status() -> None:
    from lakehouse.connection import connect

    con = connect()
    tables = con.execute(
        "SELECT schema_name, table_name FROM duckdb_tables() WHERE database_name = 'lake' ORDER BY 1, 2"
    ).fetchall()
    for schema, table in tables:
        n = con.execute(f"SELECT count(*) FROM lake.{schema}.{table}").fetchone()[0]
        print(f"  lake.{schema:<7}.{table:<22} {n:>7} rows")


def cmd_simulate() -> None:
    sql = (ROOT / "scripts" / "simulate_activity.sql").read_text()
    subprocess.run(
        ["docker", "exec", "-i", "lakesync-postgres", "psql", "-U", "postgres", "-d", "shop"],
        input=sql, text=True, check=True,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="LakeRun: Postgres CDC -> Iceberg lakehouse with DuckDB")
    sub = parser.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("ingest", help="Kafka -> bronze")
    p.add_argument("--once", action="store_true", help="stop when topics are drained")
    p.add_argument("--batch-size", type=int, default=1000, help="rows per Iceberg commit")
    p.add_argument("--workers", type=int, default=1, help="parallel consumer processes (useful up to the partition count)")
    p = sub.add_parser("transform", help="bronze -> silver -> gold")
    p.add_argument("--full-refresh", action="store_true", help="drop silver + watermarks and rebuild from bronze")
    p = sub.add_parser("run", help="ingest --once, then transform")
    p.add_argument("--workers", type=int, default=1)
    p.add_argument("--batch-size", type=int, default=1000)
    p = sub.add_parser("sql", help="run one SQL query")
    p.add_argument("query")
    sub.add_parser("shell", help="interactive SQL shell")
    sub.add_parser("status", help="row counts per layer")
    sub.add_parser("simulate", help="generate activity in Postgres")
    args = parser.parse_args()

    if args.cmd == "ingest":
        cmd_ingest(args.once, args.batch_size, args.workers)
    elif args.cmd == "transform":
        cmd_transform(args.full_refresh)
    elif args.cmd == "run":
        cmd_ingest(once=True, batch_size=args.batch_size, workers=args.workers)
        cmd_transform()
    elif args.cmd == "sql":
        cmd_sql(args.query)
    elif args.cmd == "shell":
        cmd_shell()
    elif args.cmd == "status":
        cmd_status()
    elif args.cmd == "simulate":
        cmd_simulate()


if __name__ == "__main__":
    sys.exit(main())
