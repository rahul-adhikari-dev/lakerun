"""1M-event load test: Postgres -> Debezium -> Kafka -> bronze -> silver -> gold.

    pipenv run python scripts/load_test.py [--rounds 10] [--batch-size 1000]

Each round writes exactly 100,000 change events to Postgres:
    10k user INSERTs + 30k order INSERTs + 40k order_item INSERTs + 20k order UPDATEs
The script times every stage separately and checks that silver matches Postgres.
(Load-test order totals are random and don't equal the sum of their items.)
"""

import argparse
import json
import re
import resource
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from kafka import KafkaConsumer, TopicPartition  # noqa: E402

TOPICS = {"users": "shop.public.users", "orders": "shop.public.orders", "order_items": "shop.public.order_items"}
PER_ROUND = {"users": 10_000, "orders": 30_000 + 20_000, "order_items": 40_000}

ROUND_SQL = """
INSERT INTO users (email, name)
SELECT 'load{r}_' || g || '_' || substr(md5(random()::text), 1, 6) || '@load.test', 'Load User ' || g
FROM generate_series(1, 10000) g;

INSERT INTO orders (user_id, status, total)
SELECT 1 + floor(random() * m.mx)::bigint, 'pending', round((10 + random() * 2000)::numeric, 2)
FROM generate_series(1, 30000) g, (SELECT max(id) AS mx FROM users) m;

INSERT INTO order_items (order_id, sku, quantity, price)
SELECT m.mx - floor(random() * 30000)::bigint,
       'SKU-' || (1 + floor(random() * 50)::int) * 10,
       1 + floor(random() * 4)::int,
       round((5 + random() * 500)::numeric, 2)
FROM generate_series(1, 40000) g, (SELECT max(id) AS mx FROM orders) m;

UPDATE orders SET status = 'paid', updated_at = now()
WHERE id IN (SELECT id FROM orders WHERE status = 'pending'
             AND id > (SELECT max(id) - 30000 FROM orders) LIMIT 20000);
"""

CHECKSUM_PG = """
select 'users', count(*), md5(string_agg(id||email||name, ',' order by id)) from users
union all select 'orders', count(*), md5(string_agg(id||'|'||user_id||'|'||status||'|'||total, ',' order by id)) from orders
union all select 'items', count(*), md5(string_agg(id||'|'||order_id||'|'||sku||'|'||quantity||'|'||price, ',' order by id)) from order_items
"""
CHECKSUM_LAKE = """
select 'users', count(*), md5(string_agg(user_id||email||name, ',' order by user_id)) from lake.silver.users
union all select 'orders', count(*), md5(string_agg(order_id||'|'||user_id||'|'||status||'|'||total, ',' order by order_id)) from lake.silver.orders
union all select 'items', count(*), md5(string_agg(order_item_id||'|'||order_id||'|'||sku||'|'||quantity||'|'||price, ',' order by order_item_id)) from lake.silver.order_items
"""


def psql(sql: str, *flags: str) -> str:
    return subprocess.run(
        ["docker", "exec", "-i", "lakesync-postgres", "psql", "-U", "postgres", "-d", "shop", "-v", "ON_ERROR_STOP=1", *flags],
        input=sql, text=True, check=True, capture_output=True,
    ).stdout


def end_offsets(consumer: KafkaConsumer) -> dict[str, int]:
    out = {}
    for table, topic in TOPICS.items():
        tps = [TopicPartition(topic, p) for p in consumer.partitions_for_topic(topic) or []]
        out[table] = sum(consumer.end_offsets(tps).values())
    return out


def slot_lag() -> str:
    return psql("select pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) "
                "from pg_replication_slots where slot_name='lakesync_slot'", "-At").strip()


def section(title: str) -> None:
    print(f"\n=== {title} " + "=" * (60 - len(title)), flush=True)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rounds", type=int, default=10, help="100k events per round")
    ap.add_argument("--batch-size", type=int, default=1000)
    ap.add_argument("--workers", type=int, default=1, help="parallel ingestor processes")
    args = ap.parse_args()
    results: dict = {"rounds": args.rounds, "batch_size": args.batch_size, "workers": args.workers}
    total_events = args.rounds * 100_000

    kc = KafkaConsumer(bootstrap_servers="localhost:29092")
    base = end_offsets(kc)
    expected = {t: base[t] + n * args.rounds for t, n in PER_ROUND.items()}

    # ---- Stage 1: application writes -------------------------------------------------
    section(f"1. Postgres: writing {total_events:,} change events")
    t0 = time.monotonic()
    for r in range(args.rounds):
        psql(ROUND_SQL.format(r=r))
        print(f"  round {r + 1}/{args.rounds} done  ({time.monotonic() - t0:6.1f}s, slot lag {slot_lag()})", flush=True)
    results["pg_write_s"] = round(time.monotonic() - t0, 1)
    results["slot_lag_after_writes"] = slot_lag()

    # ---- Stage 2: Debezium -> Kafka ---------------------------------------------------
    section("2. Debezium -> Kafka: waiting for every event to land")
    samples = []
    while True:
        cur = end_offsets(kc)
        done = sum(cur[t] - base[t] for t in TOPICS)
        samples.append((time.monotonic() - t0, done))
        print(f"  t={samples[-1][0]:6.1f}s  in Kafka: {done:>9,}/{total_events:,}   slot lag {slot_lag()}", flush=True)
        if all(cur[t] >= expected[t] for t in TOPICS):
            break
        time.sleep(3)
    results["cdc_total_s"] = round(time.monotonic() - t0, 1)  # from first write to last event in Kafka
    results["cdc_events_per_s"] = round(total_events / results["cdc_total_s"])
    kc.close()

    # ---- Stage 3: Kafka -> bronze ------------------------------------------------------
    section(f"3. Ingestor: Kafka -> bronze ({args.workers} worker(s), batch size {args.batch_size})")
    t1 = time.monotonic()
    proc = subprocess.run(
        ["/usr/bin/time", "-l", sys.executable, "main.py", "ingest", "--once",
         "--batch-size", str(args.batch_size), "--workers", str(args.workers)],
        cwd=ROOT, text=True, capture_output=True,
    )
    results["ingest_s"] = round(time.monotonic() - t1, 1)
    flushes = [l for l in proc.stderr.splitlines() if "Flushed to bronze" in l]
    # /usr/bin/time reports the parent process; with workers, that's only the launcher.
    rss = re.search(r"(\d+)\s+maximum resident set size", proc.stderr) if args.workers == 1 else None
    results["ingest_commits"] = len(flushes)
    results["ingest_events_per_s"] = round(total_events / results["ingest_s"])
    results["ingest_peak_rss_mb"] = round(int(rss.group(1)) / 1e6) if rss else None
    print(f"  {len(flushes)} Iceberg commits in {results['ingest_s']}s "
          f"-> {results['ingest_events_per_s']:,} events/s, peak RSS {results['ingest_peak_rss_mb']} MB", flush=True)
    if proc.returncode:
        print(proc.stderr[-3000:]); sys.exit(1)

    # ---- Stage 4: silver (incremental) + gold -------------------------------------------
    import logging

    from lakehouse.connection import connect
    from lakehouse.transform import run_silver_model, run_transform

    logging.basicConfig(level=logging.INFO, format="  %(message)s")
    con = connect()
    section("4. Transform: bronze -> silver (incremental) -> gold")
    results["transform_s"] = {k: round(v, 2) for k, v in run_transform(con).items()}

    t = time.monotonic()
    for m in ("users", "orders", "order_items"):
        run_silver_model(con, m)
    results["silver_noop_rerun_s"] = round(time.monotonic() - t, 2)
    print(f"  re-run all silver (no new data): {results['silver_noop_rerun_s']}s", flush=True)

    # ---- Stage 5: storage shape + queries ----------------------------------------------
    section("5. Storage and query latency")
    results["tables"] = {}
    for layer, table in [("bronze", t) for t in TOPICS] + [("silver", t) for t in TOPICS]:
        snaps = con.execute(f"select count(*) from iceberg_snapshots(lake.{layer}.{table})").fetchone()[0]
        try:
            meta = con.execute(f"select count(*) from iceberg_metadata(lake.{layer}.{table}) where status <> 'DELETED'").fetchone()[0]
        except Exception:
            meta = None
        rows = con.execute(f"select count(*) from lake.{layer}.{table}").fetchone()[0]
        results["tables"][f"{layer}.{table}"] = {"rows": rows, "snapshots": snaps, "files": meta}
        print(f"  {layer}.{table:<12} {rows:>9,} rows  {snaps:>5} snapshots  {meta} files", flush=True)

    queries = {
        "gold daily_revenue (full)": "select * from lake.gold.daily_revenue",
        "gold top 10 customers": "select * from lake.gold.customer_summary order by lifetime_value desc limit 10",
        "silver revenue by status": "select status, count(*), sum(total) from lake.silver.orders group by 1",
        "bronze history of 1 order": "select op, after from lake.bronze.orders where record_key = '{\"id\":5000}'",
    }
    results["queries_s"] = {}
    for name, q in queries.items():
        t = time.monotonic()
        con.execute(q).fetchall()
        results["queries_s"][name] = round(time.monotonic() - t, 3)
        print(f"  {name:<30} {results['queries_s'][name]:.3f}s", flush=True)

    # ---- Stage 6: correctness ----------------------------------------------------------
    section("6. Correctness: silver vs Postgres checksums")
    pg = sorted(tuple(l.split("|")) for l in psql(CHECKSUM_PG, "-At").strip().splitlines())
    lake = sorted(tuple(str(x) for x in r) for r in con.execute(CHECKSUM_LAKE).fetchall())
    results["checksums_match"] = pg == lake
    for a, b in zip(pg, lake):
        print(f"  {a[0]:<6} pg={a[1]:>9} lake={b[1]:>9}  {'MATCH' if a == b else 'MISMATCH'}")

    out = ROOT / "docs" / f"load_test_results_{args.rounds * 100}k_batch{args.batch_size}_w{args.workers}.json"
    out.write_text(json.dumps(results, indent=2))
    print(f"\nResults written to {out}")


if __name__ == "__main__":
    main()
