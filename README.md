# LakeRun: local CDC lakehouse (Postgres → Debezium → Kafka → Iceberg + DuckDB)

```
Postgres ──WAL──▶ Debezium ──▶ Kafka ──▶ ingestor ──▶ BRONZE ──MERGE──▶ SILVER ──agg──▶ GOLD
 (OLTP)           (CDC)       (log)     (Python)     raw events       current state    analytics
                                                   └────────── Iceberg tables on MinIO, via DuckDB ──────────┘
```

## Run it

```bash
cd docker-compose && docker compose up -d && cd ..
# register the CDC connector (first time only; to change config use PUT .../connectors/shop-connector/config)
curl -X POST -H 'Content-Type: application/json' --data @docker-compose/connector.json localhost:8083/connectors

pipenv install
pipenv run python main.py simulate     # fake shop activity in Postgres (run it again any time)
pipenv run python main.py run          # Kafka -> bronze, then bronze -> silver -> gold
pipenv run python main.py status       # row counts per layer
pipenv run python main.py sql "SELECT * FROM lake.gold.customer_summary"
pipenv run python main.py shell        # interactive SQL
pipenv run python main.py ingest       # streaming mode: keep consuming (Ctrl+C to stop)
pipenv run python main.py ingest --workers 6 --batch-size 10000   # parallel consumers (<= partitions)
pipenv run python main.py transform --full-refresh               # rebuild silver from all of bronze
pipenv run python scripts/load_test.py --rounds 10 --workers 6 --batch-size 10000   # 1M-event load test
```

DuckDB CLI (`brew install duckdb`): `duckdb -init lakehouse/sql/00_setup.sql`

UIs: Kafka UI http://localhost:8080 · MinIO console http://localhost:9001 (admin / supersecurepassword123)

## Layout

| Path | What |
|---|---|
| `docker-compose/` | Postgres, Kafka (KRaft), Debezium Connect, MinIO (+ bucket init), Iceberg REST catalog |
| `ingestor/ingestor.py` | Kafka consumer → `lake.bronze.*` (batched, atomic commit, at-least-once) |
| `lakehouse/sql/00_setup.sql` | DuckDB ↔ MinIO/Iceberg wiring + `dbz_decimal` macro |
| `lakehouse/sql/silver/*.sql` | Incremental: new bronze range only (watermarks in `lake.ops.watermarks`), latest event per key by LSN, then `MERGE` |
| `lakehouse/sql/gold/*.sql` | Business aggregates rebuilt from silver |
| `scripts/simulate_activity.sql` | Inserts/updates/deletes to exercise CDC |

## Try these

```sql
-- every change ever captured for one order (bronze = history)
SELECT op, after::JSON->>'status' AS status, source_ts FROM lake.bronze.orders WHERE record_key = '{"id":1}';
-- Iceberg snapshots and time travel
SELECT * FROM iceberg_snapshots(lake.silver.orders);
SELECT count(*) FROM lake.silver.orders AT (VERSION => <snapshot_id>);
```

Topics have 6 partitions (`KAFKA_NUM_PARTITIONS`), so up to 6 ingestor workers run in parallel. Their Iceberg commits are serialized with a local file lock (`.lakerun/commit.lock`), because concurrent DuckDB appends to one Iceberg table were shown to lose commits.

See `docs/LakeRun_Lakehouse_Guide.pdf` for the concepts, the design and interview prep.
