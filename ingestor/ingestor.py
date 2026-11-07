"""Kafka (Debezium CDC topics) -> Iceberg bronze tables, written through DuckDB.

Delivery is at-least-once: Kafka offsets are committed only after the batch is committed
to Iceberg. If a process dies between those two steps, the batch is written again on
restart; silver handles that, because it dedupes by primary key and source LSN.

Scaling out: run several ingestors in the same consumer group (`--workers N`). Kafka gives
each partition to exactly one member, so throughput grows with min(workers, partitions).
Workers parse in parallel, and only the short Iceberg commit is serialized (see
lakehouse.connection.commit_lock for why).
"""

import json
import logging
import multiprocessing as mp
import os
import time
from collections import defaultdict
from datetime import datetime, timezone

import pyarrow as pa
from kafka import ConsumerRebalanceListener, KafkaConsumer  # pyright: ignore[reportMissingModuleSource]
from kafka.errors import CommitFailedError  # pyright: ignore[reportMissingModuleSource]

from lakehouse.connection import SQL_DIR, SOURCE_TABLES, commit_lock, connect

logger = logging.getLogger(__name__)

BRONZE_SCHEMA = pa.schema(
    [
        ("kafka_topic", pa.string()),
        ("kafka_partition", pa.int32()),
        ("kafka_offset", pa.int64()),
        ("kafka_timestamp", pa.timestamp("us", tz="UTC")),
        ("record_key", pa.string()),
        ("op", pa.string()),
        ("before", pa.string()),
        ("after", pa.string()),
        ("source_lsn", pa.int64()),
        ("source_ts", pa.timestamp("us", tz="UTC")),
        ("ingested_at", pa.timestamp("us", tz="UTC")),
    ]
)

IDLE_POLLS_BEFORE_EXIT = 3  # --once: stop after this many empty polls in a row (~6 s)
COMMIT_RETRIES = 5


def _ms_to_dt(ms: int | None) -> datetime | None:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc) if ms is not None else None


def _dump(obj) -> str | None:
    return json.dumps(obj, separators=(",", ":")) if obj is not None else None


def ensure_bronze_tables(tables: list[str] = SOURCE_TABLES) -> None:
    """Create bronze tables once, before any workers start (avoids DDL races)."""
    con = connect()
    ddl = (SQL_DIR / "bronze_table.sql").read_text()
    with commit_lock():
        for table in tables:
            con.execute(ddl.replace("{table}", table))
    con.close()


class _FlushOnRevoke(ConsumerRebalanceListener):
    """When Kafka moves partitions to another worker, write what we buffered and commit
    offsets first. Otherwise the new owner would re-read those events (more duplicates)
    and our later commit for partitions we no longer own would fail."""

    def __init__(self, ingestor: "KafkaIngestor"):
        self.ingestor = ingestor

    def on_partitions_revoked(self, revoked):
        if revoked:
            self.ingestor._flush()

    def on_partitions_assigned(self, assigned):
        logger.info("Assigned %d partitions: %s", len(assigned),
                    sorted(f"{tp.topic.split('.')[-1]}[{tp.partition}]" for tp in assigned))


class KafkaIngestor:
    def __init__(
        self,
        tables: list[str] = SOURCE_TABLES,
        bootstrap_servers: str = "localhost:29092",
        topic_prefix: str = "shop.public",
        group_id: str = "lakerun-bronze",
        batch_size: int = 1000,
        flush_interval_s: float = 10.0,
    ):
        self.topic_to_table = {f"{topic_prefix}.{t}": t for t in tables}
        self.batch_size = batch_size
        self.flush_interval_s = flush_interval_s
        self.running = True
        self.buffer: dict[str, list[dict]] = defaultdict(list)
        self.last_flush = time.monotonic()

        self.con = connect()
        self.consumer = KafkaConsumer(
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,  # committed offsets live in Kafka under this group
            auto_offset_reset="earliest",  # first run: start from the Debezium snapshot
            enable_auto_commit=False,  # we commit only after the lake write succeeds
            max_partition_fetch_bytes=8 * 1024 * 1024,
        )
        self.consumer.subscribe(topics=list(self.topic_to_table), listener=_FlushOnRevoke(self))

    def _to_row(self, msg) -> dict | None:
        if msg.value is None:
            # Debezium sends a "tombstone" (null value) after each delete so Kafka log
            # compaction can drop the key. The delete event itself came just before it.
            return None
        event = json.loads(msg.value)
        source = event.get("source") or {}
        return {
            "kafka_topic": msg.topic,
            "kafka_partition": msg.partition,
            "kafka_offset": msg.offset,
            "kafka_timestamp": _ms_to_dt(msg.timestamp),
            "record_key": msg.key.decode() if msg.key else None,
            "op": event.get("op"),
            "before": _dump(event.get("before")),
            "after": _dump(event.get("after")),
            "source_lsn": source.get("lsn"),
            "source_ts": _ms_to_dt(source.get("ts_ms")),
            "ingested_at": datetime.now(timezone.utc),
        }

    def _write_batches(self, batches: dict[str, pa.Table]) -> None:
        # One DuckDB transaction = one atomic Iceberg commit across all bronze tables.
        self.con.execute("BEGIN")
        try:
            for table, batch in batches.items():
                self.con.register("batch", batch)
                self.con.execute(f"INSERT INTO lake.bronze.{table} SELECT * FROM batch")
                self.con.unregister("batch")
            self.con.execute("COMMIT")
        except Exception:
            self.con.execute("ROLLBACK")
            raise

    def _flush(self) -> None:
        rows = {t: r for t, r in self.buffer.items() if r}
        if not rows:
            return
        # CPU work happens outside the lock, so workers do it in parallel.
        batches = {t: pa.Table.from_pylist(r, schema=BRONZE_SCHEMA) for t, r in rows.items()}
        for attempt in range(1, COMMIT_RETRIES + 1):
            try:
                with commit_lock():
                    self._write_batches(batches)
                break
            except Exception as e:
                # 409 = another writer changed the table metadata first (optimistic concurrency)
                if "Conflict" not in str(e) or attempt == COMMIT_RETRIES:
                    raise
                logger.warning("Iceberg commit conflict, retry %d/%d", attempt, COMMIT_RETRIES)
                time.sleep(0.2 * attempt)
        try:
            self.consumer.commit()
        except CommitFailedError as e:
            # Lost the partitions mid-flight. Data is safely in bronze; the new owner will
            # re-read these events and silver dedupes them.
            logger.warning("Kafka offset commit failed after rebalance (will replay): %s", e)
        logger.info("Flushed to bronze: %s", {t: len(r) for t, r in rows.items()})
        self.buffer.clear()
        self.last_flush = time.monotonic()

    def run(self, once: bool = False) -> None:
        """Consume forever. With once=True, stop when there is nothing left to read."""
        idle_polls = 0
        try:
            while self.running:
                polled = self.consumer.poll(timeout_ms=2000, max_records=self.batch_size)
                for records in polled.values():
                    for msg in records:
                        row = self._to_row(msg)
                        if row:
                            self.buffer[self.topic_to_table[msg.topic]].append(row)
                idle_polls = 0 if polled else idle_polls + 1

                buffered = sum(len(r) for r in self.buffer.values())
                if (
                    buffered >= self.batch_size
                    or (buffered and time.monotonic() - self.last_flush >= self.flush_interval_s)
                    or (once and not polled)
                ):
                    self._flush()

                if once and idle_polls >= IDLE_POLLS_BEFORE_EXIT:
                    logger.info("Nothing left to read, stopping (--once).")
                    break
        except KeyboardInterrupt:
            logger.info("Interrupted, flushing what's buffered.")
            self._flush()
        finally:
            self.close()

    def close(self) -> None:
        self.consumer.close()
        self.con.close()
        logger.info("Kafka consumer closed.")


def _worker(once: bool, batch_size: int) -> None:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s [%(processName)s] %(name)s: %(message)s")
    logging.getLogger("kafka").setLevel(logging.ERROR)
    try:
        KafkaIngestor(batch_size=batch_size).run(once=once)
    except KeyboardInterrupt:
        pass


def run_workers(workers: int, once: bool, batch_size: int) -> None:
    """Run `workers` ingestor processes in one consumer group.

    Parallelism is capped by the partition count: with 6 partitions per topic, a 7th
    worker would just sit idle as a standby.
    """
    ensure_bronze_tables()
    if workers == 1:
        _worker(once, batch_size)
        return
    procs = [mp.Process(target=_worker, args=(once, batch_size), name=f"ingest-{i}") for i in range(workers)]
    for p in procs:
        p.start()
    try:
        for p in procs:
            p.join()
    except KeyboardInterrupt:
        for p in procs:
            p.join()
    failed = [p.name for p in procs if p.exitcode not in (0, None)]
    if failed:
        raise SystemExit(f"Ingestor workers failed: {failed} (pid {os.getpid()})")
