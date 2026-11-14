"""PostgreSQL side: schema bootstrap, the load log, and the transactional COPY of one file."""

from __future__ import annotations

import io
from collections.abc import Iterator
from datetime import date, datetime
from pathlib import Path

import psycopg
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv
import pyarrow.parquet as pq
from psycopg import sql

from .datasets import ARROW_TYPES, Dataset

SCHEMA = "raw"
BATCH_ROWS = 250_000

LOAD_LOG_DDL = """
create table if not exists raw.load_log (
    load_id        bigint generated always as identity primary key,
    dataset        text        not null,
    period         date,                    -- null for non-monthly files
    source_url     text        not null,
    source_version text        not null,    -- ETag of the file that was loaded
    source_bytes   bigint,
    source_rows    bigint,                  -- rows according to the file itself
    loaded_rows    bigint,                  -- rows COPY reported
    loaded_at      timestamptz,             -- value stamped into the rows' _loaded_at
    status         text        not null check (status in ('running', 'succeeded', 'failed')),
    started_at     timestamptz not null default now(),
    finished_at    timestamptz,
    error          text
);
create index if not exists load_log_lookup on raw.load_log (dataset, period, status);
"""


class RowCountMismatch(RuntimeError):
    pass


def connect(conninfo: str = "") -> psycopg.Connection:
    """Connect using libpq defaults, i.e. the standard PG* environment variables."""
    return psycopg.connect(conninfo, autocommit=False)


def bootstrap(conn: psycopg.Connection, datasets: list[Dataset]) -> None:
    with conn.transaction():
        conn.execute(f"create schema if not exists {SCHEMA}")
        conn.execute(LOAD_LOG_DDL)
        for ds in datasets:
            conn.execute(_parent_ddl(ds))


def _parent_ddl(ds: Dataset) -> sql.Composed:
    cols = [
        sql.SQL("{} {}").format(sql.Identifier(name), sql.SQL(pg_type))
        for name, pg_type in ds.columns
    ]
    lineage = [
        sql.SQL("_source_file text not null"),
        sql.SQL("_source_row bigint not null"),  # 1-based row number within the file
        sql.SQL("_loaded_at timestamptz not null"),
    ]
    if ds.monthly:
        lineage.insert(0, sql.SQL("source_month date not null"))
        tail = sql.SQL(" partition by list (source_month)")
    else:
        tail = sql.SQL("")
    return sql.SQL("create table if not exists {}.{} ({}){}").format(
        sql.Identifier(SCHEMA), sql.Identifier(ds.table), sql.SQL(", ").join(cols + lineage), tail
    )


def last_load(
    conn: psycopg.Connection, ds: Dataset, period: date | None
) -> tuple[str, bool] | None:
    """(version, complete) of the latest successful load of this file, if any.

    `complete` is false for a `--max-rows` sample, so a later full run reloads it.
    """
    row = conn.execute(
        """
        select source_version, loaded_rows = source_rows from raw.load_log
        where dataset = %s and period is not distinct from %s and status = 'succeeded'
        order by load_id desc limit 1
        """,
        (ds.name, period),
    ).fetchone()
    conn.commit()
    return (row[0], row[1]) if row else None


def start_log(
    conn: psycopg.Connection,
    ds: Dataset,
    period: date | None,
    url: str,
    version: str,
    size: int | None,
) -> int:
    # Committed on its own so a failed load still leaves a trace.
    with conn.transaction():
        (load_id,) = conn.execute(
            """
            insert into raw.load_log
                (dataset, period, source_url, source_version, source_bytes, status)
            values (%s, %s, %s, %s, %s, 'running') returning load_id
            """,
            (ds.name, period, url, version, size),
        ).fetchone()
    return load_id


def fail_log(conn: psycopg.Connection, load_id: int, error: str) -> None:
    conn.rollback()
    with conn.transaction():
        conn.execute(
            "update raw.load_log set status = 'failed', finished_at = now(), error = %s "
            "where load_id = %s",
            (error[:4000], load_id),
        )


def load_file(
    conn: psycopg.Connection,
    ds: Dataset,
    period: date | None,
    path: Path,
    load_id: int,
    max_rows: int | None = None,
) -> tuple[int, int]:
    """Replace the file's partition (or table) with the file's contents, atomically.

    Returns (rows in the file, rows loaded). With `max_rows` only the first
    rows are loaded; the count check then compares against that limit.
    """
    target = ds.target_table(period)
    loaded_at = datetime.now().astimezone()
    batches, source_rows = _read_batches(ds, path)
    expected = source_rows if max_rows is None else min(source_rows, max_rows)

    with conn.transaction():
        if ds.monthly:
            conn.execute(
                sql.SQL(
                    "create table if not exists {}.{} partition of {}.{} for values in ({})"
                ).format(
                    sql.Identifier(SCHEMA),
                    sql.Identifier(target),
                    sql.Identifier(SCHEMA),
                    sql.Identifier(ds.table),
                    sql.Literal(period),
                )
            )
        # Same transaction as COPY: readers see the old rows or the new ones, never
        # half a file, and with wal_level=minimal the COPY skips WAL.
        conn.execute(
            sql.SQL("truncate {}.{}").format(sql.Identifier(SCHEMA), sql.Identifier(target))
        )

        names = [c for c, _ in ds.columns]
        if ds.monthly:
            names = ["source_month", *names]
        names += ["_source_file", "_source_row", "_loaded_at"]
        copy_sql = sql.SQL("copy {}.{} ({}) from stdin with (format csv)").format(
            sql.Identifier(SCHEMA),
            sql.Identifier(target),
            sql.SQL(", ").join(map(sql.Identifier, names)),
        )

        loaded = 0
        with conn.cursor() as cur, cur.copy(copy_sql) as copy:
            for batch in batches:
                if max_rows is not None:
                    if loaded >= max_rows:
                        break
                    batch = batch.slice(0, max_rows - loaded)
                table = _conform(ds, batch, period, path.name, loaded_at, first_row=loaded + 1)
                buf = io.BytesIO()
                pacsv.write_csv(
                    table, buf, pacsv.WriteOptions(include_header=False, quoting_style="needed")
                )
                copy.write(buf.getvalue())
                loaded += table.num_rows

        if loaded != expected:
            raise RowCountMismatch(f"{path.name}: file has {expected} rows, COPY wrote {loaded}")

        conn.execute(
            "update raw.load_log set status = 'succeeded', finished_at = now(), "
            "source_rows = %s, loaded_rows = %s, loaded_at = %s where load_id = %s",
            (source_rows, loaded, loaded_at, load_id),
        )
    return source_rows, loaded


def _read_batches(ds: Dataset, path: Path) -> tuple[Iterator[pa.RecordBatch], int]:
    if ds.file_format == "parquet":
        pf = pq.ParquetFile(path)
        return pf.iter_batches(batch_size=BATCH_ROWS), pf.metadata.num_rows
    table = pacsv.read_csv(path)
    return iter(table.to_batches(BATCH_ROWS)), table.num_rows


def _conform(
    ds: Dataset,
    batch: pa.RecordBatch,
    period: date | None,
    file_name: str,
    loaded_at: datetime,
    first_row: int = 1,
) -> pa.Table:
    """Map a source batch onto the raw table's columns.

    Source names are matched case-insensitively (TLC has shipped `Airport_fee`
    and `airport_fee`). Columns missing from older files become nulls; columns
    the raw table does not know are dropped. Casts are safe: an overflow fails
    the load instead of wrapping silently.
    """
    by_lower = {name.lower(): i for i, name in enumerate(batch.schema.names)}
    n = batch.num_rows
    arrays: list[pa.Array] = []
    names: list[str] = []
    if ds.monthly:
        arrays.append(pa.array([period] * n, type=pa.date32()))
        names.append("source_month")
    for name, pg_type in ds.columns:
        arrow_type = ARROW_TYPES[pg_type]
        idx = by_lower.get(name)
        if idx is None:
            arrays.append(pa.nulls(n, type=arrow_type))
        else:
            arrays.append(pc.cast(batch.column(idx), arrow_type, safe=True))
        names.append(name)
    arrays.append(pa.array([file_name] * n, type=pa.string()))
    arrays.append(pa.array(range(first_row, first_row + n), type=pa.int64()))
    arrays.append(pa.array([loaded_at] * n, type=pa.timestamp("us", tz="UTC")))
    names += ["_source_file", "_source_row", "_loaded_at"]
    return pa.Table.from_arrays(arrays, names=names)


def unknown_columns(ds: Dataset, path: Path) -> list[str]:
    """Source columns the raw table does not carry (reported, not loaded)."""
    if ds.file_format == "parquet":
        source = pq.ParquetFile(path).schema_arrow.names
    else:
        source = pacsv.read_csv(path).schema.names
    known = {c for c, _ in ds.columns}
    return [c for c in source if c.lower() not in known]
