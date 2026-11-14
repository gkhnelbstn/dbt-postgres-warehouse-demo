"""Runs the real CLI against a fake CDN and a real PostgreSQL (throwaway database)."""

from __future__ import annotations

from datetime import datetime

import psycopg
import pytest

from tlc_ingest import cli

from .conftest import ZONES_CSV, make_yellow_parquet

pytestmark = pytest.mark.db


@pytest.fixture
def run_cli(fake_tlc, test_database, tmp_path, monkeypatch):
    monkeypatch.setattr(cli, "make_client", fake_tlc.client)

    def _run(*extra: str) -> int:
        return cli.main(
            [
                "--from",
                "2024-10",
                "--to",
                "2024-12",
                "--base-url",
                "https://fake",
                "--cache-dir",
                str(tmp_path),
                *extra,
            ]
        )

    return _run


def _publish_two_months(fake_tlc, oct_rows=50, oct_etag='"oct-1"'):
    fake_tlc.publish("misc/taxi_zone_lookup.csv", ZONES_CSV, '"z1"')
    fake_tlc.publish(
        "trip-data/yellow_tripdata_2024-10.parquet",
        make_yellow_parquet(oct_rows, datetime(2024, 10, 1)),
        oct_etag,
    )
    fake_tlc.publish(
        "trip-data/yellow_tripdata_2024-11.parquet",
        make_yellow_parquet(30, datetime(2024, 11, 1), extra_columns=True),
        '"nov-1"',
    )
    # December is not published yet, like the newest months at TLC.


def _counts() -> dict[str, int]:
    with psycopg.connect() as conn:
        rows = conn.execute(
            "select source_month::text, count(*) from raw.yellow_trips group by 1 order by 1"
        ).fetchall()
    return dict(rows)


def _log() -> list[tuple]:
    with psycopg.connect() as conn:
        return conn.execute(
            "select dataset, period::text, status, source_rows, loaded_rows "
            "from raw.load_log order by load_id"
        ).fetchall()


def test_load_is_incremental_and_idempotent(fake_tlc, run_cli):
    _publish_two_months(fake_tlc)

    assert run_cli() == 0
    assert _counts() == {"2024-10-01": 50, "2024-11-01": 30}
    first_log = _log()
    assert [r[2] for r in first_log] == ["succeeded"] * 3

    # Same versions again: nothing is downloaded or loaded.
    gets_before = len(fake_tlc.gets)
    assert run_cli() == 0
    assert len(fake_tlc.gets) == gets_before
    assert _log() == first_log

    # TLC republishes October with more rows: only October is replaced.
    _publish_two_months(fake_tlc, oct_rows=70, oct_etag='"oct-2"')
    assert run_cli() == 0
    assert _counts() == {"2024-10-01": 70, "2024-11-01": 30}
    assert _log()[-1] == ("yellow", "2024-10-01", "succeeded", 70, 70)


def test_failed_load_keeps_previous_rows_and_is_logged(fake_tlc, run_cli):
    _publish_two_months(fake_tlc)
    assert run_cli() == 0

    fake_tlc.publish("trip-data/yellow_tripdata_2024-10.parquet", b"not parquet", '"oct-bad"')
    assert run_cli() == 1
    assert _counts()["2024-10-01"] == 50  # the failed reload rolled back
    assert _log()[-1][:3] == ("yellow", "2024-10-01", "failed")


def test_max_rows_limits_each_file(fake_tlc, run_cli):
    _publish_two_months(fake_tlc)
    assert run_cli("--max-rows", "10") == 0
    assert _counts() == {"2024-10-01": 10, "2024-11-01": 10}
    # A later full run must not mistake the sample for a finished load.
    assert run_cli() == 0
    assert _counts() == {"2024-10-01": 50, "2024-11-01": 30}
