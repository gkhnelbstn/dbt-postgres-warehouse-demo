from __future__ import annotations

import argparse
import io
from datetime import date, datetime

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from tlc_ingest import db
from tlc_ingest.cli import _suspicious_gaps, months, parse_month
from tlc_ingest.datasets import YELLOW
from tlc_ingest.download import fetch, make_client, probe

from .conftest import make_yellow_parquet


def test_months_crosses_year_boundary():
    assert months(date(2024, 11, 1), date(2025, 2, 1)) == [
        date(2024, 11, 1),
        date(2024, 12, 1),
        date(2025, 1, 1),
        date(2025, 2, 1),
    ]


def test_parse_month_rejects_garbage():
    with pytest.raises(argparse.ArgumentTypeError):
        parse_month("2024/10")


def test_yellow_url_and_partition_names():
    m = date(2024, 10, 1)
    assert YELLOW.url("https://x/", m) == "https://x/trip-data/yellow_tripdata_2024-10.parquet"
    assert YELLOW.target_table(m) == "yellow_trips_2024_10"


def _batch(body: bytes) -> pa.RecordBatch:
    return pq.read_table(io.BytesIO(body)).to_batches()[0]


def test_conform_handles_schema_drift():
    """Old files lack new columns, new files carry unknown ones, and case varies."""
    batch = _batch(make_yellow_parquet(3, datetime(2024, 10, 1), extra_columns=True))
    table = db._conform(YELLOW, batch, date(2024, 10, 1), "f.parquet", datetime(2024, 12, 1))

    expected = [
        "source_month",
        *(c for c, _ in YELLOW.columns),
        "_source_file",
        "_source_row",
        "_loaded_at",
    ]
    assert table.column_names == expected
    assert table.column("_source_row").to_pylist() == [1, 2, 3]
    assert table.column("airport_fee").to_pylist() == [0.0] * 3  # was `Airport_fee`
    assert table.column("cbd_congestion_fee").null_count == 3  # missing in source
    assert table.column("request_source").to_pylist() == ["app"] * 3
    assert "brand_new_column" not in table.column_names
    assert table.column("passenger_count").type == pa.int32()


def test_conform_refuses_overflow():
    batch = _batch(make_yellow_parquet(1, datetime(2024, 10, 1)))
    idx = batch.schema.get_field_index("passenger_count")
    batch = batch.set_column(idx, "passenger_count", pa.array([2**40], pa.int64()))
    with pytest.raises(pa.ArrowInvalid):
        db._conform(YELLOW, batch, date(2024, 10, 1), "f", datetime(2024, 12, 1))


def test_probe_version_ignores_compression():
    """CloudFront answers a gzip-accepting client with a weak ETag and no Content-Length."""

    def cdn(request: httpx.Request) -> httpx.Response:
        if "gzip" in request.headers.get("accept-encoding", ""):
            return httpx.Response(200, headers={"etag": 'W/"abc"', "content-encoding": "gzip"})
        return httpx.Response(200, headers={"etag": '"abc"', "content-length": "12331"})

    with make_client() as real:
        assert real.headers["accept-encoding"] == "identity"
        sent = dict(real.headers)
    with httpx.Client(transport=httpx.MockTransport(cdn), headers=sent) as c:
        plain = probe(c, "https://fake/misc/taxi_zone_lookup.csv")
    with httpx.Client(transport=httpx.MockTransport(cdn)) as c:
        gzipped = probe(c, "https://fake/misc/taxi_zone_lookup.csv")
    assert plain.size == 12331
    assert plain.version == gzipped.version == '"abc"'


def test_probe_unpublished_is_none(fake_tlc):
    with fake_tlc.client() as c:
        assert probe(c, "https://fake/trip-data/nope.parquet") is None


def test_fetch_resumes_partial_download(fake_tlc, tmp_path):
    body = bytes(range(256)) * 100
    fake_tlc.publish("f.bin", body, '"v1"')
    dest = tmp_path / "f.bin"
    # A previous run died after 1000 bytes of the same version.
    (tmp_path / "f.bin.part").write_bytes(body[:1000])
    (tmp_path / "f.bin.part.version").write_text('"v1"')

    with fake_tlc.client() as c:
        fetch(c, probe(c, "https://fake/f.bin"), dest)
    assert dest.read_bytes() == body
    assert not (tmp_path / "f.bin.part").exists()


def test_fetch_discards_partial_of_older_version(fake_tlc, tmp_path):
    body = b"new contents" * 50
    fake_tlc.publish("f.bin", body, '"v2"')
    (tmp_path / "f.bin.part").write_bytes(b"old!" * 10)
    (tmp_path / "f.bin.part.version").write_text('"v1"')

    with fake_tlc.client() as c:
        fetch(c, probe(c, "https://fake/f.bin"), tmp_path / "f.bin")
    assert (tmp_path / "f.bin").read_bytes() == body


def test_fetch_uses_cache_for_same_version(fake_tlc, tmp_path):
    fake_tlc.publish("f.bin", b"abc", '"v1"')
    with fake_tlc.client() as c:
        remote = probe(c, "https://fake/f.bin")
        fetch(c, remote, tmp_path / "f.bin")
        fetch(c, remote, tmp_path / "f.bin")
    assert fake_tlc.gets == ["f.bin"]


def test_gap_detection():
    a, b, c = date(2024, 1, 1), date(2024, 2, 1), date(2024, 3, 1)
    assert not _suspicious_gaps({("y", a): "loaded", ("y", b): "loaded", ("y", c): "unpublished"})
    assert _suspicious_gaps({("y", a): "unpublished", ("y", b): "loaded"})
    assert _suspicious_gaps({("y", a): "unpublished", ("y", b): "unpublished"})
    assert not _suspicious_gaps({("zones", None): "loaded", ("y", a): "skipped"})
