from __future__ import annotations

import io
import os
from collections.abc import Callable
from datetime import datetime, timedelta

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pytest


def make_yellow_parquet(rows: int, start: datetime, extra_columns: bool = False) -> bytes:
    """A small file shaped like a real yellow-taxi file (source column names and types)."""
    pickup = [start + timedelta(minutes=i) for i in range(rows)]
    cols = {
        "VendorID": pa.array([1 + i % 2 for i in range(rows)], pa.int32()),
        "tpep_pickup_datetime": pa.array(pickup, pa.timestamp("us")),
        "tpep_dropoff_datetime": pa.array(
            [p + timedelta(minutes=12) for p in pickup], pa.timestamp("us")
        ),
        "passenger_count": pa.array([1] * rows, pa.int64()),
        "trip_distance": pa.array([2.5] * rows, pa.float64()),
        "RatecodeID": pa.array([1] * rows, pa.int64()),
        "store_and_fwd_flag": pa.array(["N"] * rows, pa.large_string()),
        "PULocationID": pa.array([132] * rows, pa.int32()),
        "DOLocationID": pa.array([236] * rows, pa.int32()),
        "payment_type": pa.array([1] * rows, pa.int64()),
        "fare_amount": pa.array([14.2] * rows, pa.float64()),
        "total_amount": pa.array([20.0] * rows, pa.float64()),
        "Airport_fee": pa.array([0.0] * rows, pa.float64()),
    }
    if extra_columns:
        cols["request_source"] = pa.array(["app"] * rows, pa.large_string())
        cols["brand_new_column"] = pa.array([7] * rows, pa.int64())
    buf = io.BytesIO()
    pq.write_table(pa.table(cols), buf)
    return buf.getvalue()


ZONES_CSV = (
    b'"LocationID","Borough","Zone","service_zone"\n'
    b'132,"Queens","JFK Airport","Airports"\n'
    b'236,"Manhattan","Upper East Side North","Yellow Zone"\n'
)


class FakeTLC:
    """In-memory stand-in for the TLC CDN: HEAD with ETag, GET with Range."""

    def __init__(self) -> None:
        self.files: dict[str, tuple[bytes, str]] = {}
        self.gets: list[str] = []

    def publish(self, path: str, body: bytes, etag: str) -> None:
        self.files[path] = (body, etag)

    def handler(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path.lstrip("/")
        if path not in self.files:
            return httpx.Response(403)
        body, etag = self.files[path]
        headers = {"etag": etag, "content-length": str(len(body))}
        if request.method == "HEAD":
            return httpx.Response(200, headers=headers)
        self.gets.append(path)
        if rng := request.headers.get("range"):
            offset = int(rng.removeprefix("bytes=").rstrip("-"))
            part = body[offset:]
            headers["content-length"] = str(len(part))
            return httpx.Response(206, headers={**headers}, content=part)
        return httpx.Response(200, headers=headers, content=body)

    def client(self) -> httpx.Client:
        return httpx.Client(transport=httpx.MockTransport(self.handler), base_url="https://fake")


@pytest.fixture
def fake_tlc() -> FakeTLC:
    return FakeTLC()


TEST_DB = "tlc_ingest_test"


@pytest.fixture
def test_database(monkeypatch) -> Callable[[], None]:
    """Point PG* at a fresh, throwaway database so tests never touch real raw data."""
    psycopg = pytest.importorskip("psycopg")
    if not os.environ.get("PGHOST"):
        pytest.skip("PGHOST not set; skipping database tests")
    try:
        admin = psycopg.connect(dbname=os.environ.get("PGDATABASE", "postgres"), autocommit=True)
    except psycopg.OperationalError as exc:
        pytest.skip(f"PostgreSQL not reachable: {exc}")
    with admin:
        admin.execute(f"drop database if exists {TEST_DB} with (force)")
        admin.execute(f"create database {TEST_DB}")
    monkeypatch.setenv("PGDATABASE", TEST_DB)
    return lambda: None
