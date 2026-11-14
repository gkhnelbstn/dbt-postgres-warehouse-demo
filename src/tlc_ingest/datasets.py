"""Declarative description of each source file family and its raw table.

Raw tables mirror the source: same columns, lower-cased names, source types.
Cleaning and renaming belong to dbt staging, not here.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import pyarrow as pa

ARROW_TYPES: dict[str, pa.DataType] = {
    "integer": pa.int32(),
    "bigint": pa.int64(),
    "double precision": pa.float64(),
    "timestamp": pa.timestamp("us"),
    "text": pa.string(),
}

_MONEY = [
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
]


@dataclass(frozen=True)
class Dataset:
    name: str
    table: str
    url_path: str  # str.format template; gets `month` for monthly datasets
    columns: tuple[tuple[str, str], ...]  # (raw column name, PostgreSQL type)
    monthly: bool = True
    file_format: str = "parquet"

    def file_name(self, month: date | None) -> str:
        return self.url_path.format(month=month).rsplit("/", 1)[-1]

    def url(self, base_url: str, month: date | None) -> str:
        return f"{base_url.rstrip('/')}/{self.url_path.format(month=month)}"

    def target_table(self, month: date | None) -> str:
        """Table the file is copied into: the month's partition, or the table itself."""
        return f"{self.table}_{month:%Y_%m}" if self.monthly else self.table


YELLOW = Dataset(
    name="yellow",
    table="yellow_trips",
    url_path="trip-data/yellow_tripdata_{month:%Y-%m}.parquet",
    columns=(
        ("vendorid", "integer"),
        ("tpep_pickup_datetime", "timestamp"),
        ("tpep_dropoff_datetime", "timestamp"),
        ("passenger_count", "integer"),
        ("trip_distance", "double precision"),
        ("ratecodeid", "integer"),
        ("store_and_fwd_flag", "text"),
        ("pulocationid", "integer"),
        ("dolocationid", "integer"),
        ("payment_type", "integer"),
        *((c, "double precision") for c in _MONEY),
        ("airport_fee", "double precision"),
        ("cbd_congestion_fee", "double precision"),  # added by TLC in 2025
        ("request_source", "text"),  # appeared in later files
    ),
)

GREEN = Dataset(
    name="green",
    table="green_trips",
    url_path="trip-data/green_tripdata_{month:%Y-%m}.parquet",
    columns=(
        ("vendorid", "integer"),
        ("lpep_pickup_datetime", "timestamp"),
        ("lpep_dropoff_datetime", "timestamp"),
        ("store_and_fwd_flag", "text"),
        ("ratecodeid", "integer"),
        ("pulocationid", "integer"),
        ("dolocationid", "integer"),
        ("passenger_count", "integer"),
        ("trip_distance", "double precision"),
        *((c, "double precision") for c in _MONEY),
        ("ehail_fee", "double precision"),
        ("payment_type", "integer"),
        ("trip_type", "integer"),
        ("cbd_congestion_fee", "double precision"),
        ("request_source", "text"),
    ),
)

ZONES = Dataset(
    name="zones",
    table="taxi_zones",
    url_path="misc/taxi_zone_lookup.csv",
    columns=(
        ("locationid", "integer"),
        ("borough", "text"),
        ("zone", "text"),
        ("service_zone", "text"),
    ),
    monthly=False,
    file_format="csv",
)

DATASETS: dict[str, Dataset] = {d.name: d for d in (YELLOW, GREEN, ZONES)}
