"""`tlc-ingest`: load a range of TLC months (plus the zone lookup) into PostgreSQL.

A file is (re)loaded only when its published version differs from the last
successful load recorded in `raw.load_log`, so re-running the same command is
a no-op, a crashed run picks up where it stopped, and a month TLC republishes
is replaced in place.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from datetime import date
from pathlib import Path

from . import db
from .datasets import DATASETS, Dataset
from .download import fetch, make_client, probe

DEFAULT_BASE_URL = "https://d37ci6vzurychx.cloudfront.net"
log = logging.getLogger("tlc_ingest")


def parse_month(value: str) -> date:
    try:
        year, month = value.split("-")
        return date(int(year), int(month), 1)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"expected YYYY-MM, got {value!r}") from exc


def months(start: date, end: date) -> list[date]:
    out, current = [], start
    while current <= end:
        out.append(current)
        current = date(current.year + current.month // 12, current.month % 12 + 1, 1)
    return out


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="tlc-ingest", description=__doc__.splitlines()[0])
    p.add_argument(
        "--dataset",
        action="append",
        choices=[n for n, d in DATASETS.items() if d.monthly],
        help="trip dataset to load; repeat for several (default: yellow)",
    )
    p.add_argument("--from", dest="start", type=parse_month, required=True, metavar="YYYY-MM")
    p.add_argument("--to", dest="end", type=parse_month, required=True, metavar="YYYY-MM")
    p.add_argument(
        "--max-rows",
        type=int,
        help="load only the first N rows of each trip file (for CI and quick tries)",
    )
    p.add_argument("--force", action="store_true", help="reload even if already loaded")
    p.add_argument("--base-url", default=os.environ.get("TLC_BASE_URL", DEFAULT_BASE_URL))
    p.add_argument(
        "--cache-dir", type=Path, default=Path(os.environ.get("TLC_CACHE_DIR", "data/cache"))
    )
    return p


def run(args: argparse.Namespace) -> int:
    if args.end < args.start:
        log.error("--to is before --from")
        return 2
    trip_datasets = [DATASETS[n] for n in (args.dataset or ["yellow"])]
    jobs: list[tuple[Dataset, date | None]] = [(DATASETS["zones"], None)]
    jobs += [(ds, m) for ds in trip_datasets for m in months(args.start, args.end)]

    summary = {"loaded": 0, "skipped": 0, "unpublished": 0, "failed": 0}
    outcomes: dict[tuple[str, date | None], str] = {}
    with make_client() as client, db.connect() as conn:
        # All raw tables exist even if only some datasets are loaded, so dbt always compiles.
        db.bootstrap(conn, list(DATASETS.values()))
        for ds, period in jobs:
            outcome = _one(conn, client, args, ds, period)
            outcomes[(ds.name, period)] = outcome
            summary[outcome] += 1

    log.info("done: %s", ", ".join(f"{k}={v}" for k, v in summary.items()))
    return 1 if summary["failed"] or _suspicious_gaps(outcomes) else 0


def _suspicious_gaps(outcomes: dict[tuple[str, date | None], str]) -> bool:
    """True if the published/unpublished pattern points at a blocked or wrong URL.

    Only the newest months may legitimately be unpublished (TLC lags about two
    months). A missing month older than a published one, or a range where no
    month is published at all, is reported as an error.
    """
    bad = False
    for name in {n for n, p in outcomes if p is not None}:
        found = sorted(
            p for (n, p), o in outcomes.items() if n == name and p and o != "unpublished"
        )
        missing = [p for (n, p), o in outcomes.items() if n == name and p and o == "unpublished"]
        if not found:
            log.error("%s: no month in the range is published; check --from/--to/--base-url", name)
            bad = True
        elif gaps := sorted(p for p in missing if p < found[-1]):
            log.error("%s: unpublished months inside the range: %s", name, gaps)
            bad = True
    return bad


def _one(conn, client, args, ds: Dataset, period: date | None) -> str:
    label = f"{ds.name} {period:%Y-%m}" if period else ds.name
    url = ds.url(args.base_url, period)
    remote = probe(client, url)
    if remote is None:
        log.warning("%s: not published (%s)", label, url)
        return "unpublished"
    previous = None if args.force else db.last_load(conn, ds, period)
    if previous and previous[0] == remote.version and (previous[1] or args.max_rows):
        log.info("%s: already loaded, skipping", label)
        return "skipped"

    load_id = db.start_log(conn, ds, period, url, remote.version, remote.size)
    started = time.monotonic()
    try:
        path = fetch(client, remote, args.cache_dir / ds.name / ds.file_name(period))
        extra = db.unknown_columns(ds, path)
        if extra:
            log.warning("%s: source columns not in raw table, not loaded: %s", label, extra)
        max_rows = args.max_rows if ds.monthly else None
        source_rows, loaded = db.load_file(conn, ds, period, path, load_id, max_rows)
    except Exception as exc:
        db.fail_log(conn, load_id, f"{type(exc).__name__}: {exc}")
        log.error("%s: failed: %s", label, exc)
        return "failed"
    elapsed = time.monotonic() - started
    log.info("%s: loaded %s of %s rows in %.1fs", label, f"{loaded:,}", f"{source_rows:,}", elapsed)
    return "loaded"


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    return run(build_parser().parse_args(argv))


if __name__ == "__main__":
    sys.exit(main())
