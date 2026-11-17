"""HTTP side: find out what version of a file is published, and fetch it resumably."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from email.utils import parsedate_to_datetime
from pathlib import Path

import httpx

USER_AGENT = "tlc-ingest/0.1"
CHUNK = 1 << 20


@dataclass(frozen=True)
class RemoteFile:
    url: str
    version: str  # ETag, or last-modified + size when the server sends no ETag
    size: int | None
    last_modified: datetime | None


def make_client() -> httpx.Client:
    # Ask for the raw bytes. CloudFront gzips text files (the zone lookup CSV) when the
    # client accepts it, and then answers with a weak ETag and no Content-Length; whether
    # a response comes compressed can change between requests, which made an unchanged
    # file look republished. Uncompressed bytes also keep Range resumes and size checks exact.
    return httpx.Client(
        headers={"User-Agent": USER_AGENT, "Accept-Encoding": "identity"},
        follow_redirects=True,
        timeout=httpx.Timeout(60.0, connect=20.0),
    )


def probe(client: httpx.Client, url: str) -> RemoteFile | None:
    """HEAD the file. None means it is not published (TLC answers 403 for those)."""
    response = client.head(url)
    if response.status_code in (403, 404):
        return None
    response.raise_for_status()
    size = int(response.headers["content-length"]) if "content-length" in response.headers else None
    raw_modified = response.headers.get("last-modified")
    last_modified = parsedate_to_datetime(raw_modified) if raw_modified else None
    etag = response.headers.get("etag")
    if etag and etag.startswith("W/"):
        etag = etag[2:]  # same file, served compressed; compare on the entity tag itself
    version = etag or f"{raw_modified}-{size}"
    return RemoteFile(url=url, version=version, size=size, last_modified=last_modified)


def fetch(client: httpx.Client, remote: RemoteFile, dest: Path) -> Path:
    """Download `remote` to `dest`, reusing a cached copy or resuming a partial one.

    The version each file belongs to is kept in a sidecar, so a cached or partial
    file left over from an older publication is never mistaken for the current one.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    tag = dest.with_name(dest.name + ".version")
    part = dest.with_name(dest.name + ".part")
    part_tag = dest.with_name(dest.name + ".part.version")

    if dest.exists() and _read(tag) == remote.version:
        return dest

    offset = part.stat().st_size if part.exists() and _read(part_tag) == remote.version else 0
    headers = {"Range": f"bytes={offset}-"} if offset else {}
    part_tag.write_text(remote.version, encoding="utf-8")

    with client.stream("GET", remote.url, headers=headers) as response:
        if response.status_code == 416 and offset == remote.size:
            pass  # the partial file was already complete
        else:
            response.raise_for_status()
            resumed = response.status_code == 206
            with part.open("ab" if resumed else "wb") as fh:
                for chunk in response.iter_bytes(CHUNK):
                    fh.write(chunk)

    actual = part.stat().st_size
    if remote.size is not None and actual != remote.size:
        raise OSError(f"{remote.url}: got {actual} bytes, expected {remote.size}")

    part.replace(dest)
    tag.write_text(remote.version, encoding="utf-8")
    part_tag.unlink(missing_ok=True)
    return dest


def _read(path: Path) -> str | None:
    return path.read_text(encoding="utf-8") if path.exists() else None
