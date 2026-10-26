"""The ingestion manifest: one row per (source, period), stored as Parquet.

This is the state that makes ingestion idempotent. `file_hash` is the SHA-256 of the
*extracted* payload, never of the zip, because zip bytes change with compression
metadata even when the contents are identical (see ADR-003). `last_modified` is what the
server reported and only decides whether a download is worth attempting; the hash
decides whether anything is rewritten.

Losing this file costs one expensive re-download, never correctness: every hash can be
recomputed from what is already in bronze.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

STATUS_INGESTED = "ingested"
STATUS_UNCHANGED = "unchanged"
STATUS_FAILED = "failed"

SCHEMA = pa.schema(
    [
        pa.field("source", pa.string(), nullable=False),
        pa.field("period", pa.string(), nullable=False),
        pa.field("source_file", pa.string(), nullable=False),
        pa.field("file_hash", pa.string(), nullable=True),
        pa.field("last_modified", pa.string(), nullable=True),
        pa.field("rows", pa.int64(), nullable=True),
        pa.field("ingested_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("status", pa.string(), nullable=False),
    ]
)


@dataclass(frozen=True, slots=True)
class ManifestEntry:
    """One ingestion outcome for one source and one period."""

    source: str
    period: str
    source_file: str
    file_hash: str | None
    last_modified: str | None
    rows: int | None
    ingested_at: datetime
    status: str

    @classmethod
    def now(
        cls,
        *,
        source: str,
        period: str,
        source_file: str,
        file_hash: str | None,
        last_modified: str | None,
        rows: int | None,
        status: str,
    ) -> ManifestEntry:
        return cls(
            source=source,
            period=period,
            source_file=source_file,
            file_hash=file_hash,
            last_modified=last_modified,
            rows=rows,
            ingested_at=datetime.now(tz=UTC),
            status=status,
        )


def load(path: Path) -> pa.Table:
    """Read the manifest, returning an empty table with the right schema if absent."""
    if not path.is_file():
        return SCHEMA.empty_table()
    return pq.read_table(path, schema=SCHEMA)


def entries(path: Path) -> list[ManifestEntry]:
    """Read the manifest as dataclasses, newest first for a given key."""
    table = load(path)
    return [
        ManifestEntry(
            source=row["source"],
            period=row["period"],
            source_file=row["source_file"],
            file_hash=row["file_hash"],
            last_modified=row["last_modified"],
            rows=row["rows"],
            ingested_at=row["ingested_at"],
            status=row["status"],
        )
        for row in table.to_pylist()
    ]


def lookup(path: Path, source: str, period: str) -> ManifestEntry | None:
    """Return the recorded entry for one source and period, or None."""
    for entry in entries(path):
        if entry.source == source and entry.period == period:
            return entry
    return None


def upsert(path: Path, entry: ManifestEntry) -> None:
    """Replace the row for `(source, period)` and rewrite the manifest atomically.

    The manifest is small (one row per source-month), so rewriting it whole is simpler
    and safer than an append that could leave a half-written file behind.
    """
    kept = [
        asdict(existing)
        for existing in entries(path)
        if not (existing.source == entry.source and existing.period == entry.period)
    ]
    kept.append(asdict(entry))
    kept.sort(key=lambda row: (row["source"], row["period"]))

    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(kept, schema=SCHEMA)
    tmp = path.with_suffix(path.suffix + ".tmp")
    pq.write_table(table, tmp, compression="zstd")
    tmp.replace(path)
