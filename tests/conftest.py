"""Shared test fixtures. No test in this suite touches the network."""

from __future__ import annotations

import zipfile
from dataclasses import replace
from pathlib import Path

import httpx
import pytest

from fee_drag.config import DEFAULTS, Settings

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def clean_env(monkeypatch):
    """Remove every variable this project reads, so a test starts from the defaults."""
    for key in (*DEFAULTS, "FEE_DRAG_PROJECT_ROOT"):
        monkeypatch.delenv(key, raising=False)


@pytest.fixture
def settings(tmp_path):
    """Settings pointing entirely inside tmp_path, with a fast retry budget."""
    return Settings(
        project_root=tmp_path,
        data_dir=tmp_path / "data",
        raw_dir=tmp_path / "data" / "raw",
        bronze_dir=tmp_path / "data" / "bronze",
        export_dir=tmp_path / "data" / "exports",
        duckdb_path=tmp_path / "data" / "warehouse.duckdb",
        manifest_path=tmp_path / "data" / "bronze" / "_manifest.parquet",
        log_level="INFO",
        cvm_daily_reports_base_url="https://example.test/INF_DIARIO/DADOS",
        cvm_registry_base_url="https://example.test/CAD/DADOS",
        cvm_extrato_base_url="https://example.test/EXTRATO/DADOS",
        bcb_sgs_base_url="https://example.test/dados/serie",
        http_timeout_seconds=5.0,
        http_max_retries=2,
        http_backoff_base_seconds=0.001,
        ingest_start_month="2021-01",
    )


def fixture_zip(period: str) -> bytes:
    return (FIXTURES / f"inf_diario_fi_{period}.zip").read_bytes()


def rezip(period: str, *, drop_rows: int = 0) -> bytes:
    """Rebuild a fixture zip with fewer data rows, to simulate a CVM republication."""
    import io

    original = fixture_zip(period)
    with zipfile.ZipFile(io.BytesIO(original)) as archive:
        name = archive.namelist()[0]
        lines = [line for line in archive.read(name).split(b"\r\n") if line]
    header, body = lines[0], lines[1:]
    kept = body[: len(body) - drop_rows] if drop_rows else body
    payload = b"\r\n".join([header, *kept]) + b"\r\n"

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(name, payload)
    return buffer.getvalue()


class RecordingTransport(httpx.BaseTransport):
    """Serve canned monthly zips and record every request that was issued."""

    def __init__(
        self, months: dict[str, bytes], *, last_modified: str = "Tue, 20 Oct 2026 03:55:48 GMT"
    ):
        self.months = months
        self.last_modified = last_modified
        self.calls: list[tuple[str, str]] = []

    def set_month(self, period: str, payload: bytes, *, last_modified: str | None = None) -> None:
        self.months[period] = payload
        if last_modified is not None:
            self.last_modified = last_modified

    @property
    def methods(self) -> list[str]:
        return [method for method, _ in self.calls]

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        period = request.url.path.rsplit("inf_diario_fi_", 1)[-1].removesuffix(".zip")
        self.calls.append((request.method, period))
        if period not in self.months:
            return httpx.Response(404, request=request)
        payload = self.months[period]
        headers = {"last-modified": self.last_modified, "content-length": str(len(payload))}
        if request.method == "HEAD":
            return httpx.Response(200, headers=headers, request=request)
        return httpx.Response(200, content=payload, headers=headers, request=request)


@pytest.fixture
def daily_zip():
    """Callable giving the raw bytes of a fixture month."""
    return fixture_zip


@pytest.fixture
def rezipped():
    """Callable rebuilding a fixture month with fewer rows."""
    return rezip


@pytest.fixture
def transport():
    return RecordingTransport({"202609": fixture_zip("202609"), "202101": fixture_zip("202101")})


@pytest.fixture
def client(settings, transport):
    from fee_drag.http_client import build_client

    with build_client(settings, transport=transport) as active:
        yield active


@pytest.fixture
def no_retry_settings(settings):
    return replace(settings, http_max_retries=0)
