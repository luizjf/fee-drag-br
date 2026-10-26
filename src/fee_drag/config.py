"""Runtime configuration, assembled from environment variables only.

Nothing in this package hardcodes a path or a URL: every caller takes a `Settings`
instance built by `Settings.from_env()`. `.env` is read by `load_env_file()` so the
project needs no extra dependency for it, and process environment always wins over the
file so CI and Docker can override anything.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path

ENV_FILE_NAME = ".env"
PERIOD_PATTERN = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")

DEFAULTS: dict[str, str] = {
    "FEE_DRAG_DATA_DIR": "data",
    "FEE_DRAG_RAW_DIR": "data/raw",
    "FEE_DRAG_BRONZE_DIR": "data/bronze",
    "FEE_DRAG_EXPORT_DIR": "data/exports",
    "FEE_DRAG_DUCKDB_PATH": "data/warehouse.duckdb",
    "FEE_DRAG_MANIFEST_PATH": "data/bronze/_manifest.parquet",
    "FEE_DRAG_LOG_LEVEL": "INFO",
    "CVM_DAILY_REPORTS_BASE_URL": "https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS",
    "CVM_REGISTRY_BASE_URL": "https://dados.cvm.gov.br/dados/FI/CAD/DADOS",
    "CVM_EXTRATO_BASE_URL": "https://dados.cvm.gov.br/dados/FI/DOC/EXTRATO/DADOS",
    "BCB_SGS_BASE_URL": "https://api.bcb.gov.br/dados/serie",
    "HTTP_TIMEOUT_SECONDS": "60",
    "HTTP_MAX_RETRIES": "5",
    "HTTP_BACKOFF_BASE_SECONDS": "1.0",
    "INGEST_START_MONTH": "2021-01",
}


class ConfigError(ValueError):
    """Raised when an environment variable holds a value we cannot use."""


def load_env_file(path: Path, *, override: bool = False) -> dict[str, str]:
    """Load `KEY=value` pairs from a dotenv-style file into `os.environ`.

    Blank lines, `#` comments, a leading `export ` and surrounding quotes are handled.
    A missing file is not an error: the defaults and the process environment cover it.
    Returns the pairs that were applied, which keeps the function testable.
    """
    if not path.is_file():
        return {}

    applied: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.removeprefix("export ").partition("=")
        key = key.strip()
        value = value.strip().strip("\"'")
        if not key:
            continue
        if override or key not in os.environ:
            os.environ[key] = value
            applied[key] = value
    return applied


def _raw(name: str) -> str:
    return os.environ.get(name) or DEFAULTS[name]


def _positive_float(name: str) -> float:
    value = _raw(name)
    try:
        parsed = float(value)
    except ValueError as exc:
        raise ConfigError(f"{name} must be a number, got {value!r}") from exc
    if parsed <= 0:
        raise ConfigError(f"{name} must be greater than zero, got {parsed}")
    return parsed


def _non_negative_int(name: str) -> int:
    value = _raw(name)
    try:
        parsed = int(value)
    except ValueError as exc:
        raise ConfigError(f"{name} must be an integer, got {value!r}") from exc
    if parsed < 0:
        raise ConfigError(f"{name} must not be negative, got {parsed}")
    return parsed


@dataclass(frozen=True, slots=True)
class Settings:
    """Everything the ingestion layer needs to know about its environment."""

    project_root: Path
    data_dir: Path
    raw_dir: Path
    bronze_dir: Path
    export_dir: Path
    duckdb_path: Path
    manifest_path: Path
    log_level: str
    cvm_daily_reports_base_url: str
    cvm_registry_base_url: str
    cvm_extrato_base_url: str
    bcb_sgs_base_url: str
    http_timeout_seconds: float
    http_max_retries: int
    http_backoff_base_seconds: float
    ingest_start_month: str

    @classmethod
    def from_env(cls, *, project_root: Path | None = None, read_env_file: bool = True) -> Settings:
        """Build settings from the process environment.

        `project_root` anchors every relative path. It defaults to
        `FEE_DRAG_PROJECT_ROOT` and then to the current working directory, so a caller
        that runs from somewhere else can still point at the right tree.
        """
        root = project_root or Path(os.environ.get("FEE_DRAG_PROJECT_ROOT", ".")).resolve()
        if read_env_file:
            load_env_file(root / ENV_FILE_NAME)

        start_month = _raw("INGEST_START_MONTH")
        if not PERIOD_PATTERN.match(start_month):
            raise ConfigError(f"INGEST_START_MONTH must look like YYYY-MM, got {start_month!r}")

        def path_of(name: str) -> Path:
            value = Path(_raw(name))
            return value if value.is_absolute() else root / value

        return cls(
            project_root=root,
            data_dir=path_of("FEE_DRAG_DATA_DIR"),
            raw_dir=path_of("FEE_DRAG_RAW_DIR"),
            bronze_dir=path_of("FEE_DRAG_BRONZE_DIR"),
            export_dir=path_of("FEE_DRAG_EXPORT_DIR"),
            duckdb_path=path_of("FEE_DRAG_DUCKDB_PATH"),
            manifest_path=path_of("FEE_DRAG_MANIFEST_PATH"),
            log_level=_raw("FEE_DRAG_LOG_LEVEL").upper(),
            cvm_daily_reports_base_url=_raw("CVM_DAILY_REPORTS_BASE_URL").rstrip("/"),
            cvm_registry_base_url=_raw("CVM_REGISTRY_BASE_URL").rstrip("/"),
            cvm_extrato_base_url=_raw("CVM_EXTRATO_BASE_URL").rstrip("/"),
            bcb_sgs_base_url=_raw("BCB_SGS_BASE_URL").rstrip("/"),
            http_timeout_seconds=_positive_float("HTTP_TIMEOUT_SECONDS"),
            http_max_retries=_non_negative_int("HTTP_MAX_RETRIES"),
            http_backoff_base_seconds=_positive_float("HTTP_BACKOFF_BASE_SECONDS"),
            ingest_start_month=start_month,
        )
