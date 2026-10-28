"""Ingest the CVM fund registry (Cadastro) into the bronze layer.

Two downloads feed four bronze sources:

| Download                    | Member                   | Bronze source               |
| --------------------------- | ------------------------ | --------------------------- |
| `registro_fundo_classe.zip` | `registro_fundo.csv`     | `cvm_registry_funds`        |
| `registro_fundo_classe.zip` | `registro_classe.csv`    | `cvm_registry_classes`      |
| `registro_fundo_classe.zip` | `registro_subclasse.csv` | `cvm_registry_subclasses`   |
| `cad_fi.csv`                | (the file itself)        | `cvm_registry_legacy`       |

The registry is a snapshot of the present, not a monthly file, so the period is the
month of the server's `Last-Modified`. That makes the partition a property of the source
rather than of the clock on the machine that ran the ingestion: within a month a new
hash overwrites, and a new month opens a new partition. Bronze therefore keeps the last
snapshot of every month, which is the raw material for `snap_fund_registry` (SCD2).

Every column lands as `VARCHAR`. The registry has no typed dictionary we trust, and the
spike showed that digit-only CNPJs and CPFs lose their leading zeros as soon as a sniffer
calls them integers. Column names stay exactly as published (`Mixed_Case` here,
`UPPER_SNAKE` in `cad_fi.csv`), and CNPJs keep their source formatting; normalising
either is staging's job.

Each CSV is hashed on its own, so a change confined to the subclasses rewrites only the
subclasses partition.
"""

from __future__ import annotations

import io
import tempfile
import zipfile
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import TYPE_CHECKING

import httpx

from fee_drag import bronze, manifest
from fee_drag.http_client import build_client, request_with_retry
from fee_drag.log import get_logger

if TYPE_CHECKING:
    from fee_drag.config import Settings

logger = get_logger(__name__)

SOURCE_FUNDS = "cvm_registry_funds"
SOURCE_CLASSES = "cvm_registry_classes"
SOURCE_SUBCLASSES = "cvm_registry_subclasses"
SOURCE_LEGACY = "cvm_registry_legacy"

RCVM175_ARCHIVE = "registro_fundo_classe.zip"
LEGACY_FILE = "cad_fi.csv"

#: Zip member to bronze source, for the RCVM 175 archive.
RCVM175_MEMBERS: dict[str, str] = {
    "registro_fundo.csv": SOURCE_FUNDS,
    "registro_classe.csv": SOURCE_CLASSES,
    "registro_subclasse.csv": SOURCE_SUBCLASSES,
}

#: Headers observed in the files published on 2026-10-20. Drift is logged, not fatal:
#: bronze stores whatever arrives as text, and staging's contract catches real breakage.
EXPECTED_HEADERS: dict[str, tuple[str, ...]] = {
    SOURCE_FUNDS: (
        "ID_Registro_Fundo", "CNPJ_Fundo", "Codigo_CVM", "Data_Registro", "Data_Constituicao",
        "Tipo_Fundo", "Denominacao_Social", "Data_Cancelamento", "Situacao",
        "Data_Inicio_Situacao", "Data_Adaptacao_RCVM175", "Data_Inicio_Exercicio_Social",
        "Data_Fim_Exercicio_Social", "Patrimonio_Liquido", "Data_Patrimonio_Liquido",
        "Diretor", "CNPJ_Administrador", "Administrador", "Tipo_Pessoa_Gestor",
        "CPF_CNPJ_Gestor", "Gestor",
    ),
    SOURCE_CLASSES: (
        "ID_Registro_Fundo", "ID_Registro_Classe", "CNPJ_Classe", "Codigo_CVM",
        "Data_Registro", "Data_Constituicao", "Data_Inicio", "Tipo_Classe",
        "Denominacao_Social", "Situacao", "Data_Inicio_Situacao", "Classificacao",
        "Indicador_Desempenho", "Classe_Cotas", "Classificacao_Anbima",
        "Tributacao_Longo_Prazo", "Entidade_Investimento",
        "Permitido_Aplicacao_CemPorCento_Exterior", "Classe_ESG", "Forma_Condominio",
        "Exclusivo", "Publico_Alvo", "Patrimonio_Liquido", "Data_Patrimonio_Liquido",
        "CNPJ_Auditor", "Auditor", "CNPJ_Custodiante", "Custodiante", "CNPJ_Controlador",
        "Controlador",
    ),
    SOURCE_SUBCLASSES: (
        "ID_Registro_Classe", "ID_Subclasse", "Codigo_CVM", "Data_Constituicao",
        "Data_Inicio", "Denominacao_Social", "Situacao", "Data_Inicio_Situacao",
        "Forma_Condominio", "Exclusivo", "Publico_Alvo", "Previdenciario", "Exclusivo_INR",
        "Exclusivo_Previdencia_Complementar",
    ),
    SOURCE_LEGACY: (
        "TP_FUNDO", "CNPJ_FUNDO", "DENOM_SOCIAL", "DT_REG", "DT_CONST", "CD_CVM", "DT_CANCEL",
        "SIT", "DT_INI_SIT", "DT_INI_ATIV", "DT_INI_EXERC", "DT_FIM_EXERC", "CLASSE",
        "DT_INI_CLASSE", "RENTAB_FUNDO", "CONDOM", "FUNDO_COTAS", "FUNDO_EXCLUSIVO",
        "TRIB_LPRAZO", "PUBLICO_ALVO", "ENTID_INVEST", "TAXA_PERFM", "INF_TAXA_PERFM",
        "TAXA_ADM", "INF_TAXA_ADM", "VL_PATRIM_LIQ", "DT_PATRIM_LIQ", "DIRETOR",
        "CNPJ_ADMIN", "ADMIN", "PF_PJ_GESTOR", "CPF_CNPJ_GESTOR", "GESTOR", "CNPJ_AUDITOR",
        "AUDITOR", "CNPJ_CUSTODIANTE", "CUSTODIANTE", "CNPJ_CONTROLADOR", "CONTROLADOR",
        "INVEST_CEMPR_EXTER", "CLASSE_ANBIMA",
    ),
}  # fmt: skip


class ArchiveError(ValueError):
    """Raised when the RCVM 175 archive is missing a member we depend on."""


@dataclass(frozen=True, slots=True)
class SnapshotResult:
    """What happened to one bronze source in one run."""

    source: str
    period: str
    status: str
    rows: int | None
    file_hash: str | None


def snapshot_period(last_modified: str | None) -> str:
    """Turn an HTTP `Last-Modified` header into the `YYYYMM` partition it belongs to.

    Falls back to the current UTC month when the server sends no header, and logs it,
    because that is the one case where the partition depends on when we ran.
    """
    if last_modified:
        try:
            return parsedate_to_datetime(last_modified).astimezone(UTC).strftime("%Y%m")
        except (TypeError, ValueError):
            logger.warning("last_modified_unparseable", extra={"last_modified": last_modified})
    else:
        logger.warning("last_modified_missing")
    return datetime.now(tz=UTC).strftime("%Y%m")


def header_drift(csv_bytes: bytes, expected: tuple[str, ...]) -> tuple[list[str], list[str]]:
    """Return the columns added to and removed from a header, in file order."""
    header_line = csv_bytes.split(b"\n", 1)[0].decode("latin-1").strip()
    header = [name.strip() for name in header_line.split(";")]
    added = [name for name in header if name not in expected]
    removed = [name for name in expected if name not in header]
    return added, removed


def partition_dir(settings: Settings, source: str, period: str) -> Path:
    return bronze.partition_dir(settings.bronze_dir, source, period)


def _all_recorded_unchanged(
    settings: Settings, sources: list[str], period: str, last_modified: str | None
) -> bool:
    """True when every source of one download is already ingested at this timestamp."""
    if last_modified is None:
        return False
    for source in sources:
        recorded = manifest.lookup(settings.manifest_path, source, period)
        if (
            recorded is None
            or recorded.status != manifest.STATUS_INGESTED
            or recorded.last_modified != last_modified
        ):
            return False
    return True


def _unchanged_results(settings: Settings, sources: list[str], period: str) -> list[SnapshotResult]:
    """Report sources skipped by the HEAD check, echoing what the manifest holds."""
    results = []
    for source in sources:
        recorded = manifest.lookup(settings.manifest_path, source, period)
        results.append(
            SnapshotResult(
                source,
                period,
                manifest.STATUS_UNCHANGED,
                recorded.rows if recorded else None,
                recorded.file_hash if recorded else None,
            )
        )
    return results


def _ingest_csv(
    settings: Settings,
    *,
    source: str,
    period: str,
    csv_name: str,
    csv_bytes: bytes,
    last_modified: str | None,
    force: bool,
) -> SnapshotResult:
    """Hash one latin-1 CSV and, if it changed, write it to its bronze partition."""
    file_hash = bronze.sha256_hex(csv_bytes)
    recorded = manifest.lookup(settings.manifest_path, source, period)

    added, removed = header_drift(csv_bytes, EXPECTED_HEADERS[source])
    if added or removed:
        logger.warning(
            "header_drift",
            extra={"source": source, "period": period, "added": added, "removed": removed},
        )

    if not force and recorded is not None and recorded.file_hash == file_hash:
        manifest.upsert(
            settings.manifest_path,
            manifest.ManifestEntry.now(
                source=source,
                period=period,
                source_file=csv_name,
                file_hash=file_hash,
                last_modified=last_modified,
                rows=recorded.rows,
                status=manifest.STATUS_INGESTED,
            ),
        )
        logger.info(
            "snapshot_unchanged_same_hash",
            extra={"source": source, "period": period, "file_hash": file_hash},
        )
        return SnapshotResult(source, period, manifest.STATUS_UNCHANGED, recorded.rows, file_hash)

    with tempfile.TemporaryDirectory(prefix="fee-drag-") as tmp:
        csv_path = Path(tmp) / csv_name
        csv_path.write_bytes(bronze.to_utf8(csv_bytes))
        rows = bronze.write_csv_partition(
            csv_path,
            partition_dir(settings, source, period),
            source_file=csv_name,
            file_hash=file_hash,
            ingested_at=datetime.now(tz=UTC),
        )

    manifest.upsert(
        settings.manifest_path,
        manifest.ManifestEntry.now(
            source=source,
            period=period,
            source_file=csv_name,
            file_hash=file_hash,
            last_modified=last_modified,
            rows=rows,
            status=manifest.STATUS_INGESTED,
        ),
    )
    logger.info(
        "snapshot_ingested",
        extra={"source": source, "period": period, "rows": rows, "file_hash": file_hash},
    )
    return SnapshotResult(source, period, manifest.STATUS_INGESTED, rows, file_hash)


def _record_failure(
    settings: Settings, sources: list[str], file_name: str, status_code: int
) -> list[SnapshotResult]:
    period = snapshot_period(None)
    results = []
    for source in sources:
        logger.warning(
            "snapshot_unavailable",
            extra={"source": source, "period": period, "status": status_code},
        )
        manifest.upsert(
            settings.manifest_path,
            manifest.ManifestEntry.now(
                source=source,
                period=period,
                source_file=file_name,
                file_hash=None,
                last_modified=None,
                rows=None,
                status=manifest.STATUS_FAILED,
            ),
        )
        results.append(SnapshotResult(source, period, manifest.STATUS_FAILED, None, None))
    return results


def ingest_rcvm175(
    settings: Settings, *, client: httpx.Client, force: bool = False
) -> list[SnapshotResult]:
    """Ingest the three RCVM 175 registry files from their shared zip."""
    url = f"{settings.cvm_registry_base_url}/{RCVM175_ARCHIVE}"
    sources = list(RCVM175_MEMBERS.values())

    head = request_with_retry(client, "HEAD", url, settings=settings)
    last_modified = head.headers.get("last-modified")
    period = snapshot_period(last_modified)

    if not force and _all_recorded_unchanged(settings, sources, period, last_modified):
        logger.info(
            "snapshot_skipped_not_modified",
            extra={"archive": RCVM175_ARCHIVE, "period": period, "last_modified": last_modified},
        )
        return _unchanged_results(settings, sources, period)

    response = request_with_retry(client, "GET", url, settings=settings)
    with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        names = set(archive.namelist())
        missing = sorted(set(RCVM175_MEMBERS) - names)
        if missing:
            raise ArchiveError(f"{RCVM175_ARCHIVE} is missing {missing}; found {sorted(names)}")
        unexpected = sorted(names - set(RCVM175_MEMBERS))
        if unexpected:
            logger.warning(
                "archive_unexpected_members",
                extra={"archive": RCVM175_ARCHIVE, "members": unexpected},
            )
        members = {name: archive.read(name) for name in RCVM175_MEMBERS}

    return [
        _ingest_csv(
            settings,
            source=source,
            period=period,
            csv_name=member,
            csv_bytes=members[member],
            last_modified=last_modified,
            force=force,
        )
        for member, source in RCVM175_MEMBERS.items()
    ]


def ingest_legacy(
    settings: Settings, *, client: httpx.Client, force: bool = False
) -> SnapshotResult:
    """Ingest `cad_fi.csv`, the registry of funds not yet adapted to RCVM 175."""
    url = f"{settings.cvm_registry_base_url}/{LEGACY_FILE}"

    head = request_with_retry(client, "HEAD", url, settings=settings)
    last_modified = head.headers.get("last-modified")
    period = snapshot_period(last_modified)

    if not force and _all_recorded_unchanged(settings, [SOURCE_LEGACY], period, last_modified):
        logger.info(
            "snapshot_skipped_not_modified",
            extra={"source": SOURCE_LEGACY, "period": period, "last_modified": last_modified},
        )
        return _unchanged_results(settings, [SOURCE_LEGACY], period)[0]

    response = request_with_retry(client, "GET", url, settings=settings)
    return _ingest_csv(
        settings,
        source=SOURCE_LEGACY,
        period=period,
        csv_name=LEGACY_FILE,
        csv_bytes=response.content,
        last_modified=last_modified,
        force=force,
    )


def ingest(
    settings: Settings,
    *,
    client: httpx.Client | None = None,
    force: bool = False,
    skip_missing: bool = True,
) -> list[SnapshotResult]:
    """Ingest both registry downloads. One failing download does not block the other."""
    owns_client = client is None
    active = client or build_client(settings)
    results: list[SnapshotResult] = []

    try:
        try:
            results.extend(ingest_rcvm175(settings, client=active, force=force))
        except httpx.HTTPStatusError as exc:
            if not skip_missing:
                raise
            results.extend(
                _record_failure(
                    settings,
                    list(RCVM175_MEMBERS.values()),
                    RCVM175_ARCHIVE,
                    exc.response.status_code,
                )
            )

        try:
            results.append(ingest_legacy(settings, client=active, force=force))
        except httpx.HTTPStatusError as exc:
            if not skip_missing:
                raise
            results.extend(
                _record_failure(settings, [SOURCE_LEGACY], LEGACY_FILE, exc.response.status_code)
            )
    finally:
        if owns_client:
            active.close()

    return results
