# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Repository scaffold: `uv`-managed `pyproject.toml` on Python 3.12 with runtime
  (httpx, polars, pyarrow, duckdb) and dev (pytest, ruff, pre-commit) dependencies.
- Directory layout for the planned layers: `src/fee_drag/`, `dbt/`, `orchestration/`,
  `app/`, `tests/`, `docs/`.
- `Makefile` with the project commands; targets whose implementation has not landed yet
  print `not implemented yet`.
- Tooling config: `.gitignore`, `.env.example`, `.pre-commit-config.yaml` and `ruff`
  settings in `pyproject.toml`.
- Bilingual `README.md` / `README.pt-BR.md`, `LICENSE` (MIT) and this changelog.
- ADR template plus ADR-001 to ADR-006 in `docs/adr/`.
- Documentation stubs in `docs/` and a smoke test covering package import.

[Unreleased]: https://github.com/fee-drag-br/fee-drag-br/compare/main...HEAD
