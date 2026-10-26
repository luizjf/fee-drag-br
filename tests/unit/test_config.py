"""Settings come from the environment only, and bad values fail loudly."""

from __future__ import annotations

from pathlib import Path

import pytest

from fee_drag.config import ConfigError, Settings, load_env_file


def test_load_env_file_parses_comments_quotes_and_export(tmp_path, monkeypatch):
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                "# a comment",
                "",
                "FEE_DRAG_LOG_LEVEL=debug",
                'export HTTP_TIMEOUT_SECONDS="30"',
                "  BCB_SGS_BASE_URL = 'https://example.test/serie'  ",
                "NOT_A_PAIR",
            ]
        ),
        encoding="utf-8",
    )
    for key in ("FEE_DRAG_LOG_LEVEL", "HTTP_TIMEOUT_SECONDS", "BCB_SGS_BASE_URL"):
        monkeypatch.delenv(key, raising=False)

    applied = load_env_file(env_file)

    assert applied == {
        "FEE_DRAG_LOG_LEVEL": "debug",
        "HTTP_TIMEOUT_SECONDS": "30",
        "BCB_SGS_BASE_URL": "https://example.test/serie",
    }


def test_missing_env_file_is_not_an_error(tmp_path):
    assert load_env_file(tmp_path / "nope.env") == {}


def test_process_environment_wins_over_the_file(tmp_path, monkeypatch):
    env_file = tmp_path / ".env"
    env_file.write_text("FEE_DRAG_LOG_LEVEL=debug\n", encoding="utf-8")
    monkeypatch.setenv("FEE_DRAG_LOG_LEVEL", "WARNING")

    assert load_env_file(env_file) == {}
    assert load_env_file(env_file, override=True) == {"FEE_DRAG_LOG_LEVEL": "debug"}


def test_defaults_apply_and_relative_paths_anchor_to_project_root(tmp_path, clean_env):
    loaded = Settings.from_env(project_root=tmp_path, read_env_file=False)

    assert loaded.project_root == tmp_path
    assert loaded.bronze_dir == tmp_path / "data" / "bronze"
    assert loaded.manifest_path == tmp_path / "data" / "bronze" / "_manifest.parquet"
    assert loaded.http_max_retries == 5
    assert loaded.log_level == "INFO"
    assert loaded.cvm_daily_reports_base_url.endswith("/INF_DIARIO/DADOS")


def test_absolute_path_overrides_are_kept_as_given(tmp_path, clean_env, monkeypatch):
    elsewhere = (tmp_path / "elsewhere" / "bronze").resolve()
    monkeypatch.setenv("FEE_DRAG_BRONZE_DIR", str(elsewhere))

    assert Settings.from_env(project_root=tmp_path, read_env_file=False).bronze_dir == elsewhere


def test_trailing_slash_is_stripped_from_base_urls(tmp_path, clean_env, monkeypatch):
    monkeypatch.setenv("CVM_DAILY_REPORTS_BASE_URL", "https://example.test/dados/")

    loaded = Settings.from_env(project_root=tmp_path, read_env_file=False)

    assert loaded.cvm_daily_reports_base_url == "https://example.test/dados"


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("HTTP_TIMEOUT_SECONDS", "not-a-number"),
        ("HTTP_TIMEOUT_SECONDS", "0"),
        ("HTTP_TIMEOUT_SECONDS", "-1"),
        ("HTTP_MAX_RETRIES", "-1"),
        ("HTTP_MAX_RETRIES", "1.5"),
        ("HTTP_BACKOFF_BASE_SECONDS", "0"),
        ("INGEST_START_MONTH", "2021-13"),
        ("INGEST_START_MONTH", "202101"),
    ],
)
def test_invalid_values_raise_config_error(tmp_path, clean_env, monkeypatch, name, value):
    monkeypatch.setenv(name, value)

    with pytest.raises(ConfigError, match=name):
        Settings.from_env(project_root=tmp_path, read_env_file=False)


def test_settings_are_frozen(settings):
    with pytest.raises(AttributeError):
        settings.bronze_dir = Path("/tmp")
