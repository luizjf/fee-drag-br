"""Smoke tests: the package is installed and importable. No network, no data."""

import fee_drag
from fee_drag import ingest


def test_package_exposes_a_version():
    assert isinstance(fee_drag.__version__, str)
    assert fee_drag.__version__


def test_ingest_subpackage_is_importable():
    assert ingest.__name__ == "fee_drag.ingest"
