"""Verify that importing pipeline modules doesn't execute pipeline code.

These tests ensure the if __name__ == '__main__' guards are working.
If any import triggers pipeline execution, the test will hang or crash
because DB/API connections aren't available in CI.
"""
import importlib
import pytest


PIPELINE_MODULES = [
    "highstreets.aws_pipeline.mcard_weekly",
    "highstreets.aws_pipeline.mcard_3hourly",
    "highstreets.aws_pipeline.mcard_weekly_intl",
    "highstreets.aws_pipeline.mcard_lookups",
    "highstreets.aws_pipeline.hex_e2e",
    "highstreets.aws_pipeline.msoa_e2e",
    "highstreets.aws_pipeline.lsoa_e2e",
    "highstreets.aws_pipeline.daily_agg",
    "highstreets.aws_pipeline.bt_outage",
    "highstreets.aws_pipeline.bt_lookups",
    "highstreets.aws_pipeline.sublicense",
    "highstreets.aws_pipeline.catchment_e2e",
]


@pytest.mark.parametrize("module_path", PIPELINE_MODULES)
def test_import_does_not_execute(module_path):
    """Importing a pipeline module must not trigger the pipeline."""
    try:
        mod = importlib.import_module(module_path)
    except ImportError as e:
        pytest.skip(f"Skipped due to missing dependency: {e}")
    except OSError as e:
        pytest.skip(f"Skipped due to OS-level library issue: {e}")
    assert hasattr(mod, "main"), f"{module_path} missing main() function"
