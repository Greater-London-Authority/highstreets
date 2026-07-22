"""Verify core library classes exist and have expected methods.

These tests do not require a database connection. They only check that
the class structure matches what the pipeline scripts expect.
"""
import inspect
import pytest


def test_datawriter_methods():
    try:
        from highstreets.data_source_sink.datawriter import DataWriter
    except (ImportError, OSError) as e:
        pytest.skip(f"Skipped due to missing dependency: {e}")

    assert hasattr(DataWriter, "safe_append_data")
    assert hasattr(DataWriter, "append_chunk")
    assert hasattr(DataWriter, "append_data_to_postgres")
    assert hasattr(DataWriter, "truncate_and_load_to_postgres")
    assert hasattr(DataWriter, "export_table_by_year_to_s3")
    assert hasattr(DataWriter, "upload_data_to_lds")


def test_fileprocessor_cache_params():
    """mcard_adjust_weekly must accept cached lookup arguments."""
    try:
        from highstreets.data_transformation.mcard_weekly_processor import FileProcessor
    except (ImportError, OSError) as e:
        pytest.skip(f"Skipped due to missing dependency: {e}")

    sig = inspect.signature(FileProcessor.mcard_adjust_weekly)
    params = list(sig.parameters.keys())
    assert "cached_inner_outer_quad" in params
    assert "cached_adjustment_factors" in params
    assert "cached_cpi_data" in params

    # All cache params must default to None (backward compatible)
    for cache_param in ["cached_inner_outer_quad", "cached_adjustment_factors", "cached_cpi_data"]:  # noqa: E501
        assert sig.parameters[cache_param].default is None, (
            f"{cache_param} must default to None for backward compatibility"
        )


def test_sublicense_manager_not_processor():
    """sublicense.py must use SublicenseManager, not the deleted SublicenseProcessor."""
    from highstreets.core.sublicense_manager import SublicenseManager

    assert hasattr(SublicenseManager, "process_sublicense_complete")

    # SublicenseProcessor should no longer be importable
    import highstreets.core.processors as processors

    assert not hasattr(processors, "SublicenseProcessor")


def test_sql_manager_has_query_methods():
    from highstreets.core.sql_manager import SQLManager

    assert hasattr(SQLManager, "get_query")
    assert hasattr(SQLManager, "execute_query")
