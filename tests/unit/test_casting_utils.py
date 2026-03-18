"""Tests for casting_utils module - pure Python parts only.

PySpark-dependent functions are tested by checking they raise ImportError
when PySpark is unavailable, or skipped if PySpark is present.
"""

from __future__ import annotations

import pytest

from tablespec.casting_utils import (
    COMMON_DATE_FORMATS,
    COMMON_TIMESTAMP_FORMATS,
    build_flexible_formats,
    convert_umf_format_to_spark,
)

pytestmark = [pytest.mark.no_spark, pytest.mark.fast]


class TestConvertUmfFormatToSpark:
    """Test UMF format to Spark SimpleDateFormat conversion."""

    def test_iso_date(self):
        """YYYY-MM-DD converts to yyyy-MM-dd."""
        assert convert_umf_format_to_spark("YYYY-MM-DD") == "yyyy-MM-dd"

    def test_us_date_slashes(self):
        """MM/DD/YYYY converts to MM/dd/yyyy."""
        assert convert_umf_format_to_spark("MM/DD/YYYY") == "MM/dd/yyyy"

    def test_us_date_dashes(self):
        """MM-DD-YYYY converts to MM-dd-yyyy."""
        assert convert_umf_format_to_spark("MM-DD-YYYY") == "MM-dd-yyyy"

    def test_compact_date(self):
        """YYYYMMDD converts to yyyyMMdd."""
        assert convert_umf_format_to_spark("YYYYMMDD") == "yyyyMMdd"

    def test_timestamp_with_seconds(self):
        """YYYY-MM-DD HH:MM:SS converts correctly with minutes as mm."""
        result = convert_umf_format_to_spark("YYYY-MM-DD HH:MM:SS")
        assert result == "yyyy-MM-dd HH:mm:ss"

    def test_timestamp_iso_t_separator(self):
        """ISO timestamp with T separator gets quoted T."""
        result = convert_umf_format_to_spark("YYYY-MM-DDTHH:MM:SS")
        assert result == "yyyy-MM-dd'T'HH:mm:ss"

    def test_two_digit_year(self):
        """YY converts to yy."""
        assert convert_umf_format_to_spark("MM/DD/YY") == "MM/dd/yy"

    def test_non_padded_month_day(self):
        """M/D/YYYY converts to M/d/yyyy."""
        assert convert_umf_format_to_spark("M/D/YYYY") == "M/d/yyyy"

    def test_12_hour_with_ampm(self):
        """12-hour format with AM/PM marker."""
        result = convert_umf_format_to_spark("MM/DD/YYYY hh:mm:ss A")
        assert result == "MM/dd/yyyy hh:mm:ss a"

    def test_fractional_seconds_preserved(self):
        """Fractional seconds (.SSSSSS) stay uppercase."""
        result = convert_umf_format_to_spark("YYYY-MM-DD HH:MM:SS.SSSSSS")
        assert result == "yyyy-MM-dd HH:mm:ss.SSSSSS"

    def test_timestamp_lowercase_minutes(self):
        """Lowercase mm for minutes in UMF format."""
        result = convert_umf_format_to_spark("YYYY-MM-DD HH:mm:ss")
        assert result == "yyyy-MM-dd HH:mm:ss"

    def test_non_padded_hour_24(self):
        """Non-padded 24-hour format H."""
        result = convert_umf_format_to_spark("YYYY-MM-DD H:MM:SS")
        assert result == "yyyy-MM-dd H:mm:ss"

    def test_non_padded_hour_12(self):
        """Non-padded 12-hour format h."""
        result = convert_umf_format_to_spark("M/D/YYYY h:mm A")
        assert result == "M/d/yyyy h:mm a"

    def test_mmddyyyy_compact(self):
        """MMDDYYYY converts to MMddyyyy."""
        assert convert_umf_format_to_spark("MMDDYYYY") == "MMddyyyy"


class TestCommonFormats:
    """Test that format tuples are defined and non-empty."""

    def test_common_date_formats_not_empty(self):
        """COMMON_DATE_FORMATS is a non-empty tuple."""
        assert isinstance(COMMON_DATE_FORMATS, tuple)
        assert len(COMMON_DATE_FORMATS) > 0

    def test_common_timestamp_formats_not_empty(self):
        """COMMON_TIMESTAMP_FORMATS is a non-empty tuple."""
        assert isinstance(COMMON_TIMESTAMP_FORMATS, tuple)
        assert len(COMMON_TIMESTAMP_FORMATS) > 0

    def test_date_formats_are_strings(self):
        """All date formats are strings."""
        for fmt in COMMON_DATE_FORMATS:
            assert isinstance(fmt, str)

    def test_timestamp_formats_are_strings(self):
        """All timestamp formats are strings."""
        for fmt in COMMON_TIMESTAMP_FORMATS:
            assert isinstance(fmt, str)


class TestBuildFlexibleFormats:
    """Test build_flexible_formats for date/timestamp format priority."""

    def test_date_with_primary(self):
        """Primary format comes first for DATE."""
        result = build_flexible_formats("DATE", "MM/DD/YYYY")
        assert result[0] == "MM/DD/YYYY"
        assert len(result) > 1

    def test_timestamp_with_primary(self):
        """Primary format comes first for TIMESTAMP."""
        result = build_flexible_formats("TIMESTAMP", "YYYY-MM-DD HH:MM:SS")
        assert result[0] == "YYYY-MM-DD HH:MM:SS"
        assert len(result) > 1

    def test_no_primary(self):
        """None primary still returns formats."""
        result = build_flexible_formats("DATE", None)
        assert len(result) > 0

    def test_unsupported_type_returns_empty(self):
        """Non-date/timestamp types return empty list."""
        assert build_flexible_formats("INTEGER", "whatever") == []
        assert build_flexible_formats("STRING", None) == []

    def test_no_duplicates(self):
        """Returned list has no duplicate formats."""
        result = build_flexible_formats("DATE", "YYYY-MM-DD")
        assert len(result) == len(set(result))

    def test_fallback_formats_included(self):
        """Fallback formats appear after primary."""
        result = build_flexible_formats("DATE", "YYYY-MM-DD", ["MM/DD/YYYY", "YYYYMMDD"])
        assert result[0] == "YYYY-MM-DD"
        idx_mm = result.index("MM/DD/YYYY")
        idx_compact = result.index("YYYYMMDD")
        assert idx_mm < idx_compact  # fallback order preserved
        assert idx_mm > 0  # after primary

    def test_case_insensitive_type(self):
        """Target type is case-insensitive."""
        result_lower = build_flexible_formats("date", "YYYY-MM-DD")
        result_upper = build_flexible_formats("DATE", "YYYY-MM-DD")
        assert result_lower == result_upper

    def test_common_date_formats_included(self):
        """COMMON_DATE_FORMATS entries appear in result for DATE type."""
        result = build_flexible_formats("DATE", None)
        for fmt in COMMON_DATE_FORMATS:
            assert fmt in result


class TestSparkDependentFunctions:
    """Test that PySpark-dependent functions handle missing PySpark."""

    def test_cast_column_with_format_raises_without_spark(self, monkeypatch):
        """cast_column_with_format raises ImportError when SPARK_AVAILABLE is False."""
        import tablespec.casting_utils as mod

        monkeypatch.setattr(mod, "SPARK_AVAILABLE", False)
        with pytest.raises(ImportError, match="PySpark is required"):
            mod.cast_column_with_format(None, "DATE", "YYYY-MM-DD")

    def test_is_excel_serial_date_raises_without_spark(self, monkeypatch):
        """is_excel_serial_date raises ImportError when SPARK_AVAILABLE is False."""
        import tablespec.casting_utils as mod

        monkeypatch.setattr(mod, "SPARK_AVAILABLE", False)
        with pytest.raises(ImportError, match="PySpark is required"):
            mod.is_excel_serial_date(None)

    def test_convert_excel_serial_to_date_raises_without_spark(self, monkeypatch):
        """convert_excel_serial_to_date raises ImportError when SPARK_AVAILABLE is False."""
        import tablespec.casting_utils as mod

        monkeypatch.setattr(mod, "SPARK_AVAILABLE", False)
        with pytest.raises(ImportError, match="PySpark is required"):
            mod.convert_excel_serial_to_date(None)
