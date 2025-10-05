"""Test type mapping utilities."""

from __future__ import annotations

import pytest

from tablespec.type_mappings import map_to_gx_spark_type


class TestGXSparkTypeMapping:
    """Test UMF to GX Spark type mapping."""

    def test_string_types(self):
        """Test VARCHAR and STRING map to StringType."""
        assert map_to_gx_spark_type("VARCHAR") == "StringType"
        assert map_to_gx_spark_type("STRING") == "StringType"
        assert map_to_gx_spark_type("varchar") == "StringType"
        assert map_to_gx_spark_type("string") == "StringType"

    def test_integer_types(self):
        """Test various integer types map correctly."""
        assert map_to_gx_spark_type("INTEGER") == "IntegerType"
        assert map_to_gx_spark_type("INT") == "IntegerType"
        assert map_to_gx_spark_type("BIGINT") == "LongType"
        assert map_to_gx_spark_type("SMALLINT") == "ShortType"
        assert map_to_gx_spark_type("TINYINT") == "ByteType"

    def test_numeric_types(self):
        """Test decimal and floating point types."""
        assert map_to_gx_spark_type("DECIMAL") == "DecimalType"
        assert map_to_gx_spark_type("FLOAT") == "FloatType"
        assert map_to_gx_spark_type("DOUBLE") == "DoubleType"

    def test_boolean_type(self):
        """Test BOOLEAN maps to BooleanType."""
        assert map_to_gx_spark_type("BOOLEAN") == "BooleanType"

    def test_date_type(self):
        """Test DATE maps to StringType (YYYYMMDD format)."""
        assert map_to_gx_spark_type("DATE") == "StringType"

    def test_timestamp_type(self):
        """Test TIMESTAMP maps to TimestampType."""
        assert map_to_gx_spark_type("TIMESTAMP") == "TimestampType"

    def test_case_insensitive(self):
        """Test that type mapping is case-insensitive."""
        assert map_to_gx_spark_type("integer") == "IntegerType"
        assert map_to_gx_spark_type("Integer") == "IntegerType"
        assert map_to_gx_spark_type("INTEGER") == "IntegerType"

    def test_unknown_type_defaults_to_string(self):
        """Test unknown types default to StringType."""
        assert map_to_gx_spark_type("UNKNOWN_TYPE") == "StringType"
        assert map_to_gx_spark_type("CUSTOM") == "StringType"
        assert map_to_gx_spark_type("") == "StringType"

    @pytest.mark.parametrize(
        ("umf_type", "expected_gx_type"),
        [
            ("VARCHAR", "StringType"),
            ("STRING", "StringType"),
            ("INTEGER", "IntegerType"),
            ("INT", "IntegerType"),
            ("BIGINT", "LongType"),
            ("SMALLINT", "ShortType"),
            ("TINYINT", "ByteType"),
            ("DECIMAL", "DecimalType"),
            ("FLOAT", "FloatType"),
            ("DOUBLE", "DoubleType"),
            ("BOOLEAN", "BooleanType"),
            ("DATE", "StringType"),
            ("TIMESTAMP", "TimestampType"),
        ],
    )
    def test_all_supported_mappings(self, umf_type: str, expected_gx_type: str):
        """Test all supported type mappings."""
        assert map_to_gx_spark_type(umf_type) == expected_gx_type
