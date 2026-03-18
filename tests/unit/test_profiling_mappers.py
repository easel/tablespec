"""Unit tests for profiling mapper classes."""

from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock

import pytest

# Check if PySpark is available
try:
    import pyspark  # noqa: F401

    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False


@pytest.fixture
def mock_spark_field():
    """Create a mock Spark StructField."""
    field = MagicMock()
    field.name = "test_column"
    field.nullable = True
    return field


@pytest.fixture
def mock_dataframe():
    """Create a mock Spark DataFrame."""
    df = MagicMock()
    df.schema.fields = []
    return df


@pytest.fixture
def sample_column_profile():
    """Create a sample ColumnProfile for testing."""
    from tablespec.profiling import ColumnProfile

    return ColumnProfile(
        column_name="test_col",
        completeness=0.95,
        approximate_num_distinct=100,
        data_type="String",
        minimum="A",
        maximum="Z",
        mean=50.5,
        standard_deviation=10.2,
    )


@pytest.fixture
def sample_dataframe_profile(sample_column_profile):
    """Create a sample DataFrameProfile for testing."""
    from tablespec.profiling import DataFrameProfile

    return DataFrameProfile(
        num_records=1000,
        columns={"test_col": sample_column_profile},
    )


@pytest.mark.skipif(not PYSPARK_AVAILABLE, reason="PySpark not installed")
class TestSparkToUmfMapper:
    """Test SparkToUmfMapper class."""

    def test_map_string_type_to_umf(self):
        """Test mapping StringType to UMF STRING."""
        from tablespec.profiling import SparkToUmfMapper

        mapper = SparkToUmfMapper()

        # Mock StringType
        mock_type = MagicMock()
        mock_type.__class__.__name__ = "StringType"

        result = mapper._map_spark_type(mock_type)
        assert result == "STRING"

    def test_map_integer_type_to_umf(self):
        """Test mapping IntegerType to UMF INTEGER."""
        from tablespec.profiling import SparkToUmfMapper

        mapper = SparkToUmfMapper()

        mock_type = MagicMock()
        mock_type.__class__.__name__ = "IntegerType"

        result = mapper._map_spark_type(mock_type)
        assert result == "INTEGER"

    def test_map_decimal_type_with_precision_scale(self):
        """Test mapping DecimalType includes precision and scale."""
        from tablespec.profiling import SparkToUmfMapper

        mapper = SparkToUmfMapper()

        # Mock DecimalType with precision and scale
        from pyspark.sql.types import DecimalType

        mock_field = MagicMock()
        mock_field.name = "price"
        mock_field.nullable = False
        mock_field.dataType = DecimalType(10, 2)

        result = mapper._map_field_to_column(mock_field)

        assert result["name"] == "price"
        assert result["data_type"] == "DECIMAL"
        assert result["precision"] == 10
        assert result["scale"] == 2
        assert result["nullable"] is False

    def test_map_nullable_field(self):
        """Test mapping nullable field preserves nullable flag."""
        from tablespec.profiling import SparkToUmfMapper

        mapper = SparkToUmfMapper()

        mock_field = MagicMock()
        mock_field.name = "optional_col"
        mock_field.nullable = True
        mock_field.dataType = MagicMock(__class__=type("StringType", (), {}))

        result = mapper._map_field_to_column(mock_field)

        assert result["nullable"] is True

    def test_unknown_spark_type_defaults_to_string(self):
        """Test unknown Spark types default to STRING."""
        from tablespec.profiling import SparkToUmfMapper

        mapper = SparkToUmfMapper()

        # Mock unknown type
        mock_type = MagicMock()
        mock_type.__class__.__name__ = "UnknownCustomType"

        result = mapper._map_spark_type(mock_type)
        assert result == "STRING"

    def test_map_dataframe_to_umf_structure(self, mock_dataframe):
        """Test complete DataFrame to UMF conversion structure."""
        from tablespec.profiling import SparkToUmfMapper

        # Mock schema with multiple fields
        field1 = MagicMock()
        field1.name = "id"
        field1.nullable = False
        field1.dataType = MagicMock(__class__=type("IntegerType", (), {}))

        field2 = MagicMock()
        field2.name = "name"
        field2.nullable = True
        field2.dataType = MagicMock(__class__=type("StringType", (), {}))

        mock_dataframe.schema.fields = [field1, field2]

        mapper = SparkToUmfMapper()
        result = mapper.map_dataframe_to_umf(mock_dataframe, "test_table", "source")

        assert result["table_name"] == "test_table"
        assert result["table_type"] == "source"
        assert len(result["columns"]) == 2
        assert result["columns"][0]["name"] == "id"
        assert result["columns"][1]["name"] == "name"


class TestDeequToUmfMapper:
    """Test DeequToUmfMapper class."""

    def test_enrich_umf_with_profiling_metadata(self, sample_dataframe_profile):
        """Test adding profiling metadata to UMF."""
        from tablespec.profiling import DeequToUmfMapper

        mapper = DeequToUmfMapper()

        umf = {
            "table_name": "test_table",
            "columns": [{"name": "test_col", "data_type": "STRING"}],
        }

        result = mapper.enrich_umf_with_profiling(
            umf, sample_dataframe_profile, sample_size=500
        )

        assert "profiling_metadata" in result
        assert result["profiling_metadata"]["tool"] == "pulseflow-profiler"
        assert result["profiling_metadata"]["version"] == "1.0.0"
        assert result["profiling_metadata"]["sample_size"] == 500
        assert result["profiling_metadata"]["total_rows"] == 1000
        assert "profiled_at" in result["profiling_metadata"]

        # Validate timestamp format
        timestamp = result["profiling_metadata"]["profiled_at"]
        datetime.fromisoformat(timestamp)  # Should not raise

    def test_column_profiling_section_structure(self, sample_column_profile):
        """Test profiling section structure for a column."""
        from tablespec.profiling import DeequToUmfMapper

        mapper = DeequToUmfMapper()

        result = mapper._build_profiling_section(sample_column_profile)

        assert "completeness" in result
        assert result["completeness"] == 0.95
        assert result["approximate_num_distinct"] == 100
        assert result["data_type_inferred"] == "String"
        assert "statistics" in result
        assert result["statistics"]["min"] == "A"
        assert result["statistics"]["max"] == "Z"
        assert result["statistics"]["mean"] == 50.5
        assert result["statistics"]["stddev"] == 10.2

    def test_override_nullable_based_on_completeness(self, sample_dataframe_profile):
        """Test nullable is overridden when completeness < 1.0."""
        from tablespec.profiling import DeequToUmfMapper

        mapper = DeequToUmfMapper()

        umf = {
            "table_name": "test_table",
            "columns": [{"name": "test_col", "data_type": "STRING", "nullable": False}],
        }

        # Profile has completeness = 0.95 (< 1.0)
        result = mapper.enrich_umf_with_profiling(umf, sample_dataframe_profile)

        # Should override nullable to True
        assert result["columns"][0]["nullable"] is True

    def test_nullable_not_overridden_for_complete_columns(
        self, sample_dataframe_profile
    ):
        """Test nullable is not overridden when completeness = 1.0."""
        from tablespec.profiling import (
            ColumnProfile,
            DataFrameProfile,
            DeequToUmfMapper,
        )

        # Create profile with completeness = 1.0
        complete_profile = ColumnProfile(
            column_name="test_col",
            completeness=1.0,
        )

        df_profile = DataFrameProfile(
            num_records=1000,
            columns={"test_col": complete_profile},
        )

        mapper = DeequToUmfMapper()

        umf = {
            "table_name": "test_table",
            "columns": [{"name": "test_col", "data_type": "STRING", "nullable": False}],
        }

        result = mapper.enrich_umf_with_profiling(umf, df_profile)

        # Should NOT override nullable
        assert result["columns"][0]["nullable"] is False

    def test_statistics_rounding(self):
        """Test statistics are rounded to 4 decimal places."""
        from tablespec.profiling import ColumnProfile, DeequToUmfMapper

        # Create profile with high-precision values
        profile = ColumnProfile(
            column_name="test_col",
            completeness=1.0,
            mean=123.456789012,
            standard_deviation=45.678901234,
        )

        mapper = DeequToUmfMapper()
        result = mapper._build_profiling_section(profile)

        # Should be rounded to 4 decimals
        assert result["statistics"]["mean"] == 123.4568
        assert result["statistics"]["stddev"] == 45.6789

    def test_nullable_dict_preserved_when_completeness_low(self):
        """Test that dict-style nullable is preserved (not replaced with bool) on low completeness."""
        from tablespec.profiling import ColumnProfile, DataFrameProfile, DeequToUmfMapper

        profile = DataFrameProfile(
            num_records=1000,
            columns={
                "col_a": ColumnProfile(column_name="col_a", completeness=0.95),
            },
        )

        mapper = DeequToUmfMapper()
        umf = {
            "table_name": "test",
            "columns": [
                {"name": "col_a", "data_type": "VARCHAR", "nullable": {"MD": False, "MP": True}},
            ],
        }

        result = mapper.enrich_umf_with_profiling(umf, profile)

        # Should be a dict with all values set to True, not a plain bool
        nullable = result["columns"][0]["nullable"]
        assert isinstance(nullable, dict)
        assert nullable == {"MD": True, "MP": True}

    def test_nullable_bool_when_no_existing_nullable(self):
        """Test that nullable is set to bool True when no existing nullable dict."""
        from tablespec.profiling import ColumnProfile, DataFrameProfile, DeequToUmfMapper

        profile = DataFrameProfile(
            num_records=1000,
            columns={
                "col_a": ColumnProfile(column_name="col_a", completeness=0.95),
            },
        )

        mapper = DeequToUmfMapper()
        umf = {
            "table_name": "test",
            "columns": [{"name": "col_a", "data_type": "VARCHAR"}],
        }

        result = mapper.enrich_umf_with_profiling(umf, profile)
        assert result["columns"][0]["nullable"] is True

    def test_num_records_propagated_to_column_profiling(self):
        """Test that DataFrameProfile.num_records is propagated into each column's profiling."""
        from tablespec.profiling import ColumnProfile, DataFrameProfile, DeequToUmfMapper

        profile = DataFrameProfile(
            num_records=5000,
            columns={
                "col_a": ColumnProfile(column_name="col_a", completeness=1.0),
                "col_b": ColumnProfile(column_name="col_b", completeness=0.9),
            },
        )

        mapper = DeequToUmfMapper()
        umf = {
            "table_name": "test",
            "columns": [
                {"name": "col_a", "data_type": "INTEGER"},
                {"name": "col_b", "data_type": "VARCHAR"},
            ],
        }

        result = mapper.enrich_umf_with_profiling(umf, profile)

        assert result["columns"][0]["profiling"]["num_records"] == 5000
        assert result["columns"][1]["profiling"]["num_records"] == 5000

    def test_distinct_values_in_profiling_section(self):
        """Test distinct_values are written to profiling section."""
        from tablespec.profiling import ColumnProfile, DeequToUmfMapper

        profile = ColumnProfile(
            column_name="status",
            completeness=1.0,
            approximate_num_distinct=3,
            distinct_values=["Active", "Inactive", "Pending"],
        )

        mapper = DeequToUmfMapper()
        result = mapper._build_profiling_section(profile)

        assert result["distinct_values"] == ["Active", "Inactive", "Pending"]

    def test_distinct_values_omitted_when_none(self):
        """Test distinct_values not present when not provided."""
        from tablespec.profiling import ColumnProfile, DeequToUmfMapper

        profile = ColumnProfile(column_name="col", completeness=1.0)

        mapper = DeequToUmfMapper()
        result = mapper._build_profiling_section(profile)

        assert "distinct_values" not in result

    def test_string_lengths_in_profiling_section(self):
        """Test string length stats are written to profiling section."""
        from tablespec.profiling import ColumnProfile, DeequToUmfMapper

        profile = ColumnProfile(
            column_name="name",
            completeness=1.0,
            string_length_min=2,
            string_length_max=50,
        )

        mapper = DeequToUmfMapper()
        result = mapper._build_profiling_section(profile)

        assert result["string_lengths"]["min_length"] == 2
        assert result["string_lengths"]["max_length"] == 50

    def test_string_lengths_omitted_when_none(self):
        """Test string_lengths not present when not provided."""
        from tablespec.profiling import ColumnProfile, DeequToUmfMapper

        profile = ColumnProfile(column_name="col", completeness=1.0)

        mapper = DeequToUmfMapper()
        result = mapper._build_profiling_section(profile)

        assert "string_lengths" not in result

    def test_profiling_section_without_statistics(self):
        """Test profiling section when no statistics are available."""
        from tablespec.profiling import ColumnProfile, DeequToUmfMapper

        # Create profile with only completeness
        profile = ColumnProfile(
            column_name="test_col",
            completeness=0.80,
        )

        mapper = DeequToUmfMapper()
        result = mapper._build_profiling_section(profile)

        assert result["completeness"] == 0.80
        # Should not have statistics section if no stats available
        assert "statistics" not in result
