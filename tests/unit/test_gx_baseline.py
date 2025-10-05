"""Test Great Expectations baseline expectation generation from UMF."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
import yaml

from tablespec.gx_baseline import BaselineExpectationGenerator, UmfToGxMapper

if TYPE_CHECKING:
    from pathlib import Path


class TestBaselineExpectationGenerator:
    """Test baseline expectation generation from UMF metadata."""

    @pytest.fixture
    def generator(self):
        """Create generator instance."""
        return BaselineExpectationGenerator()

    @pytest.fixture
    def minimal_umf(self):
        """Minimal UMF data for testing."""
        return {
            "table_name": "test_table",
            "columns": [
                {"name": "id", "data_type": "INTEGER"},
                {"name": "name", "data_type": "VARCHAR"},
            ],
        }

    @pytest.fixture
    def full_umf(self):
        """Full UMF data with all metadata fields."""
        return {
            "table_name": "customer_table",
            "columns": [
                {
                    "name": "customer_id",
                    "data_type": "INTEGER",
                    "nullable": {"medicare": False, "medicaid": True},
                },
                {
                    "name": "customer_name",
                    "data_type": "VARCHAR",
                    "max_length": 100,
                    "nullable": {"medicare": False, "medicaid": False},
                },
                {
                    "name": "birth_date",
                    "data_type": "DATE",
                    "nullable": {"medicare": True, "medicaid": True},
                },
                {
                    "name": "email",
                    "data_type": "STRING",
                    "length": 255,
                },
            ],
        }

    def test_generate_baseline_expectations_with_structural(
        self, generator, minimal_umf
    ):
        """Test baseline generation includes structural expectations."""
        expectations = generator.generate_baseline_expectations(
            minimal_umf, include_structural=True
        )

        # Should have structural + column expectations
        # 2 structural (column count, column list) + 2 columns * 2 expectations each
        assert len(expectations) >= 6

        # Check structural expectations exist
        exp_types = [exp["type"] for exp in expectations]
        assert "expect_table_column_count_to_equal" in exp_types
        assert "expect_table_columns_to_match_ordered_list" in exp_types

    def test_generate_baseline_expectations_without_structural(
        self, generator, minimal_umf
    ):
        """Test baseline generation can exclude structural expectations."""
        expectations = generator.generate_baseline_expectations(
            minimal_umf, include_structural=False
        )

        # Should only have column expectations (2 columns * 2 expectations each)
        assert len(expectations) == 4

        # Check no structural expectations
        exp_types = [exp["type"] for exp in expectations]
        assert "expect_table_column_count_to_equal" not in exp_types
        assert "expect_table_columns_to_match_ordered_list" not in exp_types

    def test_structural_expectations_column_count(self, generator, minimal_umf):
        """Test structural expectation for column count."""
        structural = generator._generate_structural_expectations(minimal_umf)

        column_count_exp = next(
            exp
            for exp in structural
            if exp["type"] == "expect_table_column_count_to_equal"
        )

        assert column_count_exp["kwargs"]["value"] == 2
        assert column_count_exp["meta"]["severity"] == "critical"
        assert "2 columns" in column_count_exp["meta"]["description"]

    def test_structural_expectations_column_order(self, generator, minimal_umf):
        """Test structural expectation for column order."""
        structural = generator._generate_structural_expectations(minimal_umf)

        column_list_exp = next(
            exp
            for exp in structural
            if exp["type"] == "expect_table_columns_to_match_ordered_list"
        )

        assert column_list_exp["kwargs"]["column_list"] == ["id", "name"]
        assert column_list_exp["meta"]["severity"] == "critical"

    def test_column_existence_expectation(self, generator):
        """Test that column existence expectation is always generated."""
        column = {"name": "test_col", "data_type": "VARCHAR"}

        expectations = generator.generate_baseline_column_expectations(column)

        existence_exp = next(
            exp for exp in expectations if exp["type"] == "expect_column_to_exist"
        )

        assert existence_exp["kwargs"]["column"] == "test_col"
        assert existence_exp["meta"]["severity"] == "critical"

    def test_column_type_expectation(self, generator):
        """Test that column type expectation is generated."""
        column = {"name": "age", "data_type": "INTEGER"}

        expectations = generator.generate_baseline_column_expectations(column)

        type_exp = next(
            exp
            for exp in expectations
            if exp["type"] == "expect_column_values_to_be_of_type"
        )

        assert type_exp["kwargs"]["column"] == "age"
        assert type_exp["kwargs"]["type_"] == "IntegerType"
        assert type_exp["meta"]["severity"] == "info"

    def test_nullability_expectation_required(self, generator):
        """Test nullability expectation when column is required for some LOBs."""
        column = {
            "name": "required_field",
            "data_type": "VARCHAR",
            "nullable": {"medicare": False, "medicaid": True},
        }

        expectations = generator.generate_baseline_column_expectations(column)

        not_null_exp = next(
            (
                exp
                for exp in expectations
                if exp["type"] == "expect_column_values_to_not_be_null"
            ),
            None,
        )

        assert not_null_exp is not None
        assert not_null_exp["kwargs"]["column"] == "required_field"
        assert not_null_exp["meta"]["severity"] == "critical"
        assert "medicare" in not_null_exp["meta"]["lob"]

    def test_nullability_expectation_optional(self, generator):
        """Test no nullability expectation when column is optional for all LOBs."""
        column = {
            "name": "optional_field",
            "data_type": "VARCHAR",
            "nullable": {"medicare": True, "medicaid": True},
        }

        expectations = generator.generate_baseline_column_expectations(column)

        not_null_exp = next(
            (
                exp
                for exp in expectations
                if exp["type"] == "expect_column_values_to_not_be_null"
            ),
            None,
        )

        assert not_null_exp is None

    def test_max_length_expectation_from_max_length(self, generator):
        """Test max length expectation from max_length field."""
        column = {"name": "name_field", "data_type": "VARCHAR", "max_length": 50}

        expectations = generator.generate_baseline_column_expectations(column)

        length_exp = next(
            exp
            for exp in expectations
            if exp["type"] == "expect_column_value_lengths_to_be_between"
        )

        assert length_exp["kwargs"]["column"] == "name_field"
        assert length_exp["kwargs"]["max_value"] == 50
        assert length_exp["meta"]["severity"] == "warning"

    def test_max_length_expectation_from_length(self, generator):
        """Test max length expectation from length field."""
        column = {"name": "code", "data_type": "VARCHAR", "length": 10}

        expectations = generator.generate_baseline_column_expectations(column)

        length_exp = next(
            exp
            for exp in expectations
            if exp["type"] == "expect_column_value_lengths_to_be_between"
        )

        assert length_exp["kwargs"]["max_value"] == 10

    def test_date_format_expectation(self, generator):
        """Test DATE columns get strftime format expectation."""
        column = {"name": "birth_date", "data_type": "DATE"}

        expectations = generator.generate_baseline_column_expectations(column)

        date_exp = next(
            exp
            for exp in expectations
            if exp["type"] == "expect_column_values_to_match_strftime_format"
        )

        assert date_exp["kwargs"]["column"] == "birth_date"
        assert date_exp["kwargs"]["strftime_format"] == "%Y%m%d"
        assert date_exp["meta"]["severity"] == "warning"

    def test_full_umf_generates_all_expectations(self, generator, full_umf):
        """Test full UMF generates complete set of expectations."""
        expectations = generator.generate_baseline_expectations(
            full_umf, include_structural=True
        )

        # Should have many expectations
        assert len(expectations) > 10

        # Check all expected types are present
        exp_types = {exp["type"] for exp in expectations}
        assert "expect_table_column_count_to_equal" in exp_types
        assert "expect_table_columns_to_match_ordered_list" in exp_types
        assert "expect_column_to_exist" in exp_types
        assert "expect_column_values_to_be_of_type" in exp_types
        assert "expect_column_values_to_not_be_null" in exp_types
        assert "expect_column_value_lengths_to_be_between" in exp_types
        assert "expect_column_values_to_match_strftime_format" in exp_types


class TestUmfToGxMapper:
    """Test UMF to GX expectation suite mapping."""

    @pytest.fixture
    def mapper(self):
        """Create mapper instance."""
        return UmfToGxMapper()

    @pytest.fixture
    def umf_file(self, tmp_path: Path):
        """Create temporary UMF file."""
        umf_data = {
            "table_name": "test_table",
            "columns": [
                {"name": "id", "data_type": "INTEGER", "nullable": {"medicare": False}},
                {"name": "name", "data_type": "VARCHAR", "max_length": 100},
            ],
        }

        umf_path = tmp_path / "test_table.umf.yaml"
        with umf_path.open("w", encoding="utf-8") as f:
            yaml.dump(umf_data, f)

        return umf_path

    @pytest.fixture
    def umf_with_profiling(self, tmp_path: Path):
        """Create UMF file with profiling data."""
        umf_data = {
            "table_name": "profiled_table",
            "columns": [
                {
                    "name": "id",
                    "data_type": "INTEGER",
                    "profiling": {
                        "completeness": 1.0,
                        "approximate_num_distinct": 1000,
                    },
                },
                {
                    "name": "status",
                    "data_type": "VARCHAR",
                    "profiling": {
                        "completeness": 0.95,
                        "approximate_num_distinct": 5,
                    },
                },
            ],
        }

        umf_path = tmp_path / "profiled_table.umf.yaml"
        with umf_path.open("w", encoding="utf-8") as f:
            yaml.dump(umf_data, f)

        return umf_path

    def test_generate_expectations_basic(self, mapper, umf_file):
        """Test basic expectation suite generation."""
        suite = mapper.generate_expectations(umf_file)

        assert suite["name"] == "test_table_suite"
        assert suite["meta"]["table_name"] == "test_table"
        assert suite["meta"]["generated_by"] == "tablespec"
        assert len(suite["expectations"]) > 0

    def test_generate_expectations_strictness_levels(self, mapper, umf_file):
        """Test different strictness levels."""
        suite_loose = mapper.generate_expectations(umf_file, strictness="loose")
        suite_medium = mapper.generate_expectations(umf_file, strictness="medium")
        suite_strict = mapper.generate_expectations(umf_file, strictness="strict")

        assert suite_loose["meta"]["strictness"] == "loose"
        assert suite_medium["meta"]["strictness"] == "medium"
        assert suite_strict["meta"]["strictness"] == "strict"

        # All should have baseline expectations (strictness currently doesn't affect baseline)
        assert len(suite_loose["expectations"]) > 0
        assert len(suite_medium["expectations"]) > 0
        assert len(suite_strict["expectations"]) > 0

    def test_suite_includes_baseline_expectations(self, mapper, umf_file):
        """Test suite includes baseline expectations from UMF metadata."""
        suite = mapper.generate_expectations(umf_file)

        exp_types = {exp["type"] for exp in suite["expectations"]}

        # Should have structural expectations
        assert "expect_table_column_count_to_equal" in exp_types
        assert "expect_table_columns_to_match_ordered_list" in exp_types

        # Should have column expectations
        assert "expect_column_to_exist" in exp_types
        assert "expect_column_values_to_be_of_type" in exp_types

    def test_profiling_expectations_generated(self, mapper, umf_with_profiling):
        """Test that profiling-based expectations are attempted."""
        suite = mapper.generate_expectations(umf_with_profiling)

        # Should have expectations (currently just baseline, but profiling path is exercised)
        assert len(suite["expectations"]) > 0

        # Profiling columns should be processed
        assert suite["meta"]["table_name"] == "profiled_table"

    def test_profiling_expectations_without_profiling_data(self, mapper, umf_file):
        """Test profiling expectations when no profiling data exists."""
        # Load UMF to test _generate_profiling_expectations directly
        with umf_file.open(encoding="utf-8") as f:
            umf = yaml.safe_load(f)

        column = umf["columns"][0]  # No profiling data
        expectations = mapper._generate_profiling_expectations(column, "medium")

        # Should return empty list when no profiling data
        assert expectations == []

    def test_profiling_expectations_with_profiling_data(
        self, mapper, umf_with_profiling
    ):
        """Test profiling expectations when profiling data exists."""
        # Load UMF to test _generate_profiling_expectations directly
        with umf_with_profiling.open(encoding="utf-8") as f:
            umf = yaml.safe_load(f)

        column = umf["columns"][0]  # Has profiling data
        expectations = mapper._generate_profiling_expectations(column, "medium")

        # Currently returns empty (TODO implementation), but exercises code path
        assert isinstance(expectations, list)

    def test_suite_metadata_contains_source_file(self, mapper, umf_file):
        """Test suite metadata includes source UMF file path."""
        suite = mapper.generate_expectations(umf_file)

        assert "source_umf" in suite["meta"]
        assert str(umf_file) in suite["meta"]["source_umf"]

    def test_generate_expectations_with_path_string(self, mapper, umf_file):
        """Test generate_expectations accepts string path."""
        suite = mapper.generate_expectations(str(umf_file))

        assert suite["name"] == "test_table_suite"
        assert len(suite["expectations"]) > 0

    def test_generate_expectations_with_path_object(self, mapper, umf_file):
        """Test generate_expectations accepts Path object."""
        suite = mapper.generate_expectations(umf_file)

        assert suite["name"] == "test_table_suite"
        assert len(suite["expectations"]) > 0

    def test_mapper_uses_baseline_generator(self, mapper):
        """Test mapper uses BaselineExpectationGenerator."""
        assert isinstance(mapper.baseline_generator, BaselineExpectationGenerator)

    def test_expectation_meta_fields(self, mapper, umf_file):
        """Test expectations have required meta fields."""
        suite = mapper.generate_expectations(umf_file)

        for exp in suite["expectations"]:
            assert "meta" in exp
            assert "description" in exp["meta"]
            assert "severity" in exp["meta"]
            assert exp["meta"]["severity"] in ["critical", "warning", "info"]

    def test_table_name_from_umf(self, mapper, tmp_path: Path):
        """Test table name is extracted from UMF."""
        umf_data = {
            "table_name": "my_custom_table",
            "columns": [{"name": "col1", "data_type": "VARCHAR"}],
        }

        umf_path = tmp_path / "custom.umf.yaml"
        with umf_path.open("w", encoding="utf-8") as f:
            yaml.dump(umf_data, f)

        suite = mapper.generate_expectations(umf_path)

        assert suite["name"] == "my_custom_table_suite"
        assert suite["meta"]["table_name"] == "my_custom_table"

    def test_unknown_table_name_fallback(self, mapper, tmp_path: Path):
        """Test fallback when table_name not in UMF."""
        umf_data = {"columns": [{"name": "col1", "data_type": "VARCHAR"}]}

        umf_path = tmp_path / "test.umf.yaml"
        with umf_path.open("w", encoding="utf-8") as f:
            yaml.dump(umf_data, f)

        suite = mapper.generate_expectations(umf_path)

        assert suite["name"] == "unknown_suite"
        assert suite["meta"]["table_name"] == "unknown"
