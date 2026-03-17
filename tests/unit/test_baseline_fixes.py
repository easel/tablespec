"""Tests for baseline generator fixes: redundant type removal and profiling expectations."""

from __future__ import annotations

import pytest

from tablespec.gx_baseline import BaselineExpectationGenerator
from tablespec.models.umf import REDUNDANT_VALIDATION_TYPES

pytestmark = [pytest.mark.no_spark]


class TestRedundantTypeRemoval:
    """Verify that redundant expectation types are never generated."""

    def test_baseline_never_generates_redundant_types_minimal(self):
        """Test minimal UMF produces no redundant types."""
        umf_data = {
            "table_name": "t",
            "columns": [
                {"name": "id", "data_type": "INTEGER"},
                {"name": "name", "data_type": "VARCHAR", "max_length": 50},
            ],
        }
        gen = BaselineExpectationGenerator()
        expectations = gen.generate_baseline_expectations(umf_data)
        for exp in expectations:
            assert exp["type"] not in REDUNDANT_VALIDATION_TYPES, (
                f"Generated redundant type: {exp['type']}"
            )

    def test_baseline_never_generates_redundant_types_full(self):
        """Test full UMF with various column types produces no redundant types."""
        umf_data = {
            "table_name": "full_table",
            "columns": [
                {"name": "id", "data_type": "INTEGER", "nullable": {"md": False}},
                {"name": "name", "data_type": "VARCHAR", "max_length": 100},
                {"name": "dob", "data_type": "DATE"},
                {"name": "created", "data_type": "DATETIME"},
                {"name": "score", "data_type": "FLOAT"},
                {"name": "amount", "data_type": "DECIMAL"},
                {"name": "flag", "data_type": "BOOLEAN"},
            ],
        }
        gen = BaselineExpectationGenerator()
        expectations = gen.generate_baseline_expectations(umf_data)
        for exp in expectations:
            assert exp["type"] not in REDUNDANT_VALIDATION_TYPES, (
                f"Generated redundant type: {exp['type']}"
            )

    def test_column_to_exist_no_longer_generated(self):
        umf_data = {"table_name": "t", "columns": [{"name": "id", "data_type": "INTEGER"}]}
        gen = BaselineExpectationGenerator()
        exps = gen.generate_baseline_expectations(umf_data)
        types = [e["type"] for e in exps]
        assert "expect_column_to_exist" not in types

    def test_column_type_no_longer_generated(self):
        umf_data = {"table_name": "t", "columns": [{"name": "id", "data_type": "INTEGER"}]}
        gen = BaselineExpectationGenerator()
        exps = gen.generate_baseline_expectations(umf_data)
        types = [e["type"] for e in exps]
        assert "expect_column_values_to_be_of_type" not in types


class TestProfilingExpectations:
    """Verify profiling-to-expectations generation."""

    def test_unique_column_gets_uniqueness(self):
        col = {
            "name": "id",
            "data_type": "INTEGER",
            "profiling": {"approximate_num_distinct": 1000, "num_records": 1000},
        }
        gen = BaselineExpectationGenerator()
        exps = gen._generate_profiling_expectations(col)
        assert any(e["type"] == "expect_column_values_to_be_unique" for e in exps)

    def test_non_unique_column_no_uniqueness(self):
        col = {
            "name": "status",
            "data_type": "VARCHAR",
            "profiling": {"approximate_num_distinct": 5, "num_records": 1000},
        }
        gen = BaselineExpectationGenerator()
        exps = gen._generate_profiling_expectations(col)
        assert not any(e["type"] == "expect_column_values_to_be_unique" for e in exps)

    def test_bounded_column_gets_range(self):
        col = {
            "name": "age",
            "data_type": "INTEGER",
            "profiling": {"minimum": 0, "maximum": 120},
        }
        gen = BaselineExpectationGenerator()
        exps = gen._generate_profiling_expectations(col)
        between = [e for e in exps if e["type"] == "expect_column_values_to_be_between"]
        assert len(between) == 1
        assert between[0]["kwargs"]["min_value"] == 0
        assert between[0]["kwargs"]["max_value"] == 120

    def test_high_completeness_gets_not_null(self):
        col = {
            "name": "id",
            "data_type": "INTEGER",
            "profiling": {"completeness": 0.995},
        }
        gen = BaselineExpectationGenerator()
        exps = gen._generate_profiling_expectations(col)
        assert any(e["type"] == "expect_column_values_to_not_be_null" for e in exps)

    def test_low_completeness_no_not_null(self):
        col = {
            "name": "notes",
            "data_type": "VARCHAR",
            "profiling": {"completeness": 0.5},
        }
        gen = BaselineExpectationGenerator()
        exps = gen._generate_profiling_expectations(col)
        assert not any(e["type"] == "expect_column_values_to_not_be_null" for e in exps)

    def test_boundary_completeness_no_not_null(self):
        """Completeness of exactly 0.99 should NOT trigger not-null (must be > 0.99)."""
        col = {
            "name": "field",
            "data_type": "VARCHAR",
            "profiling": {"completeness": 0.99},
        }
        gen = BaselineExpectationGenerator()
        exps = gen._generate_profiling_expectations(col)
        assert not any(e["type"] == "expect_column_values_to_not_be_null" for e in exps)

    def test_no_profiling_returns_empty(self):
        col = {"name": "id", "data_type": "INTEGER"}
        gen = BaselineExpectationGenerator()
        exps = gen._generate_profiling_expectations(col)
        assert exps == []

    def test_empty_profiling_returns_empty(self):
        col = {"name": "id", "data_type": "INTEGER", "profiling": {}}
        gen = BaselineExpectationGenerator()
        exps = gen._generate_profiling_expectations(col)
        assert exps == []

    def test_profiling_expectations_have_generated_from(self):
        col = {
            "name": "id",
            "data_type": "INTEGER",
            "profiling": {"minimum": 0, "maximum": 100},
        }
        gen = BaselineExpectationGenerator()
        exps = gen._generate_profiling_expectations(col)
        for exp in exps:
            assert exp["meta"]["generated_from"] == "profiling"

    def test_profiling_expectations_have_warning_severity(self):
        col = {
            "name": "id",
            "data_type": "INTEGER",
            "profiling": {"minimum": 0, "maximum": 100, "completeness": 1.0},
        }
        gen = BaselineExpectationGenerator()
        exps = gen._generate_profiling_expectations(col)
        for exp in exps:
            assert exp["meta"]["severity"] == "warning"

    def test_profiling_wired_into_baseline(self):
        """Profiling expectations should appear in generate_baseline_column_expectations."""
        col = {
            "name": "id",
            "data_type": "INTEGER",
            "profiling": {"minimum": 1, "maximum": 999},
        }
        gen = BaselineExpectationGenerator()
        exps = gen.generate_baseline_column_expectations(col)
        assert any(e["type"] == "expect_column_values_to_be_between" for e in exps)

    def test_profiling_wired_into_full_generate(self):
        """Profiling expectations should appear in generate_baseline_expectations."""
        umf_data = {
            "table_name": "t",
            "columns": [
                {
                    "name": "id",
                    "data_type": "INTEGER",
                    "profiling": {"approximate_num_distinct": 500, "num_records": 500},
                }
            ],
        }
        gen = BaselineExpectationGenerator()
        exps = gen.generate_baseline_expectations(umf_data)
        assert any(e["type"] == "expect_column_values_to_be_unique" for e in exps)
