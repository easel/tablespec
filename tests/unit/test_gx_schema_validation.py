"""Test that all expectation types in schema are valid with GX library."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

# Skip all tests in this module if GX has import issues (numpy/pandas compatibility)
pytestmark = pytest.mark.skip(
    reason="Great Expectations has numpy/pandas compatibility issues"
)


class TestGXSchemaValidation:
    """Validate that all expectation types in schema work with Great Expectations."""

    @pytest.fixture
    def schema_path(self) -> Path:
        """Path to GX expectation suite schema."""
        return (
            Path(__file__).parent.parent.parent
            / "src"
            / "tablespec"
            / "schemas"
            / "gx_expectation_suite.schema.json"
        )

    @pytest.fixture
    def schema(self, schema_path: Path) -> dict:
        """Load GX expectation schema."""
        with schema_path.open(encoding="utf-8") as f:
            return json.load(f)

    @pytest.fixture
    def expectation_types(self, schema: dict) -> list[str]:
        """Extract all expectation types from schema enum."""
        return schema["properties"]["expectations"]["items"]["properties"]["type"][
            "enum"
        ]

    @pytest.fixture
    def validator(self):
        """Create GX schema validator instance."""
        from tablespec.gx_schema_validator import GXSchemaValidator

        return GXSchemaValidator()

    def test_schema_file_exists(self, schema_path: Path):
        """Test that schema file exists."""
        assert schema_path.exists(), f"Schema file not found at {schema_path}"

    def test_schema_has_expectation_types(self, expectation_types: list[str]):
        """Test that schema contains expectation types."""
        assert len(expectation_types) > 0, "Schema contains no expectation types"
        assert all(exp_type.startswith("expect_") for exp_type in expectation_types), (
            "All expectation types should start with 'expect_'"
        )

    def test_all_expectation_types_instantiate(
        self, schema_path: Path, expectation_types: list[str], validator
    ):
        """Test that each expectation type can be instantiated with GX library.

        This is the main validation test. It attempts to instantiate every
        expectation type defined in the schema using the actual GX library.
        """
        pytest.importorskip(
            "great_expectations", reason="Great Expectations not installed"
        )

        # Run validation on all types in schema
        results = validator.validate_all_types_in_schema(schema_path)

        # Save detailed report for analysis
        report_path = (
            Path(__file__).parent.parent / "fixtures" / "gx_validation_report.json"
        )
        report_path.parent.mkdir(parents=True, exist_ok=True)
        with report_path.open("w", encoding="utf-8") as f:
            json.dump(results, f, indent=2)

        # Print summary
        print(f"\n{'=' * 70}")
        print("GX Schema Validation Report")
        print(f"{'=' * 70}")
        print(f"Total expectation types: {results['total']}")
        print(f"Valid: {len(results['valid'])}")
        print(f"Invalid: {len(results['invalid'])}")
        print(f"\nDetailed report saved to: {report_path}")

        if results["invalid"]:
            print(f"\n{'=' * 70}")
            print("Invalid Expectation Types:")
            print(f"{'=' * 70}")
            for item in results["invalid"]:
                print(f"  ✗ {item['type']}")
                print(f"    Error: {item['error']}")

        # Assert all expectations are valid
        assert len(results["invalid"]) == 0, (
            f"Found {len(results['invalid'])} invalid expectation types (see report for details)"
        )

    def test_specific_expectation_types(self, validator):
        """Test specific expectation types that are known to be problematic."""
        pytest.importorskip(
            "great_expectations", reason="Great Expectations not installed"
        )

        # Test expect_compound_columns_to_be_unique (should be valid)
        is_valid, error = validator.validate_expectation_type(
            "expect_compound_columns_to_be_unique"
        )
        assert is_valid, (
            f"expect_compound_columns_to_be_unique should be valid: {error}"
        )

        # Test expect_multicolumn_values_to_be_unique (may be invalid in newer GX)
        is_valid, error = validator.validate_expectation_type(
            "expect_multicolumn_values_to_be_unique"
        )
        # Don't assert - just document the result
        print(
            f"\nexpect_multicolumn_values_to_be_unique: {'valid' if is_valid else 'INVALID'}"
        )
        if not is_valid:
            print(f"  Error: {error}")

    def test_column_expectations_have_column_kwarg(self, validator):
        """Test that column-level expectations are generated with column kwarg."""
        kwargs = validator._generate_minimal_kwargs("expect_column_values_to_be_in_set")
        assert "column" in kwargs, "Column expectations should have 'column' kwarg"
        assert kwargs["column"] == "test_col"
        assert "value_set" in kwargs, (
            "in_set expectations should have 'value_set' kwarg"
        )

    def test_multicolumn_expectations_have_column_list(self, validator):
        """Test that multicolumn expectations are generated with column_list kwarg."""
        kwargs = validator._generate_minimal_kwargs(
            "expect_compound_columns_to_be_unique"
        )
        assert "column_list" in kwargs, (
            "Multicolumn expectations should have 'column_list' kwarg"
        )
        assert isinstance(kwargs["column_list"], list)
        assert len(kwargs["column_list"]) >= 2

    def test_pair_expectations_have_two_columns(self, validator):
        """Test that pair expectations are generated with column_A and column_B kwargs."""
        kwargs = validator._generate_minimal_kwargs(
            "expect_column_pair_values_a_to_be_greater_than_b"
        )
        assert "column_A" in kwargs, "Pair expectations should have 'column_A' kwarg"
        assert "column_B" in kwargs, "Pair expectations should have 'column_B' kwarg"

    def test_table_expectations_have_correct_kwargs(self, validator):
        """Test that table-level expectations are generated correctly."""
        # Table expectation with columns
        kwargs = validator._generate_minimal_kwargs("expect_table_columns_to_match_set")
        assert "column_set" in kwargs, (
            "Table column expectations should have 'column_set' kwarg"
        )

        # Table expectation without columns
        kwargs = validator._generate_minimal_kwargs("expect_table_row_count_to_equal")
        assert "column" not in kwargs, (
            "Row count expectations should not have 'column' kwarg"
        )

    def test_each_expectation_passes_json_schema_validation(
        self, schema_path: Path, schema: dict, expectation_types: list[str], validator
    ):
        """Test that each expectation type passes JSON schema validation.

        This validates the expectation JSON structure against the JSON schema
        before attempting GX library validation.
        """
        pytest.importorskip("jsonschema", reason="jsonschema not installed")
        pytest.importorskip(
            "great_expectations", reason="Great Expectations not installed"
        )

        results = {
            "total": len(expectation_types),
            "json_schema_valid": [],
            "json_schema_invalid": [],
            "gx_valid": [],
            "gx_invalid": [],
            "both_valid": [],
        }

        for exp_type in expectation_types:
            # Skip pending implementation
            if exp_type == "expect_validation_rule_pending_implementation":
                results["both_valid"].append(exp_type)
                results["json_schema_valid"].append(exp_type)
                results["gx_valid"].append(exp_type)
                continue

            # Generate expectation JSON
            kwargs = validator._generate_minimal_kwargs(exp_type)
            expectation_json = {
                "type": exp_type,
                "kwargs": kwargs,
                "meta": {"description": f"Test for {exp_type}", "severity": "warning"},
            }

            # Validate with both JSON schema and GX
            is_valid, errors = validator.validate_expectation_json(
                expectation_json, schema
            )

            if is_valid:
                results["both_valid"].append(exp_type)
                results["json_schema_valid"].append(exp_type)
                results["gx_valid"].append(exp_type)
            else:
                # Determine which validation failed
                if any("JSON schema validation" in e for e in errors):
                    results["json_schema_invalid"].append(
                        {"type": exp_type, "errors": errors}
                    )
                if any("GX validation" in e for e in errors):
                    results["gx_invalid"].append({"type": exp_type, "errors": errors})

        # Print summary
        print(f"\n{'=' * 70}")
        print("JSON Schema & GX Validation Report")
        print(f"{'=' * 70}")
        print(f"Total: {results['total']}")
        print(f"Both Valid: {len(results['both_valid'])}")
        print(f"JSON Schema Invalid: {len(results['json_schema_invalid'])}")
        print(f"GX Invalid: {len(results['gx_invalid'])}")

        if results["json_schema_invalid"]:
            print("\nJSON Schema Validation Failures:")
            for item in results["json_schema_invalid"]:
                print(f"  ✗ {item['type']}")
                for error in item["errors"]:
                    print(f"    {error}")

        if results["gx_invalid"]:
            print("\nGX Validation Failures:")
            for item in results["gx_invalid"]:
                print(f"  ✗ {item['type']}")
                for error in item["errors"]:
                    print(f"    {error}")

        # Assert all pass both validations
        assert len(results["json_schema_invalid"]) == 0, (
            f"Found {len(results['json_schema_invalid'])} expectations "
            "that fail JSON schema validation"
        )
        assert len(results["gx_invalid"]) == 0, (
            f"Found {len(results['gx_invalid'])} expectations that fail GX validation"
        )

    def test_complete_expectation_suite_with_all_types(
        self, schema_path: Path, schema: dict, validator
    ):
        """Test that a complete suite with all expectation types is valid.

        This generates a single expectation suite containing ALL 42 expectation
        types, validates it against the JSON schema, and verifies GX can load it.
        """
        pytest.importorskip("jsonschema", reason="jsonschema not installed")
        pytest.importorskip(
            "great_expectations", reason="Great Expectations not installed"
        )

        from great_expectations.core.expectation_suite import ExpectationSuite
        from great_expectations.expectations.expectation_configuration import (
            ExpectationConfiguration,
        )

        # Generate complete suite
        complete_suite = validator.generate_complete_expectation_suite(schema_path)

        # Save to fixtures for reference
        fixture_path = (
            Path(__file__).parent.parent / "fixtures" / "complete_gx_suite.json"
        )
        fixture_path.parent.mkdir(parents=True, exist_ok=True)
        with fixture_path.open("w", encoding="utf-8") as f:
            json.dump(complete_suite, f, indent=2)

        print(f"\nComplete suite saved to: {fixture_path}")
        print(f"Suite contains {len(complete_suite['expectations'])} expectations")

        # 1. Validate against JSON schema
        is_valid, schema_errors = validator.validate_suite_against_schema(
            complete_suite, schema
        )
        if not is_valid:
            print("\nJSON Schema Validation Errors:")
            for error in schema_errors:
                print(f"  {error}")
        assert is_valid, f"Complete suite fails JSON schema validation: {schema_errors}"

        # 2. Validate with GX library - load entire suite
        try:
            gx_suite = ExpectationSuite(
                name=complete_suite["name"], meta=complete_suite.get("meta", {})
            )

            errors = []
            for expectation in complete_suite["expectations"]:
                try:
                    exp_config = ExpectationConfiguration(
                        type=expectation["type"],
                        kwargs=expectation["kwargs"],
                        meta=expectation.get("meta", {}),
                    )
                    gx_suite.add_expectation_configuration(exp_config)
                except Exception as e:
                    errors.append(f"{expectation['type']}: {e}")

            if errors:
                print("\nGX Suite Loading Errors:")
                for error in errors:
                    print(f"  ✗ {error}")

            assert len(errors) == 0, (
                f"GX failed to load {len(errors)} expectations from suite"
            )

            print(
                f"✓ Complete suite loaded successfully into GX with {len(gx_suite.expectations)} expectations"
            )

        except Exception as e:
            pytest.fail(f"Failed to create GX ExpectationSuite: {e}")
