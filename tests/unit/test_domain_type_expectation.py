"""Tests for ExpectColumnValuesToMatchDomainType custom GX expectation.

Uses the standalone validate_domain_type() shim function to test domain type
validation logic without requiring a full GX runtime or Spark. The shim
function uses the same validation logic that the GX Expectation class delegates to.
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.no_spark

pd = pytest.importorskip("pandas")

from tablespec.validation.custom_gx_expectations import validate_domain_type  # noqa: E402


class TestDomainTypeExpectationValueSet:
    """Tests for domain types validated via value_set (e.g., us_state_code, gender)."""

    def test_valid_state_codes(self):
        """All valid US state codes should pass."""
        df = pd.DataFrame({"state": ["MD", "CA", "NY", "TX"]})
        result = validate_domain_type(df, "state", "us_state_code")
        assert result["success"]
        assert result["result"]["unexpected_count"] == 0

    def test_invalid_state_code(self):
        """Invalid state code should fail."""
        df = pd.DataFrame({"state": ["MD", "XX", "NY"]})
        result = validate_domain_type(df, "state", "us_state_code")
        assert not result["success"]
        assert result["result"]["unexpected_count"] == 1
        assert "XX" in result["result"]["partial_unexpected_list"]

    def test_all_invalid_state_codes(self):
        """All invalid state codes should report all as unexpected."""
        df = pd.DataFrame({"state": ["XX", "ZZ", "QQ"]})
        result = validate_domain_type(df, "state", "us_state_code")
        assert not result["success"]
        assert result["result"]["unexpected_count"] == 3

    def test_valid_gender_codes(self):
        """Valid gender codes should pass."""
        df = pd.DataFrame({"gender": ["M", "F", "U", "N"]})
        result = validate_domain_type(df, "gender", "gender")
        assert result["success"]

    def test_invalid_gender_code(self):
        """Invalid gender code should fail."""
        df = pd.DataFrame({"gender": ["M", "X"]})
        result = validate_domain_type(df, "gender", "gender")
        assert not result["success"]

    def test_valid_lob_codes(self):
        """Valid line of business codes should pass."""
        df = pd.DataFrame({"lob": ["MEDICAID", "MEDICARE", "MD", "ME"]})
        result = validate_domain_type(df, "lob", "lob")
        assert result["success"]

    def test_valid_yes_no_flags(self):
        """Valid Y/N flags should pass."""
        df = pd.DataFrame({"flag": ["Y", "N", "Y", "N"]})
        result = validate_domain_type(df, "flag", "yes_no_flag")
        assert result["success"]


class TestDomainTypeExpectationRegex:
    """Tests for domain types validated via regex (e.g., email, npi, zip_code)."""

    def test_valid_emails(self):
        """Valid email addresses should pass."""
        df = pd.DataFrame({"email": ["user@example.com", "test@test.org"]})
        result = validate_domain_type(df, "email", "email")
        assert result["success"]

    def test_invalid_email(self):
        """Invalid email should fail."""
        df = pd.DataFrame({"email": ["user@example.com", "not-an-email"]})
        result = validate_domain_type(df, "email", "email")
        assert not result["success"]
        assert result["result"]["unexpected_count"] == 1

    def test_valid_npi(self):
        """Valid 10-digit NPIs should pass."""
        df = pd.DataFrame({"npi": ["1234567890", "9876543210"]})
        result = validate_domain_type(df, "npi", "npi")
        assert result["success"]

    def test_invalid_npi(self):
        """Short NPI should fail."""
        df = pd.DataFrame({"npi": ["1234567890", "12345"]})
        result = validate_domain_type(df, "npi", "npi")
        assert not result["success"]

    def test_valid_zip_codes(self):
        """Valid ZIP codes should pass."""
        df = pd.DataFrame({"zip": ["12345", "98765-4321"]})
        result = validate_domain_type(df, "zip", "zip_code")
        assert result["success"]

    def test_invalid_zip_code(self):
        """Invalid ZIP code should fail."""
        df = pd.DataFrame({"zip": ["12345", "ABC"]})
        result = validate_domain_type(df, "zip", "zip_code")
        assert not result["success"]

    def test_valid_phone_numbers(self):
        """Valid phone number formats should pass."""
        df = pd.DataFrame({"phone": ["5551234567", "555-123-4567", "(555) 123-4567"]})
        result = validate_domain_type(df, "phone", "phone_number")
        assert result["success"]


class TestDomainTypeExpectationLengthBased:
    """Tests for domain types validated via length constraints."""

    def test_valid_address(self):
        """Valid address lengths should pass."""
        df = pd.DataFrame({"addr": ["123 Main St", "456 Oak Ave"]})
        result = validate_domain_type(df, "addr", "address_line_1")
        assert result["success"]

    def test_empty_address_fails(self):
        """Empty string address should fail (min_value=1)."""
        df = pd.DataFrame({"addr": ["123 Main St", ""]})
        result = validate_domain_type(df, "addr", "address_line_1")
        assert not result["success"]


class TestDomainTypeExpectationMostly:
    """Tests for the 'mostly' threshold parameter."""

    def test_mostly_threshold_passes(self):
        """Should pass when enough values match the mostly threshold."""
        # 3 out of 4 valid = 75%
        df = pd.DataFrame({"state": ["MD", "CA", "NY", "XX"]})
        result = validate_domain_type(df, "state", "us_state_code", mostly=0.7)
        assert result["success"]

    def test_mostly_threshold_fails(self):
        """Should fail when not enough values match the mostly threshold."""
        # 1 out of 4 valid = 25%
        df = pd.DataFrame({"state": ["MD", "XX", "ZZ", "QQ"]})
        result = validate_domain_type(df, "state", "us_state_code", mostly=0.5)
        assert not result["success"]


class TestDomainTypeExpectationEdgeCases:
    """Tests for edge cases and error handling."""

    def test_all_null_column(self):
        """Column with all nulls should pass (nothing to validate)."""
        df = pd.DataFrame({"state": [None, None, None]})
        result = validate_domain_type(df, "state", "us_state_code")
        assert result["success"]
        assert result["result"]["element_count"] == 0

    def test_mixed_nulls_and_valid(self):
        """Nulls should be excluded from validation; valid values pass."""
        df = pd.DataFrame({"state": ["MD", None, "CA", None]})
        result = validate_domain_type(df, "state", "us_state_code")
        assert result["success"]
        assert result["result"]["element_count"] == 2

    def test_unknown_domain_type(self):
        """Unknown domain type should fail with descriptive message."""
        df = pd.DataFrame({"col": ["a", "b"]})
        result = validate_domain_type(df, "col", "nonexistent_domain_type")
        assert not result["success"]
        assert "not found" in result["result"]["observed_value"]

    def test_result_structure(self):
        """Result should contain standard GX result keys."""
        df = pd.DataFrame({"state": ["MD"]})
        result = validate_domain_type(df, "state", "us_state_code")
        assert "success" in result
        assert "result" in result
        inner = result["result"]
        assert "element_count" in inner
        assert "unexpected_count" in inner
        assert "unexpected_percent" in inner
        assert "partial_unexpected_list" in inner
        assert "observed_value" in inner

    def test_unexpected_values_limited_to_10(self):
        """Partial unexpected list should be limited to 10 items."""
        # Create 20 invalid values
        df = pd.DataFrame({"state": [f"X{i}" for i in range(20)]})
        result = validate_domain_type(df, "state", "us_state_code")
        assert not result["success"]
        assert len(result["result"]["partial_unexpected_list"]) <= 10


class TestDomainTypeExpectationMultipleValidations:
    """Tests for domain types with multiple validation rules."""

    def test_calendar_year_valid(self):
        """Valid calendar years should pass both type and range checks."""
        df = pd.DataFrame({"year": [2020, 2021, 2025]})
        result = validate_domain_type(df, "year", "calendar_year")
        assert result["success"]

    def test_calendar_year_out_of_range(self):
        """Calendar year outside 1900-2100 should fail."""
        df = pd.DataFrame({"year": [2020, 1800]})
        result = validate_domain_type(df, "year", "calendar_year")
        assert not result["success"]
