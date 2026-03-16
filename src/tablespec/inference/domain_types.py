"""Domain type inference system for UMF columns.

This module provides automatic detection of domain types (e.g., us_state_code, email, phone_number)
based on column names, descriptions, and sample values. Used in Phase 3 spec generation to tag
columns with domain types, which are then consumed by Phase 4 (validation) and Phase 6 (sample data).
"""

import logging
from pathlib import Path
import re
from typing import Any

import yaml


class DomainTypeRegistry:
    """Registry of domain types loaded from domain_types.yaml.

    Provides lookup and matching capabilities for domain type detection.
    """

    def __init__(self, registry_path: str | Path | None = None) -> None:
        """Initialize domain type registry.

        Args:
            registry_path: Path to domain_types.yaml file. If None, uses default location.

        Raises:
            FileNotFoundError: If registry file not found
            ValueError: If registry YAML is invalid

        """
        self.logger = logging.getLogger(self.__class__.__name__)
        self.registry_path = registry_path or self._get_default_registry_path()
        self.domain_types: dict[str, dict[str, Any]] = {}
        self._load_registry()

    @staticmethod
    def _get_default_registry_path() -> Path:
        """Get default registry path relative to this module."""
        return Path(__file__).parent.parent / "domain_types.yaml"

    def _load_registry(self) -> None:
        """Load domain types from YAML file."""
        registry_file = Path(self.registry_path)
        if not registry_file.exists():
            msg = f"Domain type registry not found: {registry_file}"
            raise FileNotFoundError(msg)

        try:
            with registry_file.open() as f:
                data = yaml.safe_load(f)
                self.domain_types = data.get("domain_types", {})
                self.logger.debug(
                    f"Loaded {len(self.domain_types)} domain types from {registry_file}"
                )
        except yaml.YAMLError as e:
            msg = f"Failed to parse domain type registry: {e}"
            raise ValueError(msg) from e

    def get_domain_type(self, name: str) -> dict[str, Any] | None:
        """Get domain type definition by name.

        Args:
            name: Domain type name (e.g., 'us_state_code')

        Returns:
            Domain type definition dict or None if not found

        """
        return self.domain_types.get(name)

    def list_domain_types(self) -> list[str]:
        """Get list of all registered domain type names."""
        return list(self.domain_types.keys())

    def get_validation_specs(self, domain_type: str) -> list[dict[str, Any]]:
        """Get validation specifications for a domain type.

        Args:
            domain_type: Domain type name

        Returns:
            List of validation spec dicts, each with 'type', 'kwargs', and 'severity' keys.
            Returns empty list if domain type not found or has no validations.

        """
        dt = self.get_domain_type(domain_type)
        if not dt:
            return []

        # Support both old 'validation' (singular) and new 'validations' (plural) for backward compatibility
        if "validations" in dt:
            return dt["validations"]
        if "validation" in dt:
            # Backward compatibility: wrap single validation in a list
            return [dt["validation"]]
        return []

    def get_sample_generator_method(self, domain_type: str) -> str | None:
        """Get sample data generator method name for domain type.

        Args:
            domain_type: Domain type name

        Returns:
            Generator method name (e.g., 'generate_state_code') or None

        """
        dt = self.get_domain_type(domain_type)
        if dt and "sample_generation" in dt:
            return dt["sample_generation"].get("method")
        return None

    def get_expected_base_type(self, domain_type: str) -> str | None:
        """Get expected base type for a domain type from its validations.

        Looks for 'expect_column_values_to_be_of_type' validation rules
        in the domain type definition to determine expected type.

        Args:
            domain_type: Domain type name

        Returns:
            Expected base type (e.g., 'STRING', 'DATE', 'INTEGER', 'TIMESTAMP')
            or None if no type constraint is defined.

        Examples:
            >>> registry.get_expected_base_type("birth_date")
            "DATE"
            >>> registry.get_expected_base_type("calendar_year")
            "INTEGER"
            >>> registry.get_expected_base_type("email")
            None  # String is implied by regex validation, not explicit type

        """
        validations = self.get_validation_specs(domain_type)
        for validation in validations:
            if validation.get("type") == "expect_column_values_to_be_of_type":
                type_value = validation.get("kwargs", {}).get("type_")
                if type_value:
                    # Skip placeholders like __COLUMN_TYPE__ (used in unmapped domain type)
                    if type_value.startswith("__") and type_value.endswith("__"):
                        return None
                    # Normalize: "DateType" -> "DATE", "IntegerType" -> "INTEGER"
                    return type_value.upper().replace("TYPE", "")
        return None

    def is_domain_type_compatible_with_data_type(self, domain_type: str, data_type: str) -> bool:
        """Check if a domain type is compatible with the given data_type.

        Domain types may have an expected base type (from validations).
        If they do, the column's data_type must be compatible.
        If no expected type is defined, the domain type is compatible with any data_type.

        Args:
            domain_type: Domain type name
            data_type: Column data_type (e.g., 'StringType', 'DateType')

        Returns:
            True if compatible, False otherwise

        """
        expected = self.get_expected_base_type(domain_type)
        if expected is None:
            # No explicit type constraint - domain type is type-agnostic
            # (e.g., email is validated by regex, works with any string-like type)
            return True

        data_type_upper = data_type.upper().replace("TYPE", "")

        # Define compatibility groups
        if expected == "DATE":
            # DATE domain types are compatible with DATE and TIMESTAMP
            return data_type_upper in ("DATE", "TIMESTAMP")
        if expected == "TIMESTAMP":
            return data_type_upper in ("TIMESTAMP",)
        if expected == "INTEGER":
            return data_type_upper in ("INTEGER", "INT", "LONG")
        if expected == "STRING":
            return data_type_upper in ("STRING", "CHAR", "VARCHAR", "TEXT")
        if expected == "BOOLEAN":
            return data_type_upper in ("BOOLEAN",)

        # Exact match for unknown expected types
        return data_type_upper == expected


class DomainTypeInference:
    """Infers domain types for UMF columns based on name, description, and sample values.

    Uses pattern matching on column names and description keywords to detect domain types.
    Can be used in Phase 3 spec generation to automatically tag columns.
    """

    def __init__(self, registry: DomainTypeRegistry | None = None) -> None:
        """Initialize inference engine.

        Args:
            registry: DomainTypeRegistry instance. If None, creates default.

        """
        self.logger = logging.getLogger(self.__class__.__name__)
        self.registry = registry or DomainTypeRegistry()

    def infer_domain_type(
        self,
        column_name: str,
        description: str | None = None,
        sample_values: list[str] | None = None,
        data_type: str | None = None,
    ) -> tuple[str | None, float]:
        """Infer domain type for a column.

        Args:
            column_name: Column name to analyze
            description: Column description (optional)
            sample_values: Sample values for the column (optional)
            data_type: Data type of column (optional)

        Returns:
            Tuple of (domain_type_name, confidence_score)
            confidence_score is 0.0-1.0 where 1.0 is highest confidence

        """
        candidates: dict[str, float] = {}

        # Score each domain type based on detection rules
        for domain_type_name in self.registry.list_domain_types():
            score = self._score_domain_type(
                domain_type_name,
                column_name,
                description,
                sample_values,
                data_type,
            )
            if score > 0.0:
                candidates[domain_type_name] = score

        if not candidates:
            return None, 0.0

        # Return best match
        best_match = max(candidates.items(), key=lambda x: x[1])
        return best_match[0], best_match[1]

    def _score_domain_type(
        self,
        domain_type: str,
        column_name: str,
        description: str | None,
        sample_values: list[str] | None,
        data_type: str | None,
    ) -> float:
        """Score how well a domain type matches the column.

        Args:
            domain_type: Domain type to score
            column_name: Column name
            description: Column description
            sample_values: Sample values
            data_type: Column data type

        Returns:
            Confidence score 0.0-1.0

        """
        dt = self.registry.get_domain_type(domain_type)
        if not dt or "detection" not in dt:
            return 0.0

        detection = dt["detection"]
        score = 0.0
        max_score = 0.0

        # Check column name patterns (highest weight)
        if "column_name_patterns" in detection:
            max_score += 0.6
            name_lower = column_name.lower()
            for pattern in detection["column_name_patterns"]:
                if pattern.lower() in name_lower or self._fuzzy_match(pattern.lower(), name_lower):
                    score += 0.6
                    break

        # Check description keywords
        if description and "description_keywords" in detection:
            max_score += 0.2
            desc_lower = description.lower()
            for keyword in detection["description_keywords"]:
                if keyword.lower() in desc_lower:
                    score += 0.2
                    break

        # Check sample values against pattern
        if sample_values and "sample_value_pattern" in detection:
            pattern = detection["sample_value_pattern"]
            if pattern:
                max_score += 0.2
                try:
                    regex = re.compile(pattern)
                    # Check if most sample values match pattern
                    matches = sum(1 for v in sample_values if regex.match(str(v)))
                    if matches >= len(sample_values) * 0.7:  # 70% threshold
                        score += 0.2
                except re.error:
                    # Skip invalid regex patterns
                    pass

        # Normalize to 0-1 range
        if max_score > 0:
            score = score / max_score
        return score

    @staticmethod
    def _fuzzy_match(pattern: str, text: str, _threshold: float = 0.7) -> bool:
        """Perform simple fuzzy matching for pattern in text.

        Checks if pattern appears as a word boundary match (underscore-delimited segments).

        Args:
            pattern: Pattern to search for
            text: Text to search in
            _threshold: Minimum similarity threshold (unused, kept for API compatibility)

        Returns:
            True if pattern matches a word segment in text

        """
        # Exact substring match already handled by caller, skip here
        # Check for word boundaries using underscores as delimiters
        # e.g., "state" matches "member_state" but NOT "mbr_first_name"
        segments = text.split("_")
        return pattern in segments


__all__ = ["DomainTypeInference", "DomainTypeRegistry"]
