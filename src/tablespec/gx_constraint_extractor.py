"""Great Expectations Constraint Extractor for Phase 5 data generation.

This module extracts enumerated value constraints from GX expectation suites
to generate valid data without hard-coding domain values.
"""

import logging
import random
from pathlib import Path
from typing import Any

import yaml


class GXConstraintExtractor:
    """Extracts enumerated value constraints from GX expectation suites."""

    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__)

    def load_expectations_for_table(
        self, table_name: str, relationships_dir: Path
    ) -> dict[str, Any] | None:
        """Load GX YAML expectation suite for a table.

        Args:
            table_name: Name of the table
            relationships_dir: Directory containing Phase 4 output with expectation suites

        Returns:
            Parsed YAML expectation suite, or None if not found

        """
        expectations_file = (
            relationships_dir / "tables" / f"{table_name}.expectations.yaml"
        )

        if not expectations_file.exists():
            self.logger.debug(f"No GX expectations found for {table_name}")
            return None

        try:
            with expectations_file.open() as f:
                return yaml.safe_load(f)
        except Exception as e:
            self.logger.warning(f"Failed to load GX expectations for {table_name}: {e}")
            return None

    def extract_value_sets(self, expectations: dict[str, Any]) -> dict[str, list[str]]:
        """Extract column -> value_set mappings from expect_column_values_to_be_in_set.

        Args:
            expectations: Parsed GX expectation suite YAML

        Returns:
            Dictionary mapping column names to allowed value lists

        """
        value_sets = {}

        if not expectations or "expectations" not in expectations:
            return value_sets

        for expectation in expectations["expectations"]:
            if expectation.get("type") == "expect_column_values_to_be_in_set":
                kwargs = expectation.get("kwargs", {})
                column = kwargs.get("column")
                value_set = kwargs.get("value_set", [])

                if column and value_set:
                    # Convert all values to strings for consistency
                    value_sets[column] = [str(v) for v in value_set]

        return value_sets

    def extract_metadata_hints(
        self, expectations: dict[str, Any]
    ) -> dict[str, dict[str, list[str]]]:
        """Extract hints from expectation metadata as fallback constraints.

        Looks for metadata fields like `meta.lob`, `meta.states`, etc. that
        document allowed values but aren't in formal value_set constraints.

        Args:
            expectations: Parsed GX expectation suite YAML

        Returns:
            Dictionary mapping column names to metadata hints

        """
        metadata_hints = {}

        if not expectations or "expectations" not in expectations:
            return metadata_hints

        for expectation in expectations["expectations"]:
            kwargs = expectation.get("kwargs", {})
            column = kwargs.get("column")
            meta = expectation.get("meta", {})

            if not column:
                continue

            # Extract common metadata hint patterns
            hints = {}

            # Check for LOB values in metadata
            if "lob" in meta and isinstance(meta["lob"], list):
                hints["lob"] = [str(v) for v in meta["lob"]]

            # Check for state codes in metadata
            if "states" in meta and isinstance(meta["states"], list):
                hints["states"] = [str(v) for v in meta["states"]]

            # Check description for examples like "Ex: MD, ME, MP"
            description = meta.get("description", "")
            if description:
                hints["description_examples"] = self._extract_examples_from_description(
                    description
                )

            if hints:
                if column not in metadata_hints:
                    metadata_hints[column] = {}
                metadata_hints[column].update(hints)

        return metadata_hints

    def _extract_examples_from_description(self, description: str) -> list[str]:
        """Extract example values from description text.

        Looks for patterns like "Ex: MD, ME, MP" or "Example: A, B, C".

        Args:
            description: Description text from GX expectation

        Returns:
            List of extracted example values

        """
        examples = []

        # Look for "Ex:" or "Example:" patterns
        if "Ex:" in description or "Example:" in description:
            # Extract the part after "Ex:" or "Example:"
            parts = description.split("Ex:")
            if len(parts) < 2:
                parts = description.split("Example:")

            if len(parts) >= 2:
                example_text = parts[1].split(".")[0].strip()  # Get first sentence
                # Split by commas and clean up
                values = [v.strip() for v in example_text.split(",")]
                examples.extend(
                    [v for v in values if v and len(v) <= 50]
                )  # Sanity check

        return examples

    def get_constraints_for_column(
        self, expectations: dict[str, Any], column_name: str
    ) -> list[str] | None:
        """Get all available constraints for a specific column.

        Only uses formal expect_column_values_to_be_in_set constraints.
        Does NOT use metadata hints (meta.lob, meta.states) as these indicate
        which LOBs/states the rule applies to, not valid column values.

        Args:
            expectations: Parsed GX expectation suite YAML
            column_name: Name of the column

        Returns:
            List of allowed values from value_set constraints, or None if not found

        """
        # Only use formal value_set constraints
        value_sets = self.extract_value_sets(expectations)
        if column_name in value_sets:
            # Filter out obvious column names/placeholders
            filtered_values = [
                v
                for v in value_sets[column_name]
                if not self._looks_like_column_name(v)
            ]
            if filtered_values:
                return filtered_values

        return None

    def _looks_like_column_name(self, value: str) -> bool:
        """Check if a value looks like a column name or placeholder.

        Args:
            value: Value to check

        Returns:
            True if value appears to be a column name, not actual data

        """
        if not value or len(value.strip()) == 0:
            return True

        # Common patterns that indicate column names
        suspicious_patterns = [
            "CLIENT MBRID",
            "MEMBER_ID",
            "FIRST NAME",
            "LAST NAME",
            "MemberLastName",
            "MemberFirstName",
            "CHASE LOAD DATE",
            "LOAD DATE",
            "DATE",
        ]

        # Check for exact matches (case-insensitive)
        if value.upper() in [p.upper() for p in suspicious_patterns]:
            return True

        # Check if it's mostly uppercase with underscores (typical column name style)
        return bool("_" in value and value.upper() == value and len(value.split()) <= 4)

    def extract_regex_patterns(self, expectations: dict[str, Any]) -> dict[str, str]:
        """Extract column -> regex pattern mappings from expect_column_values_to_match_regex.

        Args:
            expectations: Parsed GX expectation suite YAML

        Returns:
            Dictionary mapping column names to regex patterns

        """
        regex_patterns = {}

        if not expectations or "expectations" not in expectations:
            return regex_patterns

        for expectation in expectations["expectations"]:
            if expectation.get("type") == "expect_column_values_to_match_regex":
                kwargs = expectation.get("kwargs", {})
                column = kwargs.get("column")
                regex = kwargs.get("regex")

                if column and regex:
                    regex_patterns[column] = regex

        return regex_patterns

    def get_regex_for_column(
        self, expectations: dict[str, Any], column_name: str
    ) -> str | None:
        """Get regex pattern for a specific column.

        Args:
            expectations: Parsed GX expectation suite YAML
            column_name: Name of the column

        Returns:
            Regex pattern string, or None if not found

        """
        regex_patterns = self.extract_regex_patterns(expectations)
        return regex_patterns.get(column_name)

    def extract_strftime_formats(self, expectations: dict[str, Any]) -> dict[str, str]:
        """Extract column -> strftime format mappings from expect_column_values_to_match_strftime_format.

        Args:
            expectations: Parsed GX expectation suite YAML

        Returns:
            Dictionary mapping column names to strftime format strings

        """
        strftime_formats = {}

        if not expectations or "expectations" not in expectations:
            return strftime_formats

        for expectation in expectations["expectations"]:
            if (
                expectation.get("type")
                == "expect_column_values_to_match_strftime_format"
            ):
                kwargs = expectation.get("kwargs", {})
                column = kwargs.get("column")
                strftime_format = kwargs.get("strftime_format")

                if column and strftime_format:
                    strftime_formats[column] = strftime_format

        return strftime_formats

    def get_strftime_format_for_column(
        self, expectations: dict[str, Any], column_name: str
    ) -> str | None:
        """Get strftime format for a specific column.

        Args:
            expectations: Parsed GX expectation suite YAML
            column_name: Name of the column

        Returns:
            Strftime format string, or None if not found

        """
        strftime_formats = self.extract_strftime_formats(expectations)
        return strftime_formats.get(column_name)

    def generate_value_from_regex(self, regex_pattern: str) -> str:
        r"""Generate a sample value that matches the given regex pattern.

        Supports common regex patterns used in healthcare data:
        - ^[A-Z]{n}$ - n uppercase letters
        - ^\d{n}$ - n digits
        - ^[A-Z]{n}\d{m}$ - n letters followed by m digits
        - Character classes like [A-Za-z], [0-9], etc.
        - + quantifier (one or more)
        - ? quantifier (zero or one)
        - \. for literal dot

        Args:
            regex_pattern: Regex pattern to match

        Returns:
            Generated value matching the pattern

        """
        # Remove anchors for processing
        pattern = regex_pattern.strip("^$")

        result = []
        i = 0

        while i < len(pattern):
            # Handle escaped characters like \. (literal dot)
            if i + 1 < len(pattern) and pattern[i] == "\\":
                next_char = pattern[i + 1]
                if next_char == ".":
                    result.append(".")
                    i += 2
                    continue
                # \d is handled below

            # Handle character classes like [A-Z], [0-9], [A-Za-z0-9]
            if pattern[i] == "[":
                end_bracket = pattern.find("]", i)
                if end_bracket == -1:
                    # Malformed pattern, skip
                    i += 1
                    continue

                char_class = pattern[i + 1 : end_bracket]

                # Check for quantifiers after the bracket
                repetition = 1
                next_pos = end_bracket + 1

                if next_pos < len(pattern):
                    if pattern[next_pos] == "{":
                        # {n}, {n,}, or {n,m} quantifier
                        end_brace = pattern.find("}", next_pos)
                        if end_brace != -1:
                            quant_str = pattern[next_pos + 1 : end_brace]
                            try:
                                if "," in quant_str:
                                    parts = quant_str.split(",")
                                    min_val = int(parts[0])
                                    # {n,} means n or more, {n,m} means n to m
                                    if len(parts) > 1 and parts[1].strip():
                                        max_val = int(parts[1])
                                        repetition = random.randint(min_val, max_val)
                                    else:
                                        # {n,} - generate between n and n+5
                                        repetition = random.randint(
                                            min_val, min_val + 5
                                        )
                                else:
                                    repetition = int(quant_str)
                                i = end_brace + 1
                            except ValueError:
                                i = next_pos
                        else:
                            i = next_pos
                    elif pattern[next_pos] == "+":
                        # + quantifier: one or more (generate 3-10)
                        repetition = random.randint(3, 10)
                        i = next_pos + 1
                    elif pattern[next_pos] == "?":
                        # ? quantifier: zero or one
                        repetition = random.choice([0, 1])
                        i = next_pos + 1
                    else:
                        i = next_pos
                else:
                    i = next_pos

                # Generate characters matching the class
                for _ in range(repetition):
                    result.append(self._generate_char_from_class(char_class))

            # Handle shorthand \d for digits
            elif pattern[i : i + 2] == r"\d":
                # Check for quantifiers
                repetition = 1
                next_pos = i + 2

                if next_pos < len(pattern):
                    if pattern[next_pos] == "{":
                        # {n}, {n,}, or {n,m} quantifier
                        end_brace = pattern.find("}", next_pos)
                        if end_brace != -1:
                            quant_str = pattern[next_pos + 1 : end_brace]
                            try:
                                if "," in quant_str:
                                    parts = quant_str.split(",")
                                    min_val = int(parts[0])
                                    if len(parts) > 1 and parts[1].strip():
                                        max_val = int(parts[1])
                                        repetition = random.randint(min_val, max_val)
                                    else:
                                        repetition = random.randint(
                                            min_val, min_val + 5
                                        )
                                else:
                                    repetition = int(quant_str)
                                i = end_brace + 1
                            except ValueError:
                                i = next_pos
                        else:
                            i = next_pos
                    elif pattern[next_pos] == "+":
                        # + quantifier: one or more
                        repetition = random.randint(3, 10)
                        i = next_pos + 1
                    elif pattern[next_pos] == "?":
                        # ? quantifier: zero or one
                        repetition = random.choice([0, 1])
                        i = next_pos + 1
                    else:
                        i = next_pos
                else:
                    i = next_pos

                for _ in range(repetition):
                    result.append(str(random.randint(0, 9)))

            # Handle parentheses groups (simplified - just process contents)
            elif pattern[i] == "(":
                end_paren = pattern.find(")", i)
                if end_paren != -1:
                    # Recursively process group content
                    group_content = pattern[i + 1 : end_paren]
                    group_result = self.generate_value_from_regex(group_content)

                    # Check for quantifier after the group
                    next_pos = end_paren + 1
                    repetition = 1

                    if next_pos < len(pattern):
                        if pattern[next_pos] == "+":
                            repetition = random.randint(1, 3)
                            i = next_pos + 1
                        elif pattern[next_pos] == "?":
                            repetition = random.choice([0, 1])
                            i = next_pos + 1
                        else:
                            i = next_pos
                    else:
                        i = next_pos

                    for _ in range(repetition):
                        result.append(group_result)
                else:
                    i += 1

            # Skip unhandled regex metacharacters
            elif pattern[i] in ".*|":
                i += 1

            else:
                # Literal character
                result.append(pattern[i])
                i += 1

        return "".join(result)

    def _generate_char_from_class(self, char_class: str) -> str:
        """Generate a random character from a character class.

        Args:
            char_class: Character class specification (e.g., "A-Z", "0-9", "A-Za-z")

        Returns:
            Random character matching the class

        """
        import string

        # Handle ranges like A-Z, a-z, 0-9
        if "A-Z" in char_class:
            return random.choice(string.ascii_uppercase)
        if "a-z" in char_class:
            return random.choice(string.ascii_lowercase)
        if "0-9" in char_class:
            return str(random.randint(0, 9))

        # Handle combined classes
        chars = []
        if "A-Z" in char_class or "a-z" in char_class:
            chars.extend(string.ascii_letters)
        if "0-9" in char_class:
            chars.extend(string.digits)

        if chars:
            return random.choice(chars)

        # Fallback to literal characters in the class
        return random.choice(char_class) if char_class else "X"
