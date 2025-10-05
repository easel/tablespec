"""Validation prompt generator - Generates Great Expectations validation prompts."""

import json
from pathlib import Path
from typing import Any

from tablespec.gx_baseline import BaselineExpectationGenerator
from tablespec.prompts.expectation_guide import (
    format_quick_reference,
    get_llm_generatable_expectations,
    get_pending_decision_tree,
)
from tablespec.type_mappings import map_to_gx_spark_type


def _has_validation_rules(umf_data: dict[str, Any]) -> bool:
    """Check if UMF data contains fields with validation rules."""
    validation_rule_indicators = [
        # Format patterns
        "format",
        "pattern",
        "always",
        "must be",
        "should be",
        # Value constraints
        "valid values",
        "allowed values",
        "values:",
        "options:",
        # Range rules
        "range",
        "between",
        "from",
        "to",
        "-",
        "greater than",
        "less than",
        # Conditional rules
        "if",
        "when",
        "for",
        "depends on",
        "based on",
        # Required combinations
        "either",
        "or",
        "both",
        "combination",
        "together",
        # Healthcare-specific patterns
        "beneficiary",
        "member id",
        "plan code",
        "loinc",
        "icd",
        # Enumeration indicators
        "medicaid",
        "medicare",
        "duals",
        "marketplace",
        # Length constraints
        "digit",
        "character",
        "length",
    ]

    # Check table description for validation rules
    table_desc = umf_data.get("description", "").lower()
    if any(indicator in table_desc for indicator in validation_rule_indicators):
        return True

    # Check column descriptions and sample values for validation patterns
    for col in umf_data.get("columns", []):
        col_desc = col.get("description", "").lower()

        # Check description for validation rule indicators
        if any(indicator in col_desc for indicator in validation_rule_indicators):
            return True

        # Check for enumerated sample values that suggest validation rules
        sample_values = col.get("sample_values", [])
        if sample_values and len(sample_values) <= 10:  # Likely enumerated values
            # Check if sample values contain structured patterns
            sample_text = " ".join(str(v).lower() for v in sample_values)
            if any(
                indicator in sample_text for indicator in validation_rule_indicators[:8]
            ):  # Focus on value constraints
                return True

    return False


def _generate_validation_prompt(umf_data: dict[str, Any]) -> str:
    """Generate Great Expectations suite creation prompt."""
    table_name = umf_data["table_name"]
    table_desc = umf_data.get("description", "No description available")

    # Generate baseline expectations from UMF metadata
    generator = BaselineExpectationGenerator()
    baseline_expectations = generator.generate_baseline_expectations(
        umf_data, include_structural=True
    )

    # Load the GX schema from tablespec package (same package now)
    schema_path = (
        Path(__file__).parent.parent / "schemas" / "gx_expectation_suite.schema.json"
    )
    with schema_path.open() as f:
        gx_schema = json.load(f)

    # Extract key schema information
    severity_levels = gx_schema["properties"]["expectations"]["items"]["properties"][
        "meta"
    ]["properties"]["severity"]["enum"]
    name_pattern = gx_schema["properties"]["name"]["pattern"]

    # Get table-level LLM-generatable expectations (NOT baseline expectations)
    expectation_types = get_llm_generatable_expectations(context="table")

    # Start prompt with critical NO COMMENTS warning at top
    prompt = f"""╔══════════════════════════════════════════════════════════════╗
║  🚨 CRITICAL: JSON OUTPUT FORMAT - NO COMMENTS ALLOWED 🚨   ║
╔══════════════════════════════════════════════════════════════╗

JSON DOES NOT SUPPORT COMMENTS (RFC 8259)

❌ ABSOLUTELY FORBIDDEN:
• // line comments
• /* block comments */
• # any other comment syntax
• Explanatory text outside JSON structure

✅ REQUIRED:
• Pure JSON only (must parse with json.loads())
• No preprocessing, no comment stripping
• Valid per RFC 8259 specification

Your output will be DIRECTLY parsed by Python's json.loads().
If it contains comments, IT WILL FAIL.

╚══════════════════════════════════════════════════════════════╝

# Great Expectations Validation Suite: {table_name}

## Required Output Format - GX 1.6+

## Key Schema Requirements

**Top-level required fields:**
- `name` (string): Suite name matching pattern `{name_pattern}` (e.g., "{table_name}_suite")
- `expectations` (array): List of expectation configurations

**Per expectation required fields:**
- `type` (string): Must be one of {len(expectation_types)} valid expectation types (see enum below)
- `kwargs` (object): Arguments specific to the expectation type
- `meta` (object): Must include at minimum "description" and "severity"

**Valid severity levels:** {", ".join(f'`"{s}"`' for s in severity_levels)}

**Valid TABLE-LEVEL expectation types for LLM generation ({len(expectation_types)} types):**
⚠️ These are the ONLY expectation types you should generate. Do NOT generate baseline expectations.
{chr(10).join(f"- {t}" for t in expectation_types)}

**FORBIDDEN legacy fields (will cause validation errors):**
- ❌ `expectation_suite_name` → Use `name` instead
- ❌ `data_asset_type` → Remove entirely (not in GX 1.6+)
- ❌ `expectation_type` → Use `type` instead

---

# Great Expectations Suite Generation: {table_name}

## Objective
Generate a Great Expectations expectation suite for validating this healthcare table. Create comprehensive expectations with detailed descriptions that capture ALL context and reasoning.

## Table Information

**Table Name**: {table_name}
**Description**: {table_desc}
**Source File**: {umf_data.get("source_file", "")}
**Purpose**: Healthcare data validation for member outreach and care management

## ✅ Baseline Expectations Already Generated

**{len(baseline_expectations)} expectations have been automatically generated from UMF metadata:**
- Column existence checks for all {len(umf_data.get("columns", []))} columns
- Type validation (expect_column_values_to_be_of_type) for all columns
- Nullability checks (expect_column_values_to_not_be_null) for required fields
- Length constraints (expect_column_value_lengths_to_be_less_than_or_equal_to) where specified
- Date format validation (expect_column_values_to_match_strftime_format) for DATE columns
- Structural checks (expect_table_column_count_to_equal, expect_table_columns_to_match_ordered_list)

**⚠️ DO NOT generate expectations for:**
- Column existence (already generated)
- Column types (already generated)
- Basic nullability (already generated)
- Max length constraints (already generated)
- Basic date formats (already generated)
- Table structure (already generated)

**These {len(baseline_expectations)} baseline expectations will be automatically merged with your output.**

## Your Focus: TABLE-LEVEL Multi-Column Rules ONLY

⚠️ **CRITICAL**: This is a TABLE-LEVEL validation prompt. Generate ONLY multi-column and cross-column expectations.

Generate expectations ONLY for:
1. **Multi-column uniqueness** (expect_compound_columns_to_be_unique)
   - Composite keys, combined unique constraints
2. **Cross-column comparisons** (expect_column_pair_values_a_to_be_greater_than_b, expect_column_pair_values_to_be_equal)
   - Date range validation (EndDate >= StartDate)
   - Amount comparisons (TotalAmount >= SubAmount)
3. **Table-level structural rules** (expect_table_row_count_to_be_between, expect_table_columns_to_match_set)
   - Row count constraints
   - Required column sets
4. **Pending complex rules** (expect_validation_rule_pending_implementation)
   - Complex multi-column business logic
   - External lookups or master table references

❌ **DO NOT generate**:
- Single-column value sets (expect_column_values_to_be_in_set) → Use column-level prompts
- Single-column patterns (expect_column_values_to_match_regex) → Use column-level prompts
- Column existence, types, nullability, length → Already handled by baseline
- Any expectation with only a single `column` kwarg → Use column-level prompts

## Column Specifications

"""

    # Add each column in compressed format (2 lines per column vs 6-8)
    for col in umf_data.get("columns", []):
        col_name = col["name"]
        col_desc = col.get("description", "No description")
        data_type = col.get("data_type", "VARCHAR")
        gx_type = map_to_gx_spark_type(data_type)
        sample_values = col.get("sample_values", [])
        max_length = col.get("max_length")
        nullable = col.get("nullable", {})
        format_spec = col.get("format", "")
        notes = col.get("notes", [])

        # Build compact single-line format: **COL** (TYPE→GXType): Desc. [Req: LOBs.] [Fmt: X.] [Ex: a,b.] [Note: X.]
        line = f"**{col_name}** ({data_type}→{gx_type}): {col_desc}."

        # Add required LOBs (only non-nullable)
        req_lobs = [lob for lob, is_null in nullable.items() if not is_null]
        if req_lobs:
            line += f" Req: {'/'.join(req_lobs)}."

        # Add format
        if format_spec:
            line += f" Fmt: {format_spec}."

        # Add max length
        if max_length:
            line += f" MaxLen: {max_length}."

        # Filter and add sample values (keep filter logic for header pollution)
        if sample_values:
            valid_samples = []
            for sample in sample_values[:3]:  # Reduced from 5 to 3 for brevity
                sample_str = str(sample).strip()
                if not sample_str:
                    continue

                # Skip if matches column name (header pollution)
                is_header = (
                    sample_str.lower() == col_name.lower()
                    or sample_str.upper() == col_name.upper()
                    or sample_str.replace(" ", "_").lower() == col_name.lower()
                    or (
                        col_desc
                        and sample_str.lower() == col_desc.lower()[: len(sample_str)]
                    )
                )

                if not is_header:
                    valid_samples.append(sample)

            if valid_samples:
                line += f" Ex: {', '.join(str(s) for s in valid_samples)}."

        # Add first note only (truncated if too long)
        if notes and notes[0]:
            note_text = str(notes[0])[:80]
            line += f" Note: {note_text}."

        prompt += line + "\n"

    # Add relationship context if available
    if "relationships" in umf_data:
        prompt += "\n## Table Relationships\n"
        for rel in umf_data.get("relationships", {}).get("outgoing", []):
            prompt += f"- {rel.get('source_column', '')} → {rel.get('target_table', '')}.{rel.get('target_column', '')}\n"

    # Add quick reference and pending decision tree
    quick_reference = format_quick_reference(context="table")
    pending_tree = get_pending_decision_tree()

    prompt += f"""
{quick_reference}

## Parameter Requirements - Critical Rules

**Multi-column uniqueness:**
- `expect_compound_columns_to_be_unique`: MUST include `column_list: ["col1", "col2", ...]` with at least 2 columns

**Column pair comparisons:**
- `expect_column_pair_values_a_to_be_greater_than_b`: Requires `column_A` and `column_B`
  - Use `or_equal: true` (NOT `or_equal_to`) for >= comparison
  - For YYYYMMDD dates: NO `parse_strings_as_datetimes` (lexical comparison works)
- `expect_column_pair_values_to_be_equal`: Requires `column_A` and `column_B`

**Table-level rules:**
- `expect_table_row_count_to_be_between`: At least one of `min_value` or `max_value`
- `expect_table_columns_to_match_set`: Requires `column_set` (array of column names)

{pending_tree}

## Table-Level Common Patterns

**Composite Keys:** Look for natural keys formed by multiple columns
- Member + Date combinations (ClientMemberId + EffectiveDate)
- Code + Sequence combinations (GapKey + LOB + SequenceNumber)

**Date Range Validation:** Identify start/end date pairs
- EndDate >= StartDate
- ExpirationDate >= IssueDate
- MeasurementEndDate >= MeasurementStartDate

**Cross-Column Dependencies:** Look for logical relationships
- If column A has value X, column B must have value Y
- TotalAmount = Sum of component amounts
- Count fields matching array lengths

**LOB Constraints:** Note that `meta.lob` indicates which LOBs the rule applies to (MD/ME/MP), NOT that column values must be MD/ME/MP. Only use `value_set` for actual column value constraints.

## Output Format

**CRITICAL: Use Great Expectations 1.6+ Format**

DO NOT use these legacy field names:
- ❌ `expectation_suite_name` → Use `name` instead
- ❌ `data_asset_type` → Remove this field entirely (not in GX 1.6+)
- ❌ `expectation_type` → Use `type` instead

Generate a valid JSON object with this EXACT structure:

```json
{{
  "name": "{table_name}_suite",
  "meta": {{
"table_name": "{table_name}",
"generated_by": "pulseflow_phase_3",
"generation_date": "2025-01-29",
"great_expectations_version": "1.6.0"
  }},
  "expectations": [
{{
  "type": "expect_column_values_to_not_be_null",
  "kwargs": {{"column": "column_name"}},
  "meta": {{
    "description": "COMPREHENSIVE description including: what is validated, why (from UMF description/notes), specific constraints, source context, sample values that influenced rule, LOB requirements if applicable",
    "severity": "critical",
    "lob": ["MD", "ME", "MP"]
  }}
}}
  ]
}}
```

## Description Requirements

Each expectation's `meta.description` MUST include:
1. **What is being validated** (e.g., "Member ID format validation")
2. **Why this validation exists** (quote from column description/notes)
3. **Specific constraints** (e.g., "Must be 2 letters + 11 digits")
4. **Source context** (e.g., "Per description: 'Plan Code + Amisys Number'")
5. **Sample values influence** (e.g., "Based on sample: AL98765432101")
6. **LOB-specific requirements** if applicable

**Good Description Examples**

"ClientMemberId is required across all LOBs (MD/ME/MP all nullable=false). Format must be 2-letter plan code + 11-digit Amisys number based on description 'Unique member identifier - Plan Code + Amisys Number' and sample value 'AL98765432101'. This is the primary member identifier for tracking."

"TotalRank is required (non-nullable MD/ME/MP). Per description: 'Member's Tier Rank 1-6 for all Active Outreaches, >6 for passive outreach'. Notes indicate risk stratification: '1=high risk, 6=low risk, >6=passive'. Samples: 1, 2, 3. Critical for member prioritization."

## Instructions

**REMEMBER: This is a TABLE-LEVEL prompt. Generate ONLY multi-column and cross-column expectations.**

**Basic expectations (existence, types, nullability, length, date formats, structure) are ALREADY GENERATED by baseline. Do NOT duplicate them.**

**Single-column expectations (value sets, regex patterns) should NOT be generated here - those belong in column-level prompts.**

Focus on generating TABLE-LEVEL expectations ONLY:

1. **Multi-column uniqueness constraints** - Identify composite keys
   - Use `expect_compound_columns_to_be_unique` with `column_list` kwarg
   - Example: ClientMemberId + GapKey + LOB form unique constraint

2. **Cross-column comparisons** - Date/amount validations
   - Use `expect_column_pair_values_a_to_be_greater_than_b` with `column_A`, `column_B`, and `or_equal` kwargs
   - Example: EndDate >= StartDate

3. **Table-level structural rules** - Row counts, column sets
   - Use `expect_table_row_count_to_be_between` for row count constraints
   - Use `expect_table_columns_to_match_set` for required column combinations

4. **Complex unmappable multi-column rules** - Use `expect_validation_rule_pending_implementation`
   - Cross-table lookups (reference to master tables)
   - Complex multi-column business logic
   - External system validations

Set appropriate severity: `critical` (data integrity), `warning` (data quality), `info` (structural checks)

Write comprehensive descriptions that explain WHY the rule exists and HOW it relates to the business context.

## FINAL REQUIREMENT

⚠️ REMINDER: JSON = NO COMMENTS (this is the 3rd time we're telling you)

Return ONLY valid JSON. Your output will be DIRECTLY parsed by Python's json.loads().

**CRITICAL OUTPUT RULES (REPEATED FOR EMPHASIS):**
- ❌ NO markdown code blocks (no ```json)
- ❌ NO JavaScript comments (NO // or /* */ - JSON DOES NOT SUPPORT COMMENTS)
- ❌ NO explanations or commentary
- ❌ NO trailing commas
- ✅ ONLY valid JSON per RFC 8259
- ✅ Parseable by json.loads() WITHOUT preprocessing
- ✅ Must conform to GX 1.6+ format

**JSON DOES NOT SUPPORT COMMENTS. We've said this THREE times in this prompt. If you add comments, your output WILL FAIL parsing.**

Legacy fields FORBIDDEN: expectation_suite_name | data_asset_type | expectation_type

**YOUR OUTPUT MUST BE PARSEABLE BY `json.loads()` WITHOUT ANY PREPROCESSING.**
"""

    return prompt
