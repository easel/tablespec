"""Column-level validation prompt generator - Generates focused per-column validation prompts."""

import hashlib
from typing import Any

from tablespec.gx_baseline import BaselineExpectationGenerator
from tablespec.prompts.expectation_guide import (
    format_quick_reference,
    get_pending_decision_tree,
)
from tablespec.type_mappings import map_to_gx_spark_type


def _should_generate_column_prompt(col: dict[str, Any]) -> bool:
    """Check if column needs a dedicated validation prompt.

    Only generate prompts for columns with COMPLEX rules that require LLM reasoning:
    - Format constraints (complex patterns beyond simple length)
    - Enumerated values (value sets)
    - Complex descriptions with validation logic

    Do NOT generate for simple cases (basic nullability, type, length) as these are
    handled by baseline expectations.

    Args:
    ----
        col: Column dict from UMF data

    Returns:
    -------
        True if column should get dedicated validation prompt

    """
    # Generate if has format constraint (complex patterns)
    if col.get("format"):
        return True

    # Generate if has enumerated sample values (likely value set)
    sample_values = col.get("sample_values", [])
    if sample_values and len(sample_values) <= 10:
        return True

    # Generate if description contains complex validation indicators
    description = col.get("description", "").lower()
    complex_validation_indicators = [
        "format",
        "pattern",
        "must be",
        "should be",
        "valid values",
        "allowed values",
        "options:",
        "range",
        "between",
        "digit",  # Specific digit patterns
        "checksum",
        "algorithm",
        "conditional",
        "if",
        "when",
    ]
    return bool(
        any(indicator in description for indicator in complex_validation_indicators)
    )


def _generate_column_validation_prompt(
    umf_data: dict[str, Any], col: dict[str, Any]
) -> str:
    """Generate focused validation prompt for a single column.

    Args:
    ----
        umf_data: Full UMF table data for context
        col: Specific column to generate prompt for

    Returns:
    -------
        Validation prompt string

    """
    table_name = umf_data["table_name"]
    table_desc = umf_data.get("description", "No description available")
    col_name = col["name"]
    col_desc = col.get("description", "No description")
    data_type = col.get("data_type", "VARCHAR")
    gx_type = map_to_gx_spark_type(data_type)
    max_length = col.get("max_length")
    nullable = col.get("nullable", {})
    format_spec = col.get("format", "")
    sample_values = col.get("sample_values", [])
    notes = col.get("notes", [])

    # Calculate prompt hash for tracking
    prompt_content = f"{table_name}:{col_name}:{data_type}:{format_spec}:{max_length}"
    prompt_hash = hashlib.sha256(prompt_content.encode()).hexdigest()

    # Generate baseline expectations for this column
    generator = BaselineExpectationGenerator()
    baseline_expectations = generator.generate_baseline_column_expectations(col)

    # Required LOBs
    req_lobs = [lob for lob, is_null in nullable.items() if not is_null]

    prompt = f"""╔══════════════════════════════════════════════════════════════╗
║  🚨 CRITICAL: JSON OBJECT OUTPUT WITH HASH TRACKING 🚨       ║
╔══════════════════════════════════════════════════════════════╗

**YOU MUST OUTPUT A JSON OBJECT WITH _prompt_hash AND expectations**

✅ CORRECT:
```
{{
  "_prompt_hash": "{prompt_hash}",
  "expectations": [
    {{"type": "...", "kwargs": {{}}, "meta": {{}}}},
    {{"type": "...", "kwargs": {{}}, "meta": {{}}}}
  ]
}}
```

❌ WRONG - NO BARE ARRAY:
```
[...]  ← DO NOT DO THIS
```

❌ NO markdown code blocks (no ```json)
❌ NO comments (// or /* */)
❌ NO explanations

╚══════════════════════════════════════════════════════════════╝

# Column Validation: {table_name}.{col_name}

## Table Context

**Table**: {table_name}
**Description**: {table_desc}

Available columns:
"""

    # List all columns for context (cross-column awareness)
    for c in umf_data.get("columns", []):
        c_name = c["name"]
        c_type = c.get("data_type", "VARCHAR")
        c_gx_type = map_to_gx_spark_type(c_type)
        prompt += f"- {c_name} ({c_type}→{c_gx_type})\n"

    prompt += f"""
## Column Details

**{col_name}** ({data_type}→{gx_type}): {col_desc}
"""

    if req_lobs:
        prompt += f"- **Required for LOBs**: {', '.join(req_lobs)}\n"
    if format_spec:
        prompt += f"- **Format**: {format_spec}\n"
    if max_length:
        prompt += f"- **Max Length**: {max_length}\n"
    if sample_values:
        # Filter out header pollution
        valid_samples = []
        for sample in sample_values[:5]:
            sample_str = str(sample).strip()
            if not sample_str:
                continue
            is_header = (
                sample_str.lower() == col_name.lower()
                or sample_str.upper() == col_name.upper()
                or sample_str.replace(" ", "_").lower() == col_name.lower()
            )
            if not is_header:
                valid_samples.append(sample)
        if valid_samples:
            prompt += (
                f"- **Sample Values**: {', '.join(str(s) for s in valid_samples[:3])}\n"
            )
    if notes:
        prompt += f"- **Notes**: {notes[0]}\n"

    prompt += f"""
## ✅ Baseline Expectations Already Generated for This Column

**{len(baseline_expectations)} expectations have been automatically generated:**
"""

    for exp in baseline_expectations:
        exp_type = exp["type"].replace("expect_", "").replace("_", " ")
        prompt += f"- `{exp['type']}` - {exp_type}\n"

    prompt += """
**⚠️ DO NOT generate these basic expectations:**
- Column existence (already generated)
- Column type validation (already generated)
- Basic nullability (already generated)
- Max length constraint (already generated)
- Basic date format (already generated for DATE columns)

**These baseline expectations will be automatically merged with your output.**

## Your Task: Column-Specific Rules for `{col_name}` ONLY

⚠️ **CRITICAL**: This is a COLUMN-LEVEL validation prompt for a SINGLE column: `{col_name}`

Generate expectations ONLY for THIS column:
1. **Enumerated value sets** - `expect_column_values_to_be_in_set` for status/code/flag fields
   - Extract from "Ex: A, B, C" patterns in description
   - Look for ≤10 distinct sample values
   - Column names containing STATUS/TYPE/CODE/FLAG

2. **Complex format patterns** - `expect_column_values_to_match_regex` for IDs, codes
   - Member IDs, NPIs, phone numbers with specific patterns
   - NOT just max length (that's handled by baseline)

3. **Value ranges** - `expect_column_values_to_be_between` for numeric constraints
   - Age ranges, percentages, counts with business limits

4. **Complex unmappable rules** - `expect_validation_rule_pending_implementation`
   - Checksum validation (Luhn, mod10)
   - External system lookups
   - Complex domain-specific algorithms

❌ **DO NOT generate**:
- Column existence → Already handled by baseline
- Type validation → Already handled by baseline
- Basic nullability → Already handled by baseline
- Max length → Already handled by baseline
- Basic date format → Already handled by baseline
- Multi-column rules → Use table-level prompts

"""

    # Add quick reference and pending tree
    quick_reference = format_quick_reference(context="column")
    pending_tree = get_pending_decision_tree()

    prompt += f"""
{quick_reference}

{pending_tree}

**Type mapping reference:**
- VARCHAR/STRING → StringType
- INTEGER → IntegerType
- BIGINT → LongType
- DECIMAL → DecimalType
- DATE (YYYYMMDD) → StringType (date format validated by baseline)

**Description requirements:**
Each `meta.description` must include:
1. What is validated
2. Why (quote from description/notes)
3. Specific constraints
4. Sample values influence
5. LOB requirements if applicable

**Example output structure:**

```json
{{
  "_prompt_hash": "{prompt_hash}",
  "expectations": [
    {{
      "type": "expect_column_values_to_be_in_set",
      "kwargs": {{
        "column": "{col_name}",
        "value_set": ["A", "B", "C"]
      }},
      "meta": {{
        "description": "{col_name} must be one of A, B, or C per description. Values from sample data.",
        "severity": "critical"
      }}
    }}
  ]
}}
```

## Critical Rules

- Severity levels: `"critical"`, `"warning"`, `"info"`
- Use GX 1.6+ format: `type`, `kwargs`, `meta` (NOT `expectation_type`)
- Output MUST be a JSON object with `_prompt_hash` and `expectations` keys
- NO bare arrays
- NO markdown fences
- NO comments
- Must parse with `json.loads()`
- Generate ONLY single-column expectations (all must have `"column": "{col_name}"` in kwargs)

**OUTPUT ONLY THE JSON OBJECT. NO OTHER TEXT.**
"""

    return prompt
