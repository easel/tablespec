"""Survivorship prompt generator - Generates data survivorship mapping prompts."""

from pathlib import Path

from .utils import _load_umf


def _generate_survivorship_prompt(target_table_name: str, umf_dir: Path) -> str:
    """Generate survivorship prompt with actual table and column information."""
    # Load the target table UMF
    target_umf_file = umf_dir / f"{target_table_name}.specs.umf.yaml"
    if not target_umf_file.exists():
        msg = f"UMF file not found: {target_umf_file}"
        raise FileNotFoundError(msg)

    target_umf = _load_umf(target_umf_file)

    # Find all provided tables (potential sources)
    source_tables = []
    for umf_file in umf_dir.glob("*.specs.umf.yaml"):
        umf = _load_umf(umf_file)
        table_type = umf.get("table_type")
        if not table_type:
            # Fallback to checking source file for backward compatibility
            source_file = umf.get("source_file", "")
            if (
                "outbound" in source_file.lower()
                or "inbound" not in source_file.lower()
            ):
                table_type = "provided"

        if table_type == "provided":
            source_tables.append(umf)

    # Collect data for template
    table_names = sorted([umf.get("table_name", "") for umf in source_tables])

    # Build table-column mapping
    table_columns = {}
    for source_umf in sorted(source_tables, key=lambda x: x.get("table_name", "")):
        table_name = source_umf.get("table_name", "")
        columns = [col.get("name", "") for col in source_umf.get("columns", [])]
        table_columns[table_name] = columns

    # Build target column descriptions
    target_columns = []
    for col in target_umf.get("columns", []):
        col_name = col.get("name", "")
        col_type = col.get("data_type", "VARCHAR")
        col_desc = col.get("description", "")[:80]
        target_columns.append(f"- **{col_name}** ({col_type}): {col_desc}")

    # Build table name list
    table_name_list = "\n".join(f"- {name}" for name in table_names)

    # Build column enum sections
    column_sections = []
    for table_name, columns in table_columns.items():
        column_list = "\n".join(f"- {col}" for col in columns)
        column_sections.append(f"\n**{table_name}:**\n{column_list}")

    # Generate structured prompt using template literal
    return f"""# Survivorship Mapping: {target_table_name}

Generate JSON mappings to derive columns from source tables using ONLY the exact column names provided below.

## REQUIRED: Use Only These Exact Names

### Valid Table Names (enum):
{table_name_list}

### Valid Column Names (per table):

{chr(10).join(column_sections)}

## Target Table: {target_table_name}
### Columns needing mappings:
{chr(10).join(target_columns)}

## REQUIRED OUTPUT FORMAT

Your output MUST be valid JSON with this structure:

```json
{{
  "metadata": {{
"table_name": "{target_table_name}",
"version": "4.0",
"description": "Brief description of target table purpose",
"primary_key": "PRIMARY_KEY_COLUMN",
"generated_by": "pulseflow-pipeline-ai"
  }},
  "mappings": {{
"TARGET_COLUMN_NAME": {{
  "candidates": [
    {{
      "table": "ExactTableNameFromEnum",
      "column": "ExactColumnNameFromEnum",
      "priority": 1
    }}
  ],
  "survivorship": {{
    "strategy": "highest_priority",
    "description": "Brief explanation of why this strategy"
  }}
}}
  }},
  "survivorship_strategies": {{
"highest_priority": {{
  "description": "Select value from highest priority source",
  "rules": [
    "Check candidates in priority order",
    "Return first non-null value",
    "Use default if all null"
  ]
}},
"most_recent_date": {{
  "description": "Select most recent date value",
  "rules": [
    "Convert all candidates to date",
    "Return maximum date value",
    "If all null, return null"
  ]
}}
  }},
  "normalization": {{
"phone_numbers": {{
  "description": "Standardize phone numbers to 10-digit format",
  "rules": [
    "Remove all non-digit characters",
    "Remove leading '1' if present",
    "Validate exactly 10 digits remain"
  ],
  "example": "(555) 123-4567 → 5551234567"
}}
  }}
}}
```

## STRICT OUTPUT REQUIREMENTS:
1. **ONLY use table names from the 'Valid Table Names' enum above**
2. **ONLY use column names from the 'Valid Column Names' for each table**
3. **DO NOT invent, modify, or guess any names not listed**
4. **If a logical mapping seems to exist but exact name doesn't match, skip it**
5. **Include all sections: metadata, mappings, survivorship_strategies, normalization**
6. **Validate every table/column reference against the enums above**
7. **Return ONLY the JSON object - no markdown code blocks, no explanations**

## JSON FORMATTING NOTES:
- ✅ Arrows (→, ->, =>) are fine in JSON strings - they're just text
- ✅ Colons, commas, quotes are handled naturally in JSON
- ✅ Use arrow characters freely in examples if they help clarity
- ✅ JSON is more forgiving than YAML for special characters

## FINAL REQUIREMENT
Return ONLY a valid JSON object. Your output will be parsed with `json.load()`.
- NO markdown code blocks (no ```json)
- NO explanations or commentary
- ONLY the raw JSON object
"""
