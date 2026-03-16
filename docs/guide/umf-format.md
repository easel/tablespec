# UMF Format

Universal Metadata Format (UMF) is a YAML-based schema format for describing database tables with rich metadata.

## Structure

```yaml
version: "1.0"
table_name: Medical_Claims
source_file: claims_spec.xlsx
sheet_name: Medical Claims
description: Healthcare claims and billing information
table_type: data_table

columns:
  - name: claim_id
    data_type: VARCHAR
    length: 50
    description: Unique claim identifier
    nullable:
      MD: false  # Medicaid
      MP: false  # Medicare Part D
      ME: false  # Medicare
    sample_values:
      - "CLM001"
      - "CLM002"

  - name: claim_amount
    data_type: DECIMAL
    precision: 10
    scale: 2
    description: Claim amount in USD
    nullable:
      MD: true
      MP: true
      ME: true

validation_rules:
  table_level:
    - rule_type: row_count
      description: Table must not be empty
      severity: error
      parameters:
        min_value: 1

  column_level:
    claim_id:
      - rule_type: uniqueness
        description: claim_id must be unique
        severity: error

relationships:
  foreign_keys:
    - column: provider_id
      references_table: Providers
      references_column: provider_id
      confidence: 0.95

  referenced_by:
    - table: Claim_Lines
      column: claim_id
      foreign_key_column: claim_id

metadata:
  updated_at: 2025-01-15T10:30:00Z
  created_by: data-platform-team
  pipeline_phase: 4
```

## Supported Data Types

- `VARCHAR` - Variable-length string (requires `length`)
- `CHAR` - Fixed-length string
- `TEXT` - Unlimited text
- `INTEGER` - Integer number
- `DECIMAL` - Fixed-precision decimal (supports `precision` and `scale`)
- `FLOAT` - Floating-point number
- `DATE` - Date without time
- `DATETIME` - Date with time
- `BOOLEAN` - True/false value
