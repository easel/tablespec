# Alignment Review: umf.schema.json Drift

**Date**: 2026-03-16
**Scope**: `src/tablespec/schemas/umf.schema.json` vs Pydantic model

## Finding

The hand-written `umf.schema.json` has drifted significantly from the Pydantic model (`models/umf.py`), which is the actual source of truth.

### Column Properties

- **JSON Schema**: 15 column properties
- **Pydantic model**: 35 column properties
- **Missing from JSON Schema**: `domain_type`, `key_type`, `provenance_policy`, `derivation_mapping`, `pivot_field`, `pivot_index`, `source`, `canonical_name`, `default`, `fallback_formats`, `exclude_from_change_detection`, `preserve_literal_null`, `null_output_value`, `profiling`, `provenance_notes`, `derived_from`, `derivation_expression`, plus others

### Type Enums

- **JSON Schema**: `STRING, DECIMAL, INTEGER, BIGINT, SMALLINT, TINYINT, DATE, TIMESTAMP, BOOLEAN, FLOAT, DOUBLE`
- **Pydantic model**: `VARCHAR, DECIMAL, INTEGER, DATE, DATETIME, BOOLEAN, TEXT, CHAR, FLOAT`

### additionalProperties

The JSON schema sets `additionalProperties: false` on column items, meaning it **rejects any UMF file using advanced features** (domain_type, key_type, etc.).

## Impact

- `UMFValidator` (in `umf_validator.py`) uses this schema and has its own tests
- The main CLI validation path (`validator.py`) uses `UMF.model_json_schema()` instead — always in sync
- External consumers using `umf.schema.json` for YAML validation (IDE, CI) get false rejections

## Recommendation

Either regenerate `umf.schema.json` from the Pydantic model, or have `UMFValidator` use `UMF.model_json_schema()` like `validator.py` does. The hand-written schema should not exist as a separate artifact that can drift.
