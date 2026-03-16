# US-009: Validate a DataFrame Against a UMF Schema

**Parent Feature**: [FEAT-007 - Table Validation](../features/FEAT-007-validation.md)

## User Story

**As a** data engineer running a PySpark pipeline,
**I want to** validate a DataFrame against its UMF specification at runtime,
**so that** I catch schema drift, type mismatches, missing columns, and business rule violations before data lands in the target table.

## Acceptance Criteria

- [ ] `TableValidator` validates a Spark DataFrame against a UMF schema, checking for missing columns, extra columns, data type mismatches, and LOB-specific nullable violations (requires `tablespec[spark]`)
- [ ] Business rule validation covers uniqueness, format patterns, and value constraints defined in UMF `ValidationRules`
- [ ] Validation errors are returned in a structured format matching `VALIDATION_ERROR_SCHEMA` for programmatic consumption
- [ ] `UMFValidator` validates UMF files themselves against the JSON schema plus business rules (VARCHAR length defaults, DECIMAL precision defaults, duplicate column name fixing)
- [ ] Validation can be run against a single file, a data dictionary, or a directory of UMF files
