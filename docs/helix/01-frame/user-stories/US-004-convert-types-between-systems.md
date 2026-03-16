# US-004: Convert Column Types Between Type Systems

**Parent Feature**: [FEAT-003 - Type System Mappings](../features/FEAT-003-type-mappings.md)

## User Story

**As a** data engineer working across PySpark and SQL environments,
**I want to** convert UMF column types to PySpark, JSON Schema, and Great Expectations type representations,
**so that** I can use a single UMF schema as the source of truth across all downstream systems without manually mapping types.

## Acceptance Criteria

- [ ] `map_to_pyspark_type(data_type)` returns the correct PySpark type for all supported UMF types (VARCHAR, INTEGER, DECIMAL, DATE, BOOLEAN, etc.)
- [ ] `map_to_json_type(data_type)` returns correct JSON Schema type strings
- [ ] `map_to_gx_spark_type(data_type)` returns correct Great Expectations Spark type names
- [ ] Type resolution is case-insensitive (e.g., "varchar" and "VARCHAR" both work)
- [ ] Unknown/unrecognized types default gracefully to string equivalents rather than raising errors
- [ ] DATE types map to StringType (reflecting YYYYMMDD string storage convention)
