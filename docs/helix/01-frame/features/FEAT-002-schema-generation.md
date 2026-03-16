# FEAT-002: Schema Generation

**Status**: Implemented
**Priority**: High

## Description

Generate schema definitions in multiple output formats from UMF metadata.

## Supported Formats

1. **SQL DDL** (`generate_sql_ddl`) - CREATE TABLE with NOT NULL, column/table comments, suggested indexes
2. **PySpark** (`generate_pyspark_schema`) - StructType Python code with correct type imports
3. **JSON Schema** (`generate_json_schema`) - Draft-07 schema with type mapping, maxLength, examples

## Source

- `src/tablespec/schemas/generators.py`
- Type conversions via `src/tablespec/type_mappings.py`
