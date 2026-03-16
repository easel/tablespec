# FEAT-003: Type System Mappings

**Status**: Implemented
**Priority**: High

## Description

Central type conversion hub between UMF, PySpark, JSON Schema, and Great Expectations type systems.

## Functions

- `map_to_pyspark_type(data_type)` - UMF to PySpark (e.g., VARCHAR -> StringType())
- `map_to_json_type(data_type)` - UMF to JSON Schema (e.g., INTEGER -> integer)
- `map_to_gx_spark_type(data_type)` - UMF to GX Spark type names

## Supported Types

VARCHAR, STRING, CHAR, INTEGER, INT, BIGINT, SMALLINT, TINYINT, DECIMAL, FLOAT, DOUBLE, BOOLEAN, DATE, TIMESTAMP, TEXT, DATETIME

## Behaviors

- Case-insensitive resolution
- Unknown types default to StringType/string
- DATE maps to StringType (stored as YYYYMMDD strings)

## Source

- `src/tablespec/type_mappings.py`
