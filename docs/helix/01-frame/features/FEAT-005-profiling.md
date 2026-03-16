# FEAT-005: Profiling Integration

**Status**: Implemented
**Priority**: Medium

## Description

Convert DataFrame profiling results from Spark and Deequ into UMF format for schema enrichment.

## Components

### Types (`profiling/types.py`)
- `ColumnProfile` - Per-column profiling data (completeness, distinct count, statistics, histogram)
- `DataFrameProfile` - Aggregate profiling result

### Spark Mapper (`profiling/spark_mapper.py`) [requires PySpark]
- `SparkToUmfMapper` - Convert Spark DataFrame schema to UMF
- Maps Spark types to UMF types
- Preserves nullable and DecimalType precision/scale

### Deequ Mapper (`profiling/deequ_mapper.py`)
- `DeequToUmfMapper` - Enrich UMF with Deequ profiling results
- Adds profiling metadata (tool, version, timestamp)
- Updates nullable based on completeness
- Includes statistics (min, max, mean, stddev)

## Related

- Domain type inference (FEAT-013) can enrich profiling results with semantic types
- Quality baselines (FEAT-012) extend profiling with drift detection

## Source

- `src/tablespec/profiling/`
