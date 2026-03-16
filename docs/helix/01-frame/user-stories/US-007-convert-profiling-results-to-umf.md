# US-007: Convert Profiling Results to UMF

**Parent Feature**: [FEAT-005 - Profiling Integration](../features/FEAT-005-profiling.md)

## User Story

**As a** data engineer running Spark or Deequ profiling jobs,
**I want to** convert profiling results into UMF format,
**so that** column statistics, completeness metrics, and inferred types enrich the UMF schema and feed into downstream validation and documentation workflows.

## Acceptance Criteria

- [ ] `SparkToUmfMapper` converts a Spark DataFrame schema to a UMF object, mapping Spark types to UMF types and preserving nullable and DecimalType precision/scale (requires `tablespec[spark]`)
- [ ] `DeequToUmfMapper` enriches an existing UMF schema with Deequ profiling results including completeness, distinct counts, min/max/mean/stddev statistics, and profiling metadata (tool, version, timestamp)
- [ ] Nullable fields are updated based on completeness metrics (columns with 100% completeness become non-nullable)
- [ ] `ColumnProfile` and `DataFrameProfile` types provide a consistent structure for profiling data regardless of the source tool
