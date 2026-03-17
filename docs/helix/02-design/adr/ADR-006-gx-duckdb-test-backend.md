# ADR-006: GX DuckDB Test Backend

## Status

Proposed

## Context

Testing GX expectations currently requires PySpark, which brings JVM startup cost, cluster configuration, and heavyweight dependencies. This makes test iteration slow and prevents running GX-based tests in lightweight CI environments.

Great Expectations supports three execution engines:

- **Pandas**: Pure Python, but different null handling, type coercion, and mutability semantics from Spark SQL.
- **Spark**: Production-accurate but heavyweight.
- **SqlAlchemy**: SQL semantics close to Spark SQL. DuckDB provides a pip-installable SqlAlchemy backend (`duckdb:///:memory:`) with no system dependencies.

GX does NOT support Polars as an execution engine.

DuckDB via SqlAlchemy provides the closest semantic match to Spark SQL for testing purposes: proper NULL propagation, SQL-standard type coercion, and immutable query semantics.

## Decision

Use Great Expectations with DuckDB via SqlAlchemy (`duckdb:///:memory:`) as the lightweight test and non-Spark execution engine.

A proof-of-concept spike must be completed before adoption. The spike must confirm:

1. GX 1.6+ SqlAlchemy datasource creation with DuckDB connection string.
2. Batch loading from CSV and Parquet files.
3. Expectation execution with correct results.
4. Result format compatibility with existing validation reporting code.

If the spike fails, fall back to Pandas execution engine (known to work with GX) and accept semantic differences from Spark.

### Raw vs Ingested Stage Handling

- **Raw stage**: `read_csv('data.csv', all_varchar=true)` -- all columns loaded as VARCHAR, matching Bronze.Raw semantics.
- **Ingested stage**: `TRY_CAST` to UMF-declared types -- cast failures become NULL, detectable as validation errors matching Bronze.Ingested semantics.

### Dependency

`duckdb` and `duckdb-engine` packaged under `tablespec[duckdb]` optional extra. Not a core dependency, consistent with ADR-003's approach to optional heavyweight dependencies.

## Consequences

### Positive

- Sub-second GX test execution without JVM startup.
- SQL semantics close to production Spark SQL, unlike Pandas.
- Enables FEAT-016 test harness and FEAT-023 `tablespec preview --against` command.
- pip-installable with no system dependencies beyond Python.

### Negative

- DuckDB SQL dialect is not identical to Spark SQL -- edge cases may diverge.
- GX SqlAlchemy+DuckDB integration is unverified in this codebase; spike may fail.
- Adds another optional dependency group to manage.
- Tests passing on DuckDB do not guarantee identical behavior on Spark -- integration tests with Spark remain necessary for production confidence.
