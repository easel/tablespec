# Product Requirements Document: tablespec

**Version**: 2.0
**Status**: Updated for post-merge codebase
**Last Updated**: 2026-03-16

## Overview

tablespec is a Python library for working with table schemas in Universal Metadata Format (UMF). It provides type-safe models, validation, profiling integration, and schema generation tools for healthcare data platforms.

## Goals

1. Establish UMF as the single source of truth for table schema definitions
2. Enable deterministic schema generation across SQL DDL, PySpark, and JSON Schema
3. Integrate with Great Expectations for automated baseline validation
4. Support profiling-driven schema enrichment from Spark and Deequ
5. Provide structured LLM prompts for AI-assisted metadata enrichment
6. Provide a CLI for schema management, conversion, and validation workflows
7. Enable bidirectional Excel conversion for domain expert collaboration
8. Support git-friendly split-format UMF with change tracking and changelogs
9. Generate realistic sample data from UMF specifications

## Target Audience

- Data engineers working with PySpark and SQL pipelines
- Data quality engineers managing Great Expectations suites
- Platform teams managing schema standards across Medicaid (MD), Medicare Part D (MP), and Medicare (ME) lines of business

## Functional Requirements

### FR-1: UMF Model and I/O

- FR-1.1: Pydantic models for UMF format with runtime validation
- FR-1.2: Support 10 data types: VARCHAR, CHAR, TEXT, INTEGER, DECIMAL, FLOAT, DATE, DATETIME, TIMESTAMP, BOOLEAN
- FR-1.3: Per-LOB nullable configuration (MD, MP, ME)
- FR-1.4: Validation rules at table and column level
- FR-1.5: Foreign key relationships with confidence scoring
- FR-1.6: Index definitions
- FR-1.7: YAML serialization/deserialization with Pydantic validation
- FR-1.8: Column name validation (alphanumeric + underscore, max 128 chars)
- FR-1.9: Unique column name enforcement
- FR-1.10: UMF metadata with pipeline phase tracking (1-7)

### FR-2: Schema Generation

- FR-2.1: SQL DDL generation with NOT NULL, column comments, table comments, and suggested indexes
- FR-2.2: PySpark StructType code generation with correct type imports
- FR-2.3: JSON Schema (draft-07) generation with type mapping, maxLength, and examples

### FR-3: Type Mappings

- FR-3.1: UMF to PySpark type mapping (all 9 types plus BIGINT, SMALLINT, TINYINT, DOUBLE, STRING, TIMESTAMP)
- FR-3.2: UMF to JSON Schema type mapping
- FR-3.3: UMF to Great Expectations Spark type mapping
- FR-3.4: Case-insensitive type resolution
- FR-3.5: Safe defaults (unknown types map to StringType/string)

### FR-4: Great Expectations Integration

- FR-4.1: Baseline expectation generation from UMF metadata (column existence, types, nullability, length, date format)
- FR-4.2: Structural expectations (column count, column order)
- FR-4.3: Expectation suite composition (baseline + profiling + AI-generated)
- FR-4.4: Constraint extraction from existing GX suites (value sets, regex patterns, strftime formats)
- FR-4.5: GX schema validation against JSON schema and GX library
- FR-4.6: Expectation suite processing with baseline merging and deduplication
- FR-4.7: Support for GX 1.6+ format (not legacy)

### FR-5: Profiling Integration

- FR-5.1: Spark DataFrame to UMF mapping (requires PySpark)
- FR-5.2: Deequ profiling results to UMF enrichment
- FR-5.3: Profiling metadata (tool, version, timestamp, sample size)
- FR-5.4: Nullable inference from completeness metrics

### FR-6: LLM Prompt Generation

- FR-6.1: Documentation enrichment prompts
- FR-6.2: Table-level validation rule prompts (multi-column expectations)
- FR-6.3: Column-level validation rule prompts (single-column expectations)
- FR-6.4: Foreign key relationship discovery prompts with cardinality estimation
- FR-6.5: Data survivorship mapping prompts
- FR-6.6: Prompt hash tracking for deduplication
- FR-6.7: Healthcare domain knowledge in prompts (member/provider/claim IDs, drug codes)

### FR-7: Table Validation

- FR-7.1: DataFrame validation against UMF specifications (requires PySpark)
- FR-7.2: Schema validation (missing/extra columns)
- FR-7.3: Data type validation
- FR-7.4: LOB-specific nullable validation
- FR-7.5: Business rule validation (uniqueness, format, value constraints)
- FR-7.6: Structured validation error output with VALIDATION_ERROR_SCHEMA

### FR-8: CLI Interface

- FR-8.1: Typer-based CLI (`tablespec` command) with Rich output formatting
- FR-8.2: `convert` command for format conversion (JSON, split, Excel)
- FR-8.3: `validate` command for UMF validation with pipeline context
- FR-8.4: `info` command for schema summary display
- FR-8.5: `batch-convert` command for directory-wide format conversion
- FR-8.6: `generate` command for SQL DDL, PySpark schema, and JSON Schema output to stdout

### FR-9: Excel Bidirectional Conversion

- FR-9.1: UMF to Excel export with data validation dropdowns and formatting
- FR-9.2: Excel to UMF import with strict validation
- FR-9.3: Helper columns (validation status, error messages) for domain experts
- FR-9.4: Round-trip fidelity between Excel and UMF formats

### FR-10: Split-Format UMF

- FR-10.1: Directory-based UMF storage (`table.yaml` + `columns/*.yaml`)
- FR-10.2: `UMFLoader` with automatic format detection (split vs JSON)
- FR-10.3: Bidirectional conversion between split and JSON formats
- FR-10.4: Git-friendly structure for per-column change tracking

### FR-11: Schema Change Management

- FR-11.1: UMF diffing (`UMFDiff`) detecting column, validation, metadata, and relationship changes
- FR-11.2: Atomic change application (`UMFChangeApplier`) for per-change commits
- FR-11.3: Git-based changelog generation from commit history
- FR-11.4: YAML diff parsing for detailed change detection
- FR-11.5: Changelog models with structured change entries and types

### FR-12: Sample Data Generation

- FR-12.1: Healthcare-specific sample data from UMF specifications
- FR-12.2: Constraint-aware generation (value sets, regex patterns, date formats)
- FR-12.3: Foreign key relationship graph for referential integrity
- FR-12.4: Domain type-aware generators (SSN, NPI, phone, state codes)
- FR-12.5: CSV and JSON output with configurable row counts
- FR-12.6: Filename pattern generation from UMF file format specs

### FR-13: Quality Baselines

- FR-13.1: Capture baseline metrics from DataFrames (row counts, distributions, statistics)
- FR-13.2: Baseline storage and retrieval
- FR-13.3: Comparison against previous baselines with drift detection
- FR-13.4: Jensen-Shannon divergence for distribution comparison
- FR-13.5: Baseline sync across table definitions (requires PySpark)

### FR-14: Domain Type Inference

- FR-14.1: Automatic domain type detection from column names and descriptions
- FR-14.2: YAML-based domain type registry (us_state_code, email, phone, etc.)
- FR-14.3: Pattern matching and sample value validation
- FR-14.4: Integration with sample data generation and validation

### FR-15: Table Merge

- FR-15.1: Spark-based merge of multiple table files with UMF metadata (requires PySpark)
- FR-15.2: Survivorship rules from UMF specifications
- FR-15.3: Configurable deduplication and conflict resolution

### FR-16: Naming Utilities

- FR-16.1: `to_spark_identifier()` for canonical snake_case conversion
- FR-16.2: `position_sort_key()` for Excel-style column ordering
- FR-16.3: Naming validation against UMF conventions

### FR-17: Date Format System

- FR-17.1: Supported date/datetime format definitions with UMF notation
- FR-17.2: Format validation and strftime conversion
- FR-17.3: Consistent format handling across sample data, validation, and type conversion

## Non-Functional Requirements

- NFR-1: Python 3.12+ required
- NFR-2: Core library must work without PySpark (optional dependency via `[spark]` extra)
- NFR-3: Pydantic v2+ for model validation
- NFR-4: Type checking with Pyright (basic mode)
- NFR-5: Code formatting with Ruff
- NFR-6: 80%+ test coverage on new code, 100% on critical paths
- NFR-7: Apache 2.0 license
- NFR-8: Distribution via GitHub Pages PyPI-compatible index
- NFR-9: Additional dependencies for extended features: typer, rich (CLI); openpyxl (Excel); ruamel.yaml (split-format YAML); gitpython (changelog)
- NFR-10: Graceful degradation via conditional imports for optional modules

## Out of Scope

- Database connectivity or query execution
- GUI or web interface
- Real-time schema synchronization

**Note**: Data processing capabilities (merge, sample data, quality baselines) are available via the `[spark]` extra but are scoped to UMF-driven workflows, not general-purpose ETL.
