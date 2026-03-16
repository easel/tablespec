# Phase 1: Frame

Requirements and problem definition for tablespec.

## Project-Level Artifacts

- [PRD](prd.md) - Product Requirements Document
- [Principles](principles.md) - Design principles

## Feature Specifications

### Core (original codebase)
- [FEAT-001](features/FEAT-001-umf-models.md) - UMF Models and I/O
- [FEAT-002](features/FEAT-002-schema-generation.md) - Schema Generation
- [FEAT-003](features/FEAT-003-type-mappings.md) - Type System Mappings
- [FEAT-004](features/FEAT-004-gx-integration.md) - Great Expectations Integration
- [FEAT-005](features/FEAT-005-profiling.md) - Profiling Integration
- [FEAT-006](features/FEAT-006-llm-prompts.md) - LLM Prompt Generation
- [FEAT-007](features/FEAT-007-validation.md) - Table Validation

### Extended (post-merge additions)
- [FEAT-008](features/FEAT-008-cli.md) - CLI Interface
- [FEAT-009](features/FEAT-009-excel-conversion.md) - Excel Bidirectional Conversion
- [FEAT-010](features/FEAT-010-change-management.md) - UMF Change Management
- [FEAT-011](features/FEAT-011-sample-data.md) - Sample Data Generation
- [FEAT-012](features/FEAT-012-quality-baselines.md) - Quality Baselines
- [FEAT-013](features/FEAT-013-domain-inference.md) - Domain Type Inference
- [FEAT-014](features/FEAT-014-naming-formatting.md) - Naming and Formatting Utilities

## User Stories

### FEAT-001: UMF Models and I/O
- [US-001](user-stories/US-001-load-and-validate-umf-schema.md) - Load and Validate a UMF Schema from YAML
- [US-002](user-stories/US-002-construct-umf-programmatically.md) - Construct a UMF Schema Programmatically

### FEAT-002: Schema Generation
- [US-003](user-stories/US-003-generate-sql-ddl-from-umf.md) - Generate SQL DDL from a UMF Schema

### FEAT-003: Type Mappings
- [US-004](user-stories/US-004-convert-types-between-systems.md) - Convert Column Types Between Type Systems

### FEAT-004: GX Integration
- [US-005](user-stories/US-005-generate-gx-baseline-from-umf.md) - Generate a Great Expectations Baseline from UMF
- [US-006](user-stories/US-006-extract-umf-constraints-from-gx-suite.md) - Extract UMF Constraints from an Existing GX Suite

### FEAT-005: Profiling
- [US-007](user-stories/US-007-convert-profiling-results-to-umf.md) - Convert Profiling Results to UMF

### FEAT-006: LLM Prompts
- [US-008](user-stories/US-008-generate-llm-prompts-for-schema-enrichment.md) - Generate LLM Prompts for Schema Enrichment

### FEAT-007: Validation
- [US-009](user-stories/US-009-validate-dataframe-against-umf.md) - Validate a DataFrame Against a UMF Schema
- [US-018](user-stories/US-018-merge-tables.md) - Merge Table Files with Survivorship

### FEAT-008: CLI Interface
- [US-010](user-stories/US-010-convert-umf-via-cli.md) - Convert UMF Formats via CLI

### FEAT-009: Excel Conversion
- [US-011](user-stories/US-011-excel-round-trip.md) - Round-Trip UMF Through Excel

### FEAT-010: UMF Change Management
- [US-012](user-stories/US-012-split-format-loading.md) - Load UMF from Split-Format Directory
- [US-014](user-stories/US-014-generate-changelog.md) - Generate Changelog from Git History
- [US-015](user-stories/US-015-diff-umf-versions.md) - Diff Two UMF Versions
- [US-020](user-stories/US-020-resolve-dependencies.md) - Resolve Pipeline Dependencies

### FEAT-011: Sample Data Generation
- [US-013](user-stories/US-013-generate-sample-data.md) - Generate Sample Data from UMF

### FEAT-012: Quality Baselines
- [US-016](user-stories/US-016-capture-quality-baseline.md) - Capture and Compare Quality Baselines
- [US-019](user-stories/US-019-sync-baselines.md) - Sync Baseline Validations Across Tables

### FEAT-013: Domain Type Inference
- [US-017](user-stories/US-017-infer-domain-types.md) - Infer Domain Types for Columns

## Status

- Frame phase backfilled from existing codebase and documentation (2026-03-15).
- Updated for post-merge codebase with ~50 new source files across 4 new packages (2026-03-16).
