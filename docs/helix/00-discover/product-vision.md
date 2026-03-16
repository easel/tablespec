# Product Vision: tablespec

## Vision Statement

Provide a single, portable, type-safe Python library that makes Universal Metadata Format (UMF) the canonical source of truth for table schemas across data platform tooling -- from schema generation and validation to profiling and LLM-assisted enrichment.

## Problem

Healthcare data platforms work with table schemas across many tools and formats (SQL DDL, PySpark, JSON Schema, Great Expectations). Without a single authoritative schema format, definitions drift between tools, validation rules diverge, and onboarding new tables requires redundant manual work across each system.

## Target Users

- **Data engineers** building and maintaining ETL pipelines with PySpark and SQL
- **Data quality engineers** creating and managing Great Expectations validation suites
- **Platform teams** standardizing schema definitions across Medicaid, Medicare Part D, and Medicare lines of business

## Key Outcomes

1. A single YAML-based UMF file is the source of truth for any table's schema
2. Schema generation to SQL DDL, PySpark StructType, and JSON Schema is deterministic and lossless
3. Great Expectations baselines are generated automatically from UMF metadata
4. Existing GX suites can be reverse-engineered back into UMF constraints
5. DataFrame profiling results (Spark, Deequ) feed back into UMF enrichment
6. LLM prompts for documentation, validation, relationship discovery, and survivorship logic are structured and repeatable

## Differentiators

- **UMF as single source of truth**: All conversions are bidirectional where possible
- **Healthcare-domain awareness**: Nullable configuration per LOB (MD/MP/ME), healthcare-specific validation patterns
- **Optional dependency model**: Core library is pure Python; PySpark features are opt-in via `tablespec[spark]`
- **LLM integration**: Structured prompt generators for AI-assisted schema enrichment

## Success Metrics

- Adoption across internal data platform tables
- Zero-drift between UMF definitions and downstream schema artifacts
- Reduction in manual GX suite authoring time
- Coverage of all supported data types across all output formats
