# Architecture: tablespec

**Version**: 2.0
**Status**: Updated for post-merge codebase
**Last Updated**: 2026-03-16

**Requirements**: [../01-frame/prd.md](../01-frame/prd.md)

## Overview

tablespec is a Python library organized as a layered architecture where the UMF Pydantic models form the core, with schema generation, type mappings, Great Expectations integration, profiling, prompt generation, validation, CLI, Excel conversion, change management, sample data generation, quality baselines, and domain inference as modules radiating outward.

## Module Dependency Graph

```
                         ┌─────────────────────┐
                         │   models/umf.py     │  Core: Pydantic UMF models
                         │   (source of truth) │
                         └─────────┬───────────┘
                                   │
         ┌──────────┬──────────┬───┼───┬──────────┬──────────┐
         │          │          │   │   │          │          │
   ┌─────▼────┐ ┌──▼─────┐ ┌─▼───▼─┐ │  ┌───────▼──┐ ┌────▼─────┐
   │type_     │ │schemas/│ │umf_   │ │  │umf_loader│ │naming.py │
   │mappings  │ │generat.│ │valid. │ │  │(split/   │ │date_     │
   │(hub)     │ │        │ │       │ │  │ JSON)    │ │formats   │
   └────┬─────┘ └────────┘ └───────┘ │  └─────┬───┘ └──────────┘
        │                             │        │
   ┌────┼──────┬──────────────────────┘   ┌────▼─────────────────┐
   │    │      │                          │                      │
┌──▼──┐ │ ┌───▼──────────┐          ┌────▼─────┐  ┌────────────┐
│gx_  │ │ │gx_processor  │          │umf_diff  │  │excel_      │
│base │ │ │gx_extractor  │          │change_   │  │converter   │
│line │ │ │gx_validator  │          │applier   │  │            │
└──┬──┘ │ └──────────────┘          │changelog │  └────────────┘
   │    │                           └──────────┘
   │    │
┌──▼────▼────┐ ┌──────────┐ ┌────────────┐ ┌──────────────┐
│profiling/  │ │prompts/  │ │inference/  │ │formatting/   │
│            │ │          │ │domain_types│ │yaml_formatter│
└────────────┘ └──────────┘ └────────────┘ └──────────────┘
      │
┌─────▼──────────┐  ┌──────────────┐  ┌──────────────┐
│validation/     │  │sample_data/  │  │quality/      │
│table_validator │  │engine        │  │baseline_svc  │
│[PySpark]       │  │              │  │[PySpark]     │
└────────────────┘  └──────────────┘  └──────────────┘
                                           │
                         ┌─────────────────┐│┌──────────┐
                         │merge.py         │││cli.py    │
                         │[PySpark]        │││(typer)   │
                         └─────────────────┘│└──────────┘
                         ┌──────────────────┘
                         │sync_baseline.py
                         └─────────────────┘
```

## Module Responsibilities

### Core Layer

**models/umf.py** - Pydantic models defining the UMF schema. All other modules depend on these models. Models enforce constraints at runtime (column name patterns, unique names, type-specific requirements, extra field rejection).

**type_mappings.py** - Central type conversion hub. All type translations (UMF to PySpark, JSON, GX) go through this module. Case-insensitive, with safe defaults for unknown types.

### Schema Layer

**schemas/generators.py** - Stateless functions that accept UMF dicts and produce formatted output (SQL DDL strings, PySpark code strings, JSON Schema dicts). Depends on type_mappings for conversions.

**schemas/*.json** - Static JSON schemas for UMF validation and GX expectation type reference.

### Great Expectations Layer

**gx_baseline.py** - `BaselineExpectationGenerator` produces deterministic expectations from UMF metadata. `UmfToGxMapper` composes full expectation suites. No external GX dependency for generation.

**gx_constraint_extractor.py** - Reverse direction: extract value sets, regex patterns, and formats from existing GX suites back into UMF-compatible constraints.

**gx_schema_validator.py** - Validates expectation types against the GX library by attempting instantiation. Used for schema correctness checking.

**validation/gx_processor.py** - Processes AI-generated expectation suites: merges with baselines, deduplicates, validates GX 1.6+ format, outputs YAML.

### Profiling Layer

**profiling/types.py** - Dataclasses for profiling results (`ColumnProfile`, `DataFrameProfile`).

**profiling/spark_mapper.py** - Converts Spark DataFrame schemas to UMF. Requires PySpark.

**profiling/deequ_mapper.py** - Enriches UMF with Deequ profiling results (completeness, statistics, nullable inference).

### Prompt Layer

**prompts/** - Stateless functions generating structured LLM prompts. Each prompt module focuses on one enrichment task. Prompts reference the expectation guide for valid types and parameters.

### Validation Layer

**validation/table_validator.py** - Validates Spark DataFrames against UMF specs. Requires PySpark. Produces structured error DataFrames.

**umf_validator.py** - Validates UMF files against JSON schema and business rules. Provides default application and duplicate fixing utilities.

### CLI Layer

**cli.py** - Typer-based CLI application with Rich output. Commands: convert, validate, info, batch-convert, changelog, sync. Conditional registration of validator-dependent commands.

### Excel Conversion Layer

**excel_converter.py** - Bidirectional Excel/UMF conversion. `UMFToExcelConverter` exports with data validation dropdowns. `ExcelToUMFConverter` imports with strict validation.

**excel_import_git.py** - Git-integrated Excel import with atomic per-change commits.

### Change Management Layer

**umf_loader.py** - `UMFLoader` with auto-detection of split vs JSON format. Bidirectional conversion.

**umf_diff.py** - `UMFDiff` comparing two UMF objects. Detects column, validation, metadata, and relationship changes.

**umf_change_applier.py** - Applies individual `UMFColumnChange`/`UMFMetadataChange`/`UMFValidationChange` to produce modified UMF copies.

**changelog_generator.py** - Git history-based changelog generation. Uses `YAMLDiffParser` for detailed change detection.

**models/changelog.py** - Pydantic models for changelog entries (`ChangeEntry`, `ChangeDetail`, `ChangeType`).

### Sample Data Layer

**sample_data/** - Healthcare-specific sample data generation from UMF specs. Key modules: `engine.py` (orchestrator), `generators.py` (healthcare generators), `column_value_generator.py`, `constraint_handlers.py`, `foreign_keys.py` (referential integrity), `graph.py` (relationship DAG), `registry.py` (key tracking), `validation.py` (output validation).

### Quality Baseline Layer

**quality/** - Baseline capture and comparison. `baseline_service.py` (high-level API, requires PySpark), `baseline_storage.py` (models and persistence), `executor.py` (quality check execution), `storage.py` (result storage).

### Inference Layer

**inference/domain_types.py** - `DomainTypeRegistry` and `DomainTypeInference`. YAML-driven domain type detection from column names, descriptions, and sample values.

### Formatting Layer

**formatting/yaml_formatter.py** - Idempotent YAML formatting with ruamel.yaml. Key sorting, literal block scalars, comment preservation.

**formatting/constants.py** - Formatting configuration constants.

### Utility Modules

**naming.py** - `to_spark_identifier()`, `position_sort_key()` for naming conventions and column ordering.

**date_formats.py** - Supported date/datetime format definitions, validation, and strftime conversion.

**merge.py** - Spark-based table merge with survivorship rules (requires PySpark).

**sync_baseline.py** - Synchronize metadata columns and baseline validations across table definitions.

**dependency_resolver.py** - Pipeline dependency loading and cycle detection.

## Optional Dependency Strategy

PySpark is isolated to modules that require it:
- `profiling/spark_mapper.py`
- `validation/table_validator.py`
- `quality/baseline_service.py`
- `merge.py`

Additional optional dependencies use conditional imports:
- `cli.py` requires typer, rich
- `excel_converter.py` requires openpyxl
- `umf_loader.py`, `formatting/` require ruamel.yaml
- `changelog_generator.py` requires gitpython

All conditionally imported in `__init__.py` or at module level with try/except.

## Data Flow Patterns

### Schema Generation Flow
```
UMF YAML → load_umf_from_yaml → UMF model → model_dump() → generator function → output format
```

### Expectation Generation Flow
```
UMF YAML → BaselineExpectationGenerator → baseline expectations
                                          ↓
AI-generated expectations → GXExpectationProcessor → merged + deduplicated suite → YAML
```

### Profiling Enrichment Flow
```
Spark DataFrame → SparkToUmfMapper → base UMF
Deequ JSON → DeequToUmfMapper → enriched UMF (profiling metadata, nullable updates)
```

### Constraint Extraction Flow
```
GX suite YAML → GXConstraintExtractor → value sets, regex, formats → UMF validation_rules
```

### CLI Conversion Flow
```
tablespec convert input output → UMFLoader.detect_format() → load → convert → save
```

### Excel Round-Trip Flow
```
UMF → UMFToExcelConverter → Excel workbook (with dropdowns/validation)
Excel workbook → ExcelToUMFConverter → validated UMF
```

### Change Management Flow
```
UMF v1 + UMF v2 → UMFDiff → list[UMFChange]
UMFChange → UMFChangeApplier → modified UMF copy
Git history → ChangelogGenerator → ChangeEntry[] → formatted changelog
```

### Sample Data Flow
```
UMF specs → SampleDataEngine → RelationshipGraph → ColumnValueGenerator → CSV/JSON
```

### Quality Baseline Flow
```
DataFrame → BaselineService.capture() → RunBaseline (row counts, distributions, stats)
RunBaseline(prev) + RunBaseline(curr) → BaselineService.compare() → drift report
```

## Packaging and Distribution

- Build system: Hatchling with uv-dynamic-versioning
- Distribution: GitHub Pages PyPI-compatible index (`https://easel.github.io/tablespec/simple/`)
- Versioning: Git tag-based via uv-dynamic-versioning (fallback: 0.0.0)
