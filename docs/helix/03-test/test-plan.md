# Test Plan: tablespec

**Version**: 2.0
**Status**: Updated for post-merge codebase
**Last Updated**: 2026-03-16

**Requirements**: [../01-frame/prd.md](../01-frame/prd.md)
**Architecture**: [../02-design/architecture.md](../02-design/architecture.md)

## Strategy

Two-tier testing: unit tests for pure Python logic and integration tests for end-to-end workflows. PySpark-dependent tests are conditionally skipped when PySpark is not available.

## Test Infrastructure

- **Framework**: pytest
- **Coverage**: pytest-cov with term-missing and HTML reports
- **Mocking**: pytest-mock (MagicMock for Spark objects)
- **Async**: anyio with asyncio backend
- **CI**: GitHub Actions on push/PR to main

## Unit Tests (`tests/unit/`)

| Test File | Module Under Test | Tests | Coverage |
|-----------|-------------------|-------|----------|
| `test_umf_models.py` | `models/umf.py` | 45 | Pydantic validation, YAML I/O, all model types |
| `test_type_mappings.py` | `type_mappings.py` | 10 | All type conversions, case insensitivity, defaults |
| `test_schema_generators.py` | `schemas/generators.py` | 32 | SQL DDL, PySpark, JSON Schema generation |
| `test_gx_baseline.py` | `gx_baseline.py` | 29 | Baseline generation, suite composition, strictness |
| `test_gx_schema_validation.py` | `gx_schema_validator.py` | skipped | GX numpy/pandas compatibility issues |
| `test_expectation_consistency.py` | `prompts/expectation_guide.py` | 9 | Cross-schema consistency (categories, parameters, schema) |
| `test_profiling_mappers.py` | `profiling/` | 13 | Spark and Deequ mappers, statistics, nullable inference |

## Integration Tests (`tests/integration/`)

| Test File | Scope | Tests | Coverage |
|-----------|-------|-------|----------|
| `test_umf_workflow.py` | End-to-end | 8 | Create/save/load UMF, generate all schemas, validation errors, round-trip, modification |

## Coverage Targets

- 80%+ on new code
- 100% on critical paths (model validation, type mappings, schema generation)

## Test Patterns

- **Parametrized tests** for comprehensive type mapping coverage
- **Conditional skipping** (`@pytest.mark.skipif`) for optional PySpark dependency
- **MagicMock** for Spark objects in unit tests
- **Round-trip testing** (save -> load -> save -> load) for data preservation
- **Cross-file consistency** tests ensuring JSON schemas stay in sync

## Known Gaps

### Pre-Existing (original codebase)
- `test_gx_schema_validation.py` is fully skipped (GX numpy/pandas compatibility)
- No dedicated unit tests for `prompts/` modules (documentation, validation, relationship, survivorship prompt generators)
- No dedicated unit tests for `gx_constraint_extractor.py`
- No dedicated unit tests for `umf_validator.py`
- No load/stress testing

### New Modules (post-merge, no tests yet)
- `cli.py` - CLI commands (convert, validate, info, batch-convert, changelog, sync)
- `excel_converter.py` - Excel bidirectional conversion
- `excel_import_git.py` - Git-integrated Excel import
- `umf_loader.py` - Split/JSON format loading and conversion
- `umf_diff.py` - UMF comparison and change detection
- `umf_change_applier.py` - Atomic change application
- `changelog_generator.py` / `changelog_diff_parser.py` / `changelog_formatter.py` - Git changelog
- `models/changelog.py` - Changelog Pydantic models
- `sample_data/` - Entire sample data generation package (engine, generators, constraints, foreign keys, graph, registry, validation)
- `quality/` - Quality baseline package (baseline_service, baseline_storage, executor, storage)
- `inference/domain_types.py` - Domain type registry and inference
- `naming.py` - Naming utilities
- `date_formats.py` - Date format system
- `formatting/` - YAML formatting package
- `merge.py` - Table merge
- `sync_baseline.py` - Baseline synchronization
- `dependency_resolver.py` - Pipeline dependency resolution
- `completeness_validator.py` - Completeness validation
- `relationship_validator.py` - Relationship validation
- `naming_validator.py` - Naming validation
- `casting_utils.py` - Type casting utilities
- `format_utils.py` - Format utilities
- `output_formatting.py` - Output formatting
- `survivorship_display.py` - Survivorship display
- `spark_factory.py` - Spark session factory
- `gx_wrapper.py` - GX wrapper utilities
- `validator.py` - Unified validator

## Running Tests

```bash
make test            # All tests
make test-unit       # Unit tests only
make test-integration # Integration tests only
make coverage        # With coverage report
```
