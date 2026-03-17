# Implementation Plan v2: Design Review Improvements

**Version**: 1.0
**Status**: Proposed
**Last Updated**: 2026-03-17

**Requirements**: ../01-frame/prd.md
**Architecture**: ../02-design/architecture.md
**Test Plan**: ../03-test/test-plan.md

## Overview

Phased implementation plan for improvements identified during design review. The dependency graph ensures each phase has its prerequisites satisfied before work begins.

## Dependency Graph

```
Phase 0: Testing Infrastructure (FEAT-016)  <-- prerequisite for all
  0a: GX DuckDB Harness (spike first, Pandas fallback)
  0b: UMF Builder DSL
  0c: Test discovery conventions + fast marker
  0d: Golden file runner + Hypothesis strategies

Phase 1a: Unified Expectation Model (ADR-005, FEAT-017 partial)
Phase 1b: Validation Pipeline (FEAT-017 remainder)

Phase 2: Custom GX Extensions (FEAT-018)
Phase 3: SQL Generator CTE Mode (FEAT-019)
Phase 4: Domain Type Improvements (FEAT-020)
Phase 5: UMF Loader Improvements (FEAT-021)
Phase 6: Authoring Tools (FEAT-023)
Phase 7: Schema Compatibility Checker (FEAT-022)
```

Phases 1-5 are parallelizable after Phase 0 completes. Phase 6 depends on Phases 1-5. Phase 7 is independent after Phase 0.

## Phase 0: Testing Infrastructure (FEAT-016)

**Prerequisite for all subsequent phases.** No feature work should begin without this foundation.

### 0a: GX DuckDB Harness

1. **Spike** (timebox: 1 day): Verify GX 1.6+ SqlAlchemy + DuckDB integration.
   - Create DuckDB datasource via `duckdb:///:memory:`.
   - Load CSV batch, execute 3-5 representative expectations, verify result format.
   - If spike fails: implement Pandas fallback and document semantic differences.
2. Add `duckdb` and `duckdb-engine` to `[duckdb]` optional extra in `pyproject.toml`.
3. Implement `GXTestHarness` class with `run()` method returning `GXTestResult`.
4. Support `stage="raw"` (all VARCHAR) and `stage="ingested"` (typed) loading.

### 0b: UMF Builder DSL

1. Implement `UMFBuilder` in `tests/conftest.py` or `tests/builders.py`.
2. Support `.column()`, `.expectation()`, `.build()`, `.as_dict()`.
3. Verify all builder outputs pass Pydantic validation.
4. Do NOT migrate existing tests -- builder is additive.

### 0c: Test Discovery Conventions

1. Register `@pytest.mark.fast` in `pyproject.toml`.
2. Document source-to-test file mapping convention.
3. Add `pytest -m fast` to Makefile as `make test-fast`.

### 0d: Golden File Runner + Hypothesis Strategies

1. Create `tests/golden/` directory structure.
2. Implement golden file discovery and comparison fixture.
3. Create `tests/strategies.py` with `umf_column()`, `umf_dict()`, `umf_object()`.
4. Verify strategies produce Pydantic-valid outputs.

## Phase 1a: Unified Expectation Model (ADR-005)

1. Define `Expectation`, `ExpectationMeta`, `ExpectationSuite` in `models/umf.py`.
2. Add `expectations` field to UMF model alongside existing `validation_rules` and `quality_checks`.
3. Implement loader logic: populate `ExpectationSuite` from old format on read.
4. Update `classify_validation_type()` to set `stage` on expectations.
5. Write property test: any UMF with old-format rules loads with equivalent expectations in new model.

## Phase 1b: Validation Pipeline (FEAT-017)

1. Implement `GXSuiteExecutor` with batch execution and `execute_staged()`.
2. Fix `BaselineExpectationGenerator` to stop producing redundant types.
3. Implement profiling-to-expectations conversion (the TODO stub in `gx_baseline.py`).
4. Implement `should_block_pipeline()` with severity/blocking/threshold logic.
5. Implement `ValidationReport` class.

## Phase 2: Custom GX Extensions (FEAT-018)

1. Implement `ExpectColumnValuesToMatchDomainType` custom expectation.
2. Implement `ExpectColumnPairDateOrder` custom expectation.
3. Add registration property test.
4. Test all custom expectations via GX harness against DuckDB.

## Phase 3: SQL Generator CTE Mode (FEAT-019)

1. Add `mode` parameter to `SQLPlanGenerator` (`"views"` default, `"cte"` new).
2. Implement CTE generation for linear chains, diamond dependencies, fan-out/fan-in.
3. Semantic equivalence tests: both modes produce same results on DuckDB.
4. Golden file tests for ~15 representative CTE outputs.

## Phase 4: Domain Type Improvements (FEAT-020)

1. Add `COMMON_ABBREVIATIONS` dict and `expand_column_name()`.
2. Replace bare string returns with `InferenceResult` dataclass.
3. Update Excel converter to read from `DomainTypeRegistry`.
4. Add regex validation on registry load.

## Phase 5: UMF Loader Improvements (FEAT-021)

1. Replace generic error messages with targeted diagnostics.
2. Add expectation type validation with known-types registry.
3. Write split format roundtrip property test.

## Phase 6: Authoring Tools (FEAT-023)

**Depends on Phases 1-5.** These tools compose features from earlier phases.

1. Implement pure functions for column and validation mutation.
2. Implement LLM response applier with deduplication and validation.
3. Implement `tablespec preview` with staged display and `--against` dry-run.
4. Implement Textual TUI (can be done incrementally, independent of CLI commands).

## Phase 7: Schema Compatibility Checker (FEAT-022)

**Independent after Phase 0.** Can be parallelized with Phases 1-5.

1. Define `SAFE_WIDENINGS` type lattice.
2. Implement nullable-aware context-by-context comparison.
3. Implement `CompatibilityReport` with `CompatibilityIssue` details.
4. Hypothesis properties: reflexivity, addition safety, removal detection.
5. Golden files for ~15 compatibility scenarios.

## Exit Criteria

Each phase is complete when:

- All tests pass (`make check`).
- New code has 80%+ coverage.
- Property tests run without failures for 1000+ examples.
- Golden files (where applicable) produce exact matches.
- No regressions in existing test suite.

### Phase-Specific Exit Criteria

- **Phase 0a**: GX DuckDB spike passes (3-5 expectations execute correctly against DuckDB) or Pandas fallback is implemented and semantic differences are documented.
- **Phase 1a**: Backward-compatible loading of existing UMF files verified -- all existing YAML files in the repository load without errors and produce equivalent expectation suites.
- **Phase 6**: User-facing acceptance criteria -- CLI commands produce correct output for documented use cases, `tablespec preview --against` validates sample CSV files, and LLM response applier correctly integrates generated expectations.
