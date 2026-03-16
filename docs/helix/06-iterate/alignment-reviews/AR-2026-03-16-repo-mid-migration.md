# Alignment Review: tablespec (Mid-Migration)

**Review Date**: 2026-03-16
**Scope**: repo
**Status**: complete
**Review Epic**: tablespec-9d2
**Review Beads**: tablespec-9d2.1, tablespec-9d2.2
**Primary Governing Artifact**: docs/helix/01-frame/prd.md

## Review Metadata

- **Reviewer**: Claude (AI-assisted review)
- **Run Trigger**: Mid-migration from pulseflow fork; 16 of 21 migration beads closed. Planning docs written for pre-merge codebase.
- **Authority Baseline**: Product Vision -> PRD -> Feature Specs / User Stories -> Architecture / ADRs -> Solution / Technical Designs -> Test Plans / Tests -> Implementation Plans -> Source Code
- **Upstream Beads References**: tablespec-9d2 (review epic), tablespec-58r (migration epic)

## Scope and Governing Artifacts

### Scope Definition

- Full repository: all HELIX planning docs vs current implementation state

### Governing Artifacts

- `docs/helix/00-discover/product-vision.md`
- `docs/helix/01-frame/prd.md`
- `docs/helix/01-frame/features/FEAT-001 through FEAT-007`
- `docs/helix/01-frame/user-stories/US-001 through US-009`
- `docs/helix/02-design/architecture.md`
- `docs/helix/02-design/adr/ADR-001 through ADR-003`
- `docs/helix/03-test/test-plan.md`
- `docs/helix/04-build/implementation-plan.md`
- `docs/helix/05-deploy/deployment-checklist.md`

## Intent Summary

### Product Vision

tablespec provides a single, portable, type-safe Python library making UMF the canonical source of truth for table schemas across data platform tooling.

### Product Requirements

7 functional requirement groups (FR-1 through FR-7): UMF models, schema generation, type mappings, GX integration, profiling, LLM prompts, validation.

### Feature Specs / User Stories

7 feature specs covering original modules. 9 user stories for API consumers covering load/save, programmatic creation, schema generation, type conversion, GX baselines, constraint extraction, profiling, LLM prompts, and DataFrame validation.

### Architecture / ADRs

Layered architecture with UMF models at core, type mappings as hub, schema generators, GX integration, profiling, prompts, and validation as independent modules. 3 ADRs: DATE-as-YYYYMMDD, GX 1.6+ only, optional PySpark.

## Planning Stack Findings

| Finding | Type | Evidence | Impact | Owning Review Bead |
|---------|------|----------|--------|--------------------|
| PRD lists 7 FR groups; implementation has 19+ | underspecified | prd.md vs 70+ source files | HIGH - 12 functional areas undocumented | tablespec-9d2.1 |
| PRD says "metadata-only, no data processing" | contradiction | prd.md "Out of Scope" vs merge.py, casting_utils.py | MEDIUM - scope statement contradicts implementation | tablespec-9d2.2 |
| Architecture shows ~6 module groups; now ~10 | stale | architecture.md vs src/tablespec/ | HIGH - 4 new packages, 30+ new modules | tablespec-9d2.1 |
| Feature specs cover 7 features; ~12 more needed | underspecified | FEAT-001-007 vs new modules | HIGH - major API surfaces undocumented | tablespec-9d2.1 |
| Test plan lists 8 test files; ~30 new modules untested | underspecified | test-plan.md vs src/ | HIGH - test coverage not tracked | tablespec-9d2.1 |
| User stories cover 9 API surfaces; ~11 more needed | underspecified | US-001-009 vs new public API | MEDIUM - API consumers not represented | tablespec-9d2.1 |
| Dependencies grew from 3 to 8+; not reflected | stale | pyproject.toml vs architecture.md | LOW - build config accurate, docs not | tablespec-9d2.1 |

## Implementation Map

### Topology (Post-Migration)

```
src/tablespec/           # ~70 .py files across 8 packages
  models/                # umf.py, changelog.py, __init__.py
  schemas/               # generators.py, *.json schemas
  prompts/               # 8 prompt modules + utils
  profiling/             # types, spark_mapper, deequ_mapper
  validation/            # gx_processor, table_validator, custom_gx_expectations
  formatting/            # YAML formatting (NEW)
  inference/             # Domain type inference (NEW)
  quality/               # Baseline capture/comparison (NEW)
  sample_data/           # Healthcare sample data generation (NEW)
  + 30 top-level modules # cli, naming, excel, changelog, merge, etc. (NEW)
```

### Entry Points and Interfaces

- Python API via `__init__.py` exports
- CLI via `tablespec` command (typer, `cli.py`)
- Excel round-trip via `excel_converter.py`

### Unplanned or Orphaned Areas

- `dependency_resolver.py` references `models.pipeline.PipelineMetadata` which doesn't exist (pulseflow-specific model not ported; module has conditional guard)
- `merge.py` references `ingestion.constants` and `ingestion.raw_ingester` which don't exist (not ported; conditional guards in place)

## Gap Register

| Area | Classification | Planning Evidence | Implementation Evidence | Resolution Direction | Review Bead |
|------|----------------|-------------------|------------------------|---------------------|-------------|
| UMF Models (FEAT-001) | INCOMPLETE | FEAT-001 lists ~10 models | models/umf.py has ~27 models | plan-to-code | tablespec-9d2.1 |
| Schema Generation (FEAT-002) | ALIGNED | FEAT-002 | schemas/generators.py | - | - |
| Type Mappings (FEAT-003) | INCOMPLETE | FEAT-003 lists 3 functions | type_mappings.py has 5 functions + VALID_PYSPARK_TYPES | plan-to-code | tablespec-9d2.1 |
| GX Integration (FEAT-004) | INCOMPLETE | FEAT-004 lists 4 components | +gx_wrapper, custom_expectations, sync_baseline | plan-to-code | tablespec-9d2.1 |
| Profiling (FEAT-005) | ALIGNED | FEAT-005 | profiling/ unchanged | - | - |
| LLM Prompts (FEAT-006) | INCOMPLETE | FEAT-006 uses _prefixed names | Renamed to public API + new modules | plan-to-code | tablespec-9d2.1 |
| Validation (FEAT-007) | INCOMPLETE | FEAT-007 lists 2 components | +validator, completeness, relationship, naming validators | plan-to-code | tablespec-9d2.1 |
| CLI | UNDERSPECIFIED | Not in planning docs | cli.py fully implemented | plan-to-code | tablespec-9d2.1 |
| Excel Conversion | UNDERSPECIFIED | Not in planning docs | excel_converter.py, excel_import_git.py | plan-to-code | tablespec-9d2.1 |
| UMF Tooling | UNDERSPECIFIED | Not in planning docs | umf_loader, umf_diff, umf_change_applier | plan-to-code | tablespec-9d2.1 |
| Changelog | UNDERSPECIFIED | Not in planning docs | 4 changelog modules | plan-to-code | tablespec-9d2.1 |
| Sample Data | UNDERSPECIFIED | Not in planning docs | sample_data/ (8 modules) | plan-to-code | tablespec-9d2.1 |
| Quality Baselines | UNDERSPECIFIED | Not in planning docs | quality/ (4 modules) | plan-to-code | tablespec-9d2.1 |
| Domain Inference | UNDERSPECIFIED | Not in planning docs | inference/ package | plan-to-code | tablespec-9d2.1 |
| Naming Utilities | UNDERSPECIFIED | Not in planning docs | naming.py, naming_validator.py | plan-to-code | tablespec-9d2.1 |
| Date/Format System | UNDERSPECIFIED | Not in planning docs | date_formats.py, format_utils.py, casting_utils.py | plan-to-code | tablespec-9d2.1 |
| Table Merge | UNDERSPECIFIED | Not in planning docs | merge.py | plan-to-code | tablespec-9d2.1 |
| PRD Scope | DIVERGENT | "metadata-only, no data processing" | merge.py, casting_utils.py do Spark ops | decision-needed | tablespec-9d2.2 |

## Execution Beads Generated

The existing bead `tablespec-58r.21` (Update HELIX docs for merged changes) covers the plan-to-code resolution for all INCOMPLETE and UNDERSPECIFIED gaps. It is blocked on `tablespec-58r.20` (tests).

| Bead ID | Type | HELIX Labels | Goal | Dependencies |
|---------|------|-------------|------|-------------|
| tablespec-58r.21 | task | phase:build, kind:build, area:docs | Update all HELIX docs for merged changes | tablespec-58r.20 |
| tablespec-9d2.2 | task | phase:review, kind:review, area:prd | Resolve PRD scope contradiction | None |

## Execution Order

### Critical Path

1. Complete remaining migration beads (58r.5, 58r.11, 58r.16 - in progress)
2. `tablespec-58r.20` - Migrate and update tests
3. `tablespec-58r.21` - Update HELIX docs (resolves all INCOMPLETE/UNDERSPECIFIED gaps)
4. `tablespec-9d2.2` - Resolve PRD scope decision

### Parallelizable Work

- `tablespec-9d2.2` (PRD scope decision) can proceed now, independent of migration

### First Recommended Execution Set

- Complete in-flight beads: 58r.5, 58r.11, 58r.16
- Then: 58r.20 (tests), 58r.21 (docs update)

## Open Decisions

| Decision | Why It Is Open | Governing Artifacts | Recommended Owner |
|----------|---------------|---------------------|-------------------|
| Is tablespec still "metadata-only"? | merge.py and casting_utils.py perform Spark data operations, contradicting PRD scope | prd.md "Out of Scope" | Product owner |
| Should dependency_resolver.py reference PipelineMetadata? | References a model not ported from pulseflow; may be pipeline-specific leakage | dependency_resolver.py | Tech lead |
| Should merge.py reference ingestion.constants? | References modules not ported; may need standalone constants | merge.py | Tech lead |
