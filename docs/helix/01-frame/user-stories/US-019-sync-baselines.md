# US-019: Sync Baseline Validations Across Tables

**Parent Feature**: [FEAT-012 - Quality Baselines](../features/FEAT-012-quality-baselines.md)

## User Story

**As a** platform engineer maintaining table standards,
**I want to** sync metadata columns and baseline validations across all table definitions,
**so that** every table has required metadata columns and up-to-date programmatic validations.

## Acceptance Criteria

- [ ] `sync_baseline.py` ensures all tables have required metadata columns
- [ ] Baseline validations stay in sync with the baseline generator
- [ ] User customizations (severity changes) are preserved
- [ ] Conflicts (modified rule content) are detected and reported
- [ ] Operation is idempotent
