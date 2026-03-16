# US-018: Merge Table Files with Survivorship

**Parent Feature**: [FEAT-007 - Table Validation](../features/FEAT-007-validation.md)

## User Story

**As a** data engineer merging vendor files,
**I want to** merge multiple table files using UMF survivorship rules,
**so that** deduplication and conflict resolution follow the spec rather than ad-hoc logic.

## Acceptance Criteria

- [ ] `merge.py` merges multiple Spark DataFrames using UMF metadata (requires `tablespec[spark]`)
- [ ] Survivorship rules from UMF drive conflict resolution
- [ ] Configurable deduplication strategy
