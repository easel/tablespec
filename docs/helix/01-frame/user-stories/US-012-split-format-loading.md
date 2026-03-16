# US-012: Load UMF from Split-Format Directory

**Parent Feature**: [FEAT-010 - UMF Change Management](../features/FEAT-010-change-management.md)

## User Story

**As a** data engineer using git for schema version control,
**I want to** store UMF specs as a directory of YAML files (one per column) and load them transparently,
**so that** git diffs show per-column changes and merge conflicts are isolated.

## Acceptance Criteria

- [ ] `UMFLoader` auto-detects split format from a directory containing `table.yaml` + `columns/`
- [ ] `UMFLoader` auto-detects JSON format from a `.json` file
- [ ] Loading from either format produces the same `UMF` object
- [ ] `UMFLoader` converts between formats bidirectionally
