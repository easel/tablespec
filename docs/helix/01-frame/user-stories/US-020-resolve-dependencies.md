# US-020: Resolve Pipeline Dependencies

**Parent Feature**: [FEAT-010 - UMF Change Management](../features/FEAT-010-change-management.md)

## User Story

**As a** data engineer working with cross-pipeline table references,
**I want to** validate dependency versions and detect cycles,
**so that** pipeline ordering is correct and version constraints are satisfied.

## Acceptance Criteria

- [ ] `dependency_resolver.py` loads pipeline dependencies from metadata
- [ ] Version constraint validation against packaging specifiers
- [ ] Cycle detection in dependency graph
- [ ] Clear error reporting for unresolved or conflicting dependencies
