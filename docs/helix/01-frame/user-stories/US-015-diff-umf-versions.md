# US-015: Diff Two UMF Versions

**Parent Feature**: [FEAT-010 - UMF Change Management](../features/FEAT-010-change-management.md)

## User Story

**As a** data engineer reviewing schema changes,
**I want to** compare two UMF versions and see a structured list of differences,
**so that** I can understand what changed before approving a pull request.

## Acceptance Criteria

- [ ] `UMFDiff` detects added, removed, and modified columns
- [ ] Validation rule and metadata changes are identified separately
- [ ] `UMFChangeApplier` can apply individual changes to produce intermediate UMF versions
- [ ] Changes are typed (`UMFColumnChange`, `UMFMetadataChange`, `UMFValidationChange`)
