# US-014: Generate Changelog from Git History

**Parent Feature**: [FEAT-010 - UMF Change Management](../features/FEAT-010-change-management.md)

## User Story

**As a** data governance lead,
**I want to** generate a changelog of schema changes from git history,
**so that** I can track who changed what and when for audit and compliance purposes.

## Acceptance Criteria

- [ ] `ChangelogGenerator` produces structured entries from git commits in a table directory
- [ ] Each entry includes timestamp, author, change type, and affected components
- [ ] YAML diff parsing detects column, validation, metadata, and relationship changes
- [ ] `tablespec changelog` CLI command outputs formatted changelog
