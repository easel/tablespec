# US-010: Convert UMF Formats via CLI

**Parent Feature**: [FEAT-008 - CLI Interface](../features/FEAT-008-cli.md)

## User Story

**As a** data engineer managing table specs,
**I want to** convert UMF between JSON, split, and Excel formats from the command line,
**so that** I can work with specs in the format best suited to each workflow (git for split, artifact for JSON, review for Excel).

## Acceptance Criteria

- [ ] `tablespec convert input.json output/` converts JSON to split-format directory
- [ ] `tablespec convert tables/my_table/ output.json` converts split to JSON
- [ ] `tablespec batch-convert tables/ output/ --format json` batch-converts a directory
- [ ] Format is auto-detected from input path (file vs directory)
- [ ] Errors are displayed with Rich formatting and clear messages
