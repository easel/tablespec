# FEAT-010: UMF Change Management

**Status**: Implemented
**Priority**: High

## Description

Split-format UMF storage, schema diffing, atomic change application, and git-based changelog generation.

## Components

### UMF Loader (`umf_loader.py`)
- `UMFLoader` - Load UMF from split (directory) or JSON format with auto-detection
- `UMFFormat` enum: SPLIT (default, git-friendly) and JSON (artifact standard)
- Bidirectional conversion between formats

### UMF Diff (`umf_diff.py`)
- `UMFDiff` - Compare two UMF versions
- Detects: column added/removed/modified, validation rule changes, metadata changes, relationship changes
- Change types: `UMFColumnChange`, `UMFMetadataChange`, `UMFValidationChange`

### Change Applier (`umf_change_applier.py`)
- `apply_column_change()`, `apply_metadata_change()`, `apply_validation_change()`
- Returns modified deep copies for immutable change tracking

### Changelog Generator (`changelog_generator.py`)
- `ChangelogGenerator` - Git history-based changelog for table directories
- `YAMLDiffParser` - Detailed YAML diff parsing from git commits
- Structured output via `ChangeEntry` and `ChangeDetail` models

## Dependencies

- ruamel.yaml (split-format YAML)
- gitpython (changelog generation)

## Source

- `src/tablespec/umf_loader.py`
- `src/tablespec/umf_diff.py`
- `src/tablespec/umf_change_applier.py`
- `src/tablespec/changelog_generator.py`
- `src/tablespec/changelog_diff_parser.py`
- `src/tablespec/changelog_formatter.py`
- `src/tablespec/models/changelog.py`
