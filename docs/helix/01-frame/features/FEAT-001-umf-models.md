# FEAT-001: UMF Models and I/O

**Status**: Implemented
**Priority**: Critical

## Description

Type-safe Pydantic models for the Universal Metadata Format (UMF), plus YAML serialization and deserialization.

## Models

- **UMF** - Root model: version, table_name, columns, validation_rules, relationships, metadata
- **UMFColumn** - Column definition: name, data_type, length, precision, scale, nullable, sample_values
- **Nullable** - Per-LOB nullability: MD (Medicaid), MP (Medicare Part D), ME (Medicare)
- **ValidationRule** - Rule: rule_type, description, severity (error/warning/info), parameters
- **ValidationRules** - Table-level and column-level rule collections
- **ForeignKey** - FK with confidence scoring and legacy format support
- **ReferencedBy** - Reverse FK reference
- **Index** - Database index definition
- **Relationships** - FK, referenced_by, and index collections
- **UMFMetadata** - Timestamps, creator, pipeline phase (1-7)

## Key Behaviors

- Column names validated: `^[A-Za-z][A-Za-z0-9_]*$`, max 128 chars
- Unique column names enforced
- VARCHAR requires length; DECIMAL recommends precision
- Extra fields forbidden (`extra="forbid"`)
- Version format: `\d+\.\d+`

## I/O

- `load_umf_from_yaml(path)` - Load and validate UMF from YAML
- `save_umf_to_yaml(umf, path)` - Save UMF to YAML, excluding None values
- `UMFLoader` - Auto-detect and load from split or JSON format (see FEAT-010)

## Changelog Models

- **ChangeEntry** - Structured changelog entry with timestamp, author, change type
- **ChangeDetail** - Per-field change detail
- **ChangeType** - Enum of change categories

## Source

- `src/tablespec/models/umf.py`
- `src/tablespec/models/changelog.py`
