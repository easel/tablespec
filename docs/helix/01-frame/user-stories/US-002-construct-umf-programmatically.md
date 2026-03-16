# US-002: Construct a UMF Schema Programmatically

**Parent Feature**: [FEAT-001 - UMF Models and I/O](../features/FEAT-001-umf-models.md)

## User Story

**As a** platform engineer managing schema standards,
**I want to** construct UMF schemas programmatically using type-safe Python models,
**so that** I can generate and manage table specifications across Medicaid, Medicare Part D, and Medicare lines of business in automated workflows without hand-editing YAML.

## Acceptance Criteria

- [ ] UMF, UMFColumn, Nullable, ValidationRules, Relationships, and UMFMetadata models can be instantiated with keyword arguments and compose into a complete schema
- [ ] Pydantic validation fires on construction, catching invalid column names, unsupported data types, and missing required fields immediately
- [ ] ForeignKey confidence scoring and legacy format support work when building relationships programmatically
- [ ] The constructed UMF object can be serialized to YAML via `save_umf_to_yaml` for storage or distribution
