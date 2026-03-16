# US-006: Extract UMF Constraints from an Existing GX Suite

**Parent Feature**: [FEAT-004 - Great Expectations Integration](../features/FEAT-004-gx-integration.md)

## User Story

**As a** data quality engineer with existing Great Expectations suites,
**I want to** extract validation constraints from those suites back into UMF format,
**so that** I can consolidate tribal knowledge already captured in GX into the canonical UMF schema and avoid maintaining rules in two places.

## Acceptance Criteria

- [ ] `GXConstraintExtractor` parses an existing GX suite and extracts value sets, regex patterns, strftime format strings, and metadata hints into UMF `ValidationRules`
- [ ] Extracted constraints can be merged into an existing UMF schema
- [ ] Sample values are generated from extracted regex patterns for documentation purposes
- [ ] `GXSchemaValidator` validates expectation types against the GX library and produces corrected schemas containing only valid types
