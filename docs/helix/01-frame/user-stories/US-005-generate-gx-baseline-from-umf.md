# US-005: Generate a Great Expectations Baseline from UMF

**Parent Feature**: [FEAT-004 - Great Expectations Integration](../features/FEAT-004-gx-integration.md)

## User Story

**As a** data quality engineer setting up validation for a new table,
**I want to** generate a baseline Great Expectations suite directly from a UMF schema,
**so that** I get deterministic structural and type expectations (column existence, order, types, nullability, lengths) without writing expectations by hand.

## Acceptance Criteria

- [ ] `BaselineExpectationGenerator` produces structural expectations: column count and column order
- [ ] Per-column expectations include: column existence, type matching, LOB-specific nullability, length constraints, and date format checks
- [ ] `UmfToGxMapper` composes a complete suite by merging baseline, profiling, and AI-generated expectations
- [ ] `GXExpectationProcessor` merges and deduplicates expectations using type:column signatures
- [ ] Output conforms to GX 1.6+ format (legacy fields are rejected)
