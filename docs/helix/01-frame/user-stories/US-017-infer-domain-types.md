# US-017: Infer Domain Types for Columns

**Parent Feature**: [FEAT-013 - Domain Type Inference](../features/FEAT-013-domain-inference.md)

## User Story

**As a** data engineer building table specs,
**I want to** automatically detect domain types (state code, SSN, phone) from column names and descriptions,
**so that** I can enrich UMF specs with semantic types without manual tagging.

## Acceptance Criteria

- [ ] `DomainTypeInference` infers domain types from column name patterns
- [ ] `DomainTypeRegistry` loads domain definitions from YAML
- [ ] Inferred types integrate with sample data generation and validation
- [ ] Unknown columns return no domain type rather than a false match
