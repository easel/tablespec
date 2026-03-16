# US-013: Generate Sample Data from UMF

**Parent Feature**: [FEAT-011 - Sample Data Generation](../features/FEAT-011-sample-data.md)

## User Story

**As a** QA engineer setting up test environments,
**I want to** generate realistic sample data from UMF specifications,
**so that** I can test pipelines with data that respects types, constraints, and foreign key relationships.

## Acceptance Criteria

- [ ] Sample data engine generates rows matching UMF column types and constraints
- [ ] Foreign key relationships produce referentially consistent data across tables
- [ ] Healthcare domain types (SSN, NPI, state codes) generate realistic values
- [ ] Output available in CSV and JSON formats with configurable row counts
