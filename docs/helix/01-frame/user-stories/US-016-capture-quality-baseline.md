# US-016: Capture and Compare Quality Baselines

**Parent Feature**: [FEAT-012 - Quality Baselines](../features/FEAT-012-quality-baselines.md)

## User Story

**As a** data quality engineer monitoring pipeline health,
**I want to** capture a quality baseline from a DataFrame and compare it to previous runs,
**so that** I can detect data drift in row counts, distributions, and statistics.

## Acceptance Criteria

- [ ] `BaselineService.capture()` records row counts, column distributions, and numeric stats (requires `tablespec[spark]`)
- [ ] `BaselineService.compare()` produces drift metrics between two baselines
- [ ] Distribution drift uses Jensen-Shannon divergence
- [ ] Baselines are stored and retrievable via `BaselineWriter`
