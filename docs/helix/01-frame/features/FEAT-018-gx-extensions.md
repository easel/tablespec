# FEAT-018: Custom GX Extensions

**Status**: Proposed
**Priority**: High

## Description

Custom Great Expectations expectation classes that bridge tablespec domain concepts into GX execution.

## Components

### ExpectColumnValuesToMatchDomainType (`src/tablespec/validation/custom_gx_expectations.py`)

Loads the domain type registry (`src/tablespec/domain_types.yaml`), validates that column values match the validation spec for the assigned domain type (regex patterns, value sets, format constraints).

Works on both Pandas and Spark execution engines. Bridges domain types from FEAT-013 into the GX validation pipeline.

```python
# Usage in expectation suite
{
    "type": "expect_column_values_to_match_domain_type",
    "kwargs": {"column": "gender_cd", "domain_type": "gender_code"}
}
```

### Cross-Column Date Ordering (`src/tablespec/validation/custom_gx_expectations.py`)

`ExpectColumnPairDateOrder` for start_date < end_date patterns common in healthcare data (eligibility spans, enrollment periods, claim dates).

Wraps GX's `expect_column_pair_values_a_to_be_greater_than_b` with date parsing semantics, supporting UMF date formats.

### Registration Testing (`tests/unit/test_custom_gx_expectations.py`)

Property test: every custom expectation class defined in `custom_gx_expectations.py` is registered with GX and executable against the test harness. Prevents silent registration failures.

## Source

- `src/tablespec/validation/custom_gx_expectations.py` (existing, to be extended)
- `src/tablespec/inference/domain_types.py` (existing registry)

## Dependencies

- FEAT-016 (GX test harness for testing custom expectations)
- FEAT-013 (domain type registry)
