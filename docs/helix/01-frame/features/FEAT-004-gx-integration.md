# FEAT-004: Great Expectations Integration

**Status**: Implemented
**Priority**: High

## Description

Bidirectional integration with Great Expectations: generate baseline expectations from UMF, extract constraints from existing suites, validate schemas, and process expectation suites.

## Components

### Baseline Generation (`gx_baseline.py`)
- `BaselineExpectationGenerator` - Deterministic expectations from UMF metadata
- `UmfToGxMapper` - Compose expectation suites (baseline + profiling + AI-generated)
- Structural expectations: column count, column order
- Column expectations: existence, types, nullability, length, date format

### Constraint Extraction (`gx_constraint_extractor.py`)
- `GXConstraintExtractor` - Reverse-engineer UMF constraints from GX suites
- Extract: value sets, regex patterns, strftime formats, metadata hints
- Generate sample values from regex patterns

### Schema Validation (`gx_schema_validator.py`)
- `GXSchemaValidator` - Validate expectation types against GX library
- Validate against JSON schema and GX library instantiation
- Generate corrected schemas with only valid types

### Expectation Processing (`validation/gx_processor.py`)
- `GXExpectationProcessor` - Process AI-generated GX suites
- Merge baseline expectations with AI-generated
- Deduplicate using type:column signatures
- Validate GX 1.6+ format (reject legacy fields)

## Source

- `src/tablespec/gx_baseline.py`
- `src/tablespec/gx_constraint_extractor.py`
- `src/tablespec/gx_schema_validator.py`
- `src/tablespec/validation/gx_processor.py`
