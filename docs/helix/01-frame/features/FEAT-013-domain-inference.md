# FEAT-013: Domain Type Inference

**Status**: Implemented
**Priority**: Medium

## Description

Automatic detection of domain types (e.g., us_state_code, email, phone_number) from column names, descriptions, and sample values. Used in spec generation to tag columns for downstream validation and sample data.

## Components

### Domain Type Registry (`inference/domain_types.py`)
- `DomainTypeRegistry` - YAML-driven registry of domain types with patterns and validation rules
- Default registry path relative to module
- Lookup by name and pattern matching

### Domain Type Inference (`inference/domain_types.py`)
- `DomainTypeInference` - Infer domain types from column metadata
- Name-based matching (regex patterns on column names)
- Description-based matching
- Sample value validation

## Source

- `src/tablespec/inference/domain_types.py`
