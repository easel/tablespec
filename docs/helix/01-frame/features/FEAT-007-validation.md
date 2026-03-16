# FEAT-007: Table Validation

**Status**: Implemented
**Priority**: Medium

## Description

Validate Spark DataFrames against UMF specifications and validate UMF files against JSON schema.

## Components

### Table Validator (`validation/table_validator.py`) [requires PySpark]
- `TableValidator` - Validate DataFrame against UMF
- Schema validation (missing/extra columns)
- Data type validation
- LOB-specific nullable validation
- Business rule validation (uniqueness, format, value constraints)
- Structured error output via `VALIDATION_ERROR_SCHEMA`

### UMF Validator (`umf_validator.py`)
- `UMFValidator` - Validate UMF files against JSON schema + business rules
- File, data, and directory validation
- Default specification application (VARCHAR length 255, DECIMAL precision 18/scale 2)
- Duplicate column name fixing

### Completeness Validator (`completeness_validator.py`)
- Validate completeness of UMF specifications against expected fields

### Relationship Validator (`relationship_validator.py`)
- Validate foreign key relationships and referential integrity definitions

### Naming Validator (`naming_validator.py`)
- Validate column and table names against naming conventions

## Source

- `src/tablespec/validation/table_validator.py`
- `src/tablespec/umf_validator.py`
- `src/tablespec/completeness_validator.py`
- `src/tablespec/relationship_validator.py`
- `src/tablespec/naming_validator.py`
