# tablespec

Python library for working with table schemas in Universal Metadata Format (UMF). Provides type-safe models, validation, profiling integration, and schema generation tools.

## Table of Contents

- [Features](#features)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [UMF Format](#umf-format)
- [Core Features](#core-features)
  - [UMF Models](#umf-models)
  - [Schema Generation](#schema-generation)
  - [Type Mappings](#type-mappings)
  - [Great Expectations Integration](#great-expectations-integration)
  - [Profiling Integration](#profiling-integration)
  - [LLM Prompt Generation](#llm-prompt-generation)
- [API Reference](#api-reference)
- [Development](#development)

## Features

- **Type-Safe UMF Models**: Pydantic-based models with runtime validation
- **Schema Generation**: Generate SQL DDL, PySpark schemas, and JSON schemas from UMF
- **Great Expectations Integration**: Baseline expectation generation and constraint extraction
- **Profiling Mappers**: Convert Spark DataFrame profiles and Deequ profiles to UMF
- **Validation**: Table validation against UMF specifications with Great Expectations
- **Type Mappings**: Convert between UMF, PySpark, JSON, and Great Expectations types
- **LLM Prompt Generation**: Generate structured prompts for documentation, validation rules, relationships, and survivorship logic

## Installation

### Using uv (recommended)

```bash
# Add to your uv project
uv add tablespec --index-url https://easel.github.io/tablespec/simple/

# With Spark support (for profiling and validation)
uv add tablespec[spark] --index-url https://easel.github.io/tablespec/simple/
```

### Using pip

```bash
# Install from GitHub Pages index
pip install tablespec --index-url https://easel.github.io/tablespec/simple/

# With Spark support
pip install tablespec[spark] --index-url https://easel.github.io/tablespec/simple/
```

**Note**: This package is distributed via GitHub Pages. The `--index-url` flag is required.

## Quick Start

### Loading and Saving UMF Files

```python
from tablespec import load_umf_from_yaml, save_umf_to_yaml, UMF

# Load UMF from YAML
umf = load_umf_from_yaml("schema.yaml")

# Access metadata
print(f"Table: {umf.table_name}")
print(f"Columns: {len(umf.columns)}")

# Modify and save
umf.description = "Updated description"
save_umf_to_yaml(umf, "updated_schema.yaml")
```

### Creating UMF Programmatically

```python
from tablespec import UMF, UMFColumn, Nullable

umf = UMF(
    version="1.0",
    table_name="Medical_Claims",
    description="Healthcare claims data",
    columns=[
        UMFColumn(
            name="claim_id",
            data_type="VARCHAR",
            length=50,
            description="Unique claim identifier",
            nullable=Nullable(MD=False, MP=False, ME=False)
        ),
        UMFColumn(
            name="claim_amount",
            data_type="DECIMAL",
            precision=10,
            scale=2,
            description="Claim amount in USD",
            nullable=Nullable(MD=True, MP=True, ME=True)
        )
    ]
)

save_umf_to_yaml(umf, "medical_claims.yaml")
```

### Generating Schemas

```python
from tablespec import generate_sql_ddl, generate_pyspark_schema, generate_json_schema

# Load UMF
umf = load_umf_from_yaml("schema.yaml")
umf_dict = umf.model_dump()

# Generate SQL DDL
ddl = generate_sql_ddl(umf_dict)
print(ddl)

# Generate PySpark schema code
spark_schema = generate_pyspark_schema(umf_dict)
print(spark_schema)

# Generate JSON Schema
json_schema = generate_json_schema(umf_dict)
print(json_schema)
```

## UMF Format

Universal Metadata Format (UMF) is a YAML-based schema format for describing database tables with rich metadata.

### Structure

```yaml
version: "1.0"
table_name: Medical_Claims
source_file: claims_spec.xlsx
sheet_name: Medical Claims
description: Healthcare claims and billing information
table_type: data_table

columns:
  - name: claim_id
    data_type: VARCHAR
    length: 50
    description: Unique claim identifier
    nullable:
      MD: false  # Medicaid
      MP: false  # Medicare Part D
      ME: false  # Medicare
    sample_values:
      - "CLM001"
      - "CLM002"

  - name: claim_amount
    data_type: DECIMAL
    precision: 10
    scale: 2
    description: Claim amount in USD
    nullable:
      MD: true
      MP: true
      ME: true

validation_rules:
  table_level:
    - rule_type: row_count
      description: Table must not be empty
      severity: error
      parameters:
        min_value: 1

  column_level:
    claim_id:
      - rule_type: uniqueness
        description: claim_id must be unique
        severity: error

relationships:
  foreign_keys:
    - column: provider_id
      references_table: Providers
      references_column: provider_id
      confidence: 0.95

  referenced_by:
    - table: Claim_Lines
      column: claim_id
      foreign_key_column: claim_id

metadata:
  updated_at: 2025-01-15T10:30:00Z
  created_by: data-platform-team
  pipeline_phase: 4
```

### Supported Data Types

- `VARCHAR` - Variable-length string (requires `length`)
- `CHAR` - Fixed-length string
- `TEXT` - Unlimited text
- `INTEGER` - Integer number
- `DECIMAL` - Fixed-precision decimal (supports `precision` and `scale`)
- `FLOAT` - Floating-point number
- `DATE` - Date without time
- `DATETIME` - Date with time
- `BOOLEAN` - True/false value

## Core Features

### UMF Models

Type-safe Pydantic models with validation:

```python
from tablespec import UMF, UMFColumn, ValidationRules, ValidationRule

# Models enforce constraints at runtime
umf = UMF(
    version="1.0",
    table_name="Valid_Name",  # Validates naming convention
    columns=[
        UMFColumn(
            name="column1",
            data_type="VARCHAR",
            length=100  # Required for VARCHAR
        )
    ]
)

# Add validation rules
validation_rule = ValidationRule(
    rule_type="uniqueness",
    description="Column must be unique",
    severity="error"
)
```

### Schema Generation

Generate schemas in multiple formats:

```python
from tablespec import generate_sql_ddl, generate_pyspark_schema, generate_json_schema

umf_dict = umf.model_dump()

# SQL DDL for Spark SQL / Databricks
ddl = generate_sql_ddl(umf_dict)
# Output: CREATE TABLE Medical_Claims (claim_id VARCHAR(50) NOT NULL, ...)

# PySpark StructType code
pyspark = generate_pyspark_schema(umf_dict)
# Output: StructType([StructField("claim_id", StringType(), False), ...])

# JSON Schema for validation
json_schema = generate_json_schema(umf_dict)
# Output: {"type": "object", "properties": {...}, "required": [...]}
```

### Type Mappings

Convert between type systems:

```python
from tablespec import map_to_pyspark_type, map_to_json_type, map_to_gx_spark_type

# UMF to PySpark
pyspark_type = map_to_pyspark_type("VARCHAR", length=100)
# Returns: StringType()

# UMF to JSON Schema
json_type = map_to_json_type("DECIMAL", precision=10, scale=2)
# Returns: "number"

# UMF to Great Expectations Spark type
gx_type = map_to_gx_spark_type("INTEGER")
# Returns: "IntegerType"
```

### Great Expectations Integration

#### Baseline Expectation Generation

Generate deterministic expectations from UMF metadata:

```python
from tablespec import BaselineExpectationGenerator, load_umf_from_yaml

# Load UMF
umf = load_umf_from_yaml("schema.yaml")
umf_dict = umf.model_dump()

# Generate baseline expectations
generator = BaselineExpectationGenerator()
expectations = generator.generate_baseline_expectations(
    umf_dict,
    include_structural=True
)

# Expectations include:
# - Column existence
# - Column types
# - Nullability
# - Length constraints
# - Column count and order
```

#### Constraint Extraction

Extract existing Great Expectations suite into UMF format:

```python
from tablespec import GXConstraintExtractor

extractor = GXConstraintExtractor()

# Extract from GX checkpoint JSON
validation_rules = extractor.extract_from_checkpoint(
    checkpoint_path="checkpoints/my_checkpoint.json"
)

# Add to UMF
umf.validation_rules = validation_rules
```

#### UMF to Great Expectations Mapping

Map UMF models to GX format:

```python
from tablespec import UmfToGxMapper

mapper = UmfToGxMapper()

# Convert column definitions
gx_columns = mapper.map_columns(umf.columns)

# Convert validation rules
gx_expectations = mapper.map_validation_rules(umf.validation_rules)
```

### Profiling Integration

Convert profiling results to UMF format:

#### Spark DataFrame Profiling

```python
from tablespec import SparkToUmfMapper  # Requires tablespec[spark]
from pyspark.sql import DataFrame

# Profile Spark DataFrame
mapper = SparkToUmfMapper()
umf = mapper.create_umf_from_dataframe(
    df=spark_df,
    table_name="Medical_Claims",
    source_file="claims.parquet"
)

# UMF includes inferred types, nullability, and sample values
save_umf_to_yaml(umf, "medical_claims.yaml")
```

#### Deequ Profiling

```python
from tablespec import DeequToUmfMapper

# Convert Deequ profile to UMF
mapper = DeequToUmfMapper()
umf = mapper.create_umf_from_profile(
    profile_json="deequ_profile.json",
    table_name="Medical_Claims"
)
```

### LLM Prompt Generation

Generate structured prompts for LLM-based enrichment:

```python
from tablespec import (
    _generate_documentation_prompt,
    _generate_validation_prompt,
    _generate_relationship_prompt,
    _generate_survivorship_prompt
)

umf_dict = umf.model_dump()

# Generate documentation prompt
doc_prompt = _generate_documentation_prompt(umf_dict)
# Asks LLM to enhance table and column descriptions

# Generate validation rules prompt
validation_prompt = _generate_validation_prompt(umf_dict)
# Asks LLM to suggest validation rules (uniqueness, ranges, formats)

# Generate relationship prompt
relationship_prompt = _generate_relationship_prompt(
    umf_dict,
    all_tables=["Medical_Claims", "Providers", "Members"]
)
# Asks LLM to identify foreign key relationships

# Generate survivorship prompt
survivorship_prompt = _generate_survivorship_prompt(umf_dict)
# Asks LLM to suggest survivorship/merge logic for deduplication
```

## API Reference

### Core Models

- `UMF` - Main UMF model
- `UMFColumn` - Column definition
- `UMFMetadata` - Table metadata
- `Nullable` - Nullable configuration per LOB
- `ValidationRule` - Individual validation rule
- `ValidationRules` - Table and column-level rules
- `ForeignKey` - Foreign key relationship
- `ReferencedBy` - Reverse foreign key
- `Relationships` - All table relationships
- `Index` - Database index definition

### Functions

#### I/O
- `load_umf_from_yaml(path)` - Load UMF from YAML file
- `save_umf_to_yaml(umf, path)` - Save UMF to YAML file

#### Schema Generation
- `generate_sql_ddl(umf_dict)` - Generate SQL DDL
- `generate_pyspark_schema(umf_dict)` - Generate PySpark schema code
- `generate_json_schema(umf_dict)` - Generate JSON Schema

#### Type Mappings
- `map_to_pyspark_type(data_type, **kwargs)` - UMF to PySpark type
- `map_to_json_type(data_type, **kwargs)` - UMF to JSON type
- `map_to_gx_spark_type(data_type)` - UMF to GX Spark type

#### Great Expectations
- `BaselineExpectationGenerator` - Generate baseline expectations
- `UmfToGxMapper` - Map UMF to Great Expectations format
- `GXConstraintExtractor` - Extract constraints from GX suites
- `GXExpectationProcessor` - Process expectations (requires Spark)
- `TableValidator` - Validate tables against UMF (requires Spark)

#### Profiling
- `SparkToUmfMapper` - Convert Spark DataFrame to UMF (requires Spark)
- `DeequToUmfMapper` - Convert Deequ profile to UMF
- `DataFrameProfile` - DataFrame profiling result
- `ColumnProfile` - Column profiling result

#### Prompt Generation
- `_generate_documentation_prompt(umf_dict)` - Documentation enrichment
- `_generate_validation_prompt(umf_dict)` - Validation rule generation
- `_generate_column_validation_prompt(umf_dict, column_name)` - Column-specific validation
- `_generate_relationship_prompt(umf_dict, all_tables)` - Relationship detection
- `_generate_survivorship_prompt(umf_dict)` - Survivorship logic

## Development

### Setup

```bash
# Clone repository
git clone <repository-url>
cd tablespec

# Install with development dependencies
uv sync --all-extras

# Install with Spark support
uv sync --extra spark
```

### Running Tests

```bash
# Run all tests
uv run pytest

# Run with coverage
uv run pytest --cov=src/tablespec --cov-report=html

# Run specific test file
uv run pytest tests/unit/test_gx_baseline.py
```

### Project Structure

```
src/tablespec/
├── __init__.py              # Public API
├── models/
│   └── umf.py              # Pydantic UMF models
├── schemas/
│   └── generators.py       # Schema generation (SQL, PySpark, JSON)
├── type_mappings.py        # Type system conversions
├── gx_baseline.py          # GX baseline expectation generation
├── gx_constraint_extractor.py  # Extract constraints from GX
├── gx_schema_validator.py  # Schema validation with GX
├── profiling/
│   ├── types.py            # Profiling result types
│   ├── spark_mapper.py     # Spark to UMF mapper
│   └── deequ_mapper.py     # Deequ to UMF mapper
├── prompts/
│   ├── documentation.py    # Documentation prompts
│   ├── validation.py       # Validation rule prompts
│   ├── relationship.py     # Relationship detection prompts
│   └── survivorship.py     # Survivorship logic prompts
└── validation/
    ├── gx_processor.py     # GX expectation processing
    └── table_validator.py  # Table validation engine
```

## License

Apache License 2.0 - see LICENSE file for details.
