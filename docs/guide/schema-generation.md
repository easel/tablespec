# Schema Generation

tablespec provides type-safe UMF models, schema generators for multiple output formats, and type mappings between systems.

## UMF Models

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

## Schema Generation

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

## Type Mappings

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
