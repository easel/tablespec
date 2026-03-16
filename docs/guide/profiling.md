# Profiling Integration

tablespec converts profiling results from Spark DataFrames and Deequ into UMF format.

## Spark DataFrame Profiling

```python
from tablespec import SparkToUmfMapper  # Requires tablespec[spark]
from tablespec import save_umf_to_yaml
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

## Deequ Profiling

```python
from tablespec import DeequToUmfMapper

# Convert Deequ profile to UMF
mapper = DeequToUmfMapper()
umf = mapper.create_umf_from_profile(
    profile_json="deequ_profile.json",
    table_name="Medical_Claims"
)
```
