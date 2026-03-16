# Great Expectations Integration

tablespec integrates with Great Expectations for baseline expectation generation, constraint extraction, and UMF-to-GX mapping.

## Baseline Expectation Generation

Generate deterministic expectations from UMF metadata:

```python
from tablespec import BaselineExpectationGenerator, load_umf_from_yaml

# Load UMF
umf = load_umf_from_yaml("examples/schema.yaml")
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

## Constraint Extraction

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

## UMF to Great Expectations Mapping

Map UMF models to GX format:

```python
from tablespec import UmfToGxMapper

mapper = UmfToGxMapper()

# Convert column definitions
gx_columns = mapper.map_columns(umf.columns)

# Convert validation rules
gx_expectations = mapper.map_validation_rules(umf.validation_rules)
```
