# Domain Type Inference

Automatic detection of semantic domain types from column names, descriptions, and sample values. Uses a YAML-based registry of domain types (e.g., `us_state_code`, `email`, `phone_number`, `npi`, `ssn`).

## Usage

```python
from tablespec import DomainTypeInference, DomainTypeRegistry

# List available domain types
registry = DomainTypeRegistry()
print(registry.list_domain_types())

# Infer domain type for a column
inference = DomainTypeInference()
domain_type, confidence = inference.infer_domain_type(
    "member_state_code",
    description="State where member resides",
    sample_values=["CA", "NY", "TX"],
)
# domain_type="us_state_code", confidence=0.95
```
