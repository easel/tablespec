# Split-Format UMF

Directory-based UMF storage for git-friendly per-column change tracking. `UMFLoader` auto-detects whether a path is a split directory or a JSON file.

## Directory Structure

```
tables/medical_claims/
├── table.yaml          # Table-level metadata
└── columns/
    ├── claim_id.yaml   # One file per column
    ├── claim_amount.yaml
    └── provider_id.yaml
```

## Loading and Converting

```python
from tablespec import UMFLoader, UMFFormat

loader = UMFLoader()

# Load from any format (auto-detected)
umf = loader.load("tables/medical_claims/")   # split directory
umf = loader.load("medical_claims.json")       # JSON file

# Convert between formats
loader.convert("medical_claims.json", "tables/medical_claims/", target_format=UMFFormat.SPLIT)
loader.convert("tables/medical_claims/", "medical_claims.json", target_format=UMFFormat.JSON)
```
