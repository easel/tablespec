# Change Management

Detect differences between UMF versions and generate structured changelogs. `UMFDiff` compares two UMF objects to identify column, validation, metadata, and relationship changes. Integrates with git for commit-level change history.

## Diffing UMF Versions

```python
from tablespec import UMFDiff, UMF, load_umf_from_yaml

old_umf = load_umf_from_yaml("v1/schema.yaml")
new_umf = load_umf_from_yaml("v2/schema.yaml")

diff = UMFDiff(old_umf, new_umf)
column_changes = diff.get_column_changes()

for change in column_changes:
    print(change.description())
    # "Add column diagnosis_code"
    # "Modify column claim_amount: data_type changed from INTEGER to DECIMAL"
```

## Generating Changelogs from Git History

```python
from tablespec import ChangelogGenerator

# Generate changelog from git history
generator = ChangelogGenerator(repo_path=".")
changelog = generator.generate(table_path="tables/medical_claims/")
```
