# LLM Prompt Generation

tablespec generates structured prompts for LLM-based enrichment of UMF schemas.

## Available Prompt Generators

```python
from pathlib import Path
from tablespec import (
    generate_documentation_prompt,
    generate_validation_prompt,
    generate_relationship_prompt,
    generate_survivorship_prompt
)

umf_dict = umf.model_dump()

# Generate documentation prompt
doc_prompt = generate_documentation_prompt(umf_dict)
# Asks LLM to enhance table and column descriptions

# Generate validation rules prompt
validation_prompt = generate_validation_prompt(umf_dict)
# Asks LLM to suggest validation rules (uniqueness, ranges, formats)

# Generate relationship prompt (uses UMF directory paths)
relationship_prompt = generate_relationship_prompt(
    Path("tables/medical_claims"),
    Path("tables")
)
# Asks LLM to identify foreign key relationships

# Generate survivorship prompt (uses table name and UMF directory)
survivorship_prompt = generate_survivorship_prompt(
    "Medical_Claims",
    Path("tables/medical_claims")
)
# Asks LLM to suggest survivorship/merge logic for deduplication
```
