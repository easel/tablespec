# Sample Data Generation

Generate realistic, constraint-aware sample data from UMF specifications. Supports healthcare-specific generators (SSN, NPI, drug codes), foreign key relationship graphs for referential integrity, and CSV/JSON output.

## Usage

```python
from tablespec import SampleDataGenerator, GenerationConfig

config = GenerationConfig(record_count=100, seed=42)
generator = SampleDataGenerator(
    input_dir="tables/",
    output_dir="sample_output/",
    config=config,
)
generator.generate()
# Produces CSV files in sample_output/ with realistic, relationship-aware data
```
