# tablespec

Python library for working with table schemas in Universal Metadata Format (UMF).

## Quick Start

```python
from tablespec import load_umf_from_yaml, generate_sql_ddl

umf = load_umf_from_yaml("schema.yaml")
ddl = generate_sql_ddl(umf.model_dump(exclude_none=True))
print(ddl)
```

## Features

- **Type-safe models** — Pydantic models for UMF format with runtime validation
- **Schema generation** — SQL DDL, PySpark StructType, JSON Schema from UMF
- **Great Expectations** — Baseline generation, constraint extraction, validation
- **CLI** — `tablespec validate`, `tablespec generate`, `tablespec convert`
- **Excel round-trip** — Export/import for domain expert collaboration
- **Domain type inference** — Automatic detection of healthcare data types

## Installation

```bash
pip install tablespec --index-url https://easel.github.io/tablespec/simple/

# With PySpark support
pip install tablespec[spark] --index-url https://easel.github.io/tablespec/simple/
```
