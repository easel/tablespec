# FEAT-008: CLI Interface

**Status**: Implemented
**Priority**: High

## Description

Typer-based CLI (`tablespec` command) for schema management, conversion, and validation workflows with Rich output formatting.

## Commands

- **`convert`** - Convert UMF between formats (JSON, split, Excel). Auto-detects input format.
- **`validate`** - Validate a UMF schema with optional pipeline context
- **`info`** - Display summary of a UMF schema (table name, column count, types)
- **`batch-convert`** - Convert all UMF files in a directory to a target format
- **`changelog`** - Generate changelog from git history for a table directory
- **`sync`** - Synchronize baseline validations across table definitions

## Planned Commands

- **`generate`** - Generate SQL DDL, PySpark schema, or JSON Schema from UMF. Supports `--format sql|pyspark|json` and stdout output for piping into CI scripts.

## Dependencies

- typer (CLI framework)
- rich (terminal formatting)
- Conditional: validator module for validate/convert/info commands

## Source

- `src/tablespec/cli.py`
