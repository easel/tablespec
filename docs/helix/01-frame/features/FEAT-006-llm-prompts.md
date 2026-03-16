# FEAT-006: LLM Prompt Generation

**Status**: Implemented
**Priority**: Medium

## Description

Generate structured prompts for LLM-based schema enrichment across documentation, validation, relationships, and survivorship.

## Components

### Documentation (`prompts/documentation.py`)
- `generate_documentation_prompt` - Business purpose, data flow, relationships, compliance

### Validation (`prompts/validation.py`, `prompts/column_validation.py`)
- `generate_validation_prompt` - Table-level multi-column expectations
- `generate_column_validation_prompt` - Single-column expectations with prompt hash tracking
- `has_validation_rules` / `should_generate_column_prompt` - Filtering helpers

### Relationships (`prompts/relationship.py`)
- `generate_relationship_prompt` - FK discovery with cardinality estimation
- Healthcare-domain awareness (member/provider/claim IDs, drug codes)
- Handles both UMF and lookup table directories

### Survivorship (`prompts/survivorship.py`)
- `generate_survivorship_prompt` - Data survivorship and merge logic mapping

### Expectation Guide (`prompts/expectation_guide.py`)
- Loads categorized expectation types from JSON schemas
- Provides parameter requirements, validation, quick reference
- Decision tree for pending implementation expectations

### Filename Pattern (`prompts/filename_pattern.py`)
- `generate_filename_pattern_prompt` - Filename convention and pattern prompts

### Validation Per Column (`prompts/validation_per_column.py`)
- Column-specific validation prompt generation with granular targeting

## Source

- `src/tablespec/prompts/`
