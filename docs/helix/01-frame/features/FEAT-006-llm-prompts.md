# FEAT-006: LLM Prompt Generation

**Status**: Implemented
**Priority**: Medium

## Description

Generate structured prompts for LLM-based schema enrichment across documentation, validation, relationships, and survivorship.

## Components

### Documentation (`prompts/documentation.py`)
- `_generate_documentation_prompt` - Business purpose, data flow, relationships, compliance

### Validation (`prompts/validation.py`, `prompts/column_validation.py`)
- `_generate_validation_prompt` - Table-level multi-column expectations
- `_generate_column_validation_prompt` - Single-column expectations with prompt hash tracking
- `_has_validation_rules` / `_should_generate_column_prompt` - Filtering helpers

### Relationships (`prompts/relationship.py`)
- `_generate_relationship_prompt` - FK discovery with cardinality estimation
- Healthcare-domain awareness (member/provider/claim IDs, drug codes)
- Handles both UMF and lookup table directories

### Survivorship (`prompts/survivorship.py`)
- `_generate_survivorship_prompt` - Data survivorship and merge logic mapping

### Expectation Guide (`prompts/expectation_guide.py`)
- Loads categorized expectation types from JSON schemas
- Provides parameter requirements, validation, quick reference
- Decision tree for pending implementation expectations

### Filename Pattern (`prompts/filename_pattern.py`)
- `_generate_filename_pattern_prompt` - Filename convention and pattern prompts

### Validation Per Column (`prompts/validation_per_column.py`)
- Column-specific validation prompt generation with granular targeting

## Source

- `src/tablespec/prompts/`
