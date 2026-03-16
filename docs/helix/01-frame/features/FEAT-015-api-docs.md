# FEAT-015: Browsable API Documentation

**Status**: Planned
**Priority**: Medium

## Description

Auto-generated API documentation site using MkDocs + mkdocstrings, built from existing docstrings and Pydantic Field descriptions.

## Motivation

The library has 35+ public API symbols, complex nested Pydantic models, and domain-specific concepts. Inline documentation (docstrings, Field descriptions) is good but not browsable or searchable without reading source.

The GitHub Pages site currently serves a PyPI package index, not documentation.

## Planned Approach

- MkDocs with mkdocstrings plugin for auto-generation from type annotations and docstrings
- Pydantic models benefit most since their Field(description=...) metadata is already rich
- Deploy alongside or integrated with the existing GitHub Pages PyPI index

## Source

- Configuration: `mkdocs.yml` (to be created)
- Content: auto-generated from `src/tablespec/` docstrings
