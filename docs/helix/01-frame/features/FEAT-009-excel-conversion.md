# FEAT-009: Excel Bidirectional Conversion

**Status**: Implemented
**Priority**: Medium

## Description

Round-trip conversion between Excel workbooks and UMF schemas, designed for non-technical domain expert collaboration.

## Components

### UMF to Excel (`UMFToExcelConverter`)
- Data validation dropdowns for types, nullable, severity
- Column formatting with headers, styles, and conditional formatting
- Helper columns for validation status and error messages

### Excel to UMF (`ExcelToUMFConverter`)
- Strict validation of Excel input against UMF schema rules
- Type inference and constraint extraction from cell values

### Git-Integrated Import (`excel_import_git.py`)
- Atomic per-change commits using UMF diff
- Preserves change attribution in git history

## Dependencies

- openpyxl

## Source

- `src/tablespec/excel_converter.py`
- `src/tablespec/excel_import_git.py`
