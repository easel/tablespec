# US-011: Round-Trip UMF Through Excel

**Parent Feature**: [FEAT-009 - Excel Conversion](../features/FEAT-009-excel-conversion.md)

## User Story

**As a** data steward who works primarily in Excel,
**I want to** export a UMF schema to Excel, make edits with validation assistance, and import it back,
**so that** I can review and update table definitions without learning YAML syntax.

## Acceptance Criteria

- [ ] `UMFToExcelConverter` produces a workbook with dropdown validation for data types and nullable values
- [ ] `ExcelToUMFConverter` imports the workbook back to a valid UMF object
- [ ] Round-trip (export then import) preserves all UMF fields
- [ ] Invalid entries in Excel produce clear validation errors on import
