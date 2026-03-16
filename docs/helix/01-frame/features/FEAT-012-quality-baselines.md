# FEAT-012: Quality Baselines

**Status**: Implemented
**Priority**: Medium

## Description

Capture, store, and compare quality baselines from DataFrames for drift detection. Requires PySpark.

## Components

### Baseline Service (`quality/baseline_service.py`) [requires PySpark]
- `BaselineService.capture()` - Capture row counts, column distributions, numeric stats
- `BaselineService.compare()` - Compare current vs previous baseline
- Jensen-Shannon divergence for distribution drift

### Baseline Storage (`quality/baseline_storage.py`)
- `RunBaseline`, `ColumnDistribution`, `NumericStats` models
- `RowCountComparison`, `DistributionComparison`, `RecordComparison` comparison models
- `BaselineWriter` for persistence

### Executor (`quality/executor.py`)
- Quality check execution against baselines

### Sync Baseline (`sync_baseline.py`)
- Synchronize metadata columns and baseline validations across table definitions
- Idempotent operation preserving user customizations
- Conflict detection for modified rule content

## Source

- `src/tablespec/quality/`
- `src/tablespec/sync_baseline.py`
