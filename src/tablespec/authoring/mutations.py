"""Pure functions for UMF mutations. Each takes a UMF and returns a new UMF."""

from __future__ import annotations

from typing import Any

from tablespec.models.umf import UMF, UMFColumn


def add_column(umf: UMF, name: str, data_type: str, **kwargs: Any) -> UMF:
    """Add a column. Raises ValueError if name already exists."""
    existing_names = {col.name for col in umf.columns}
    if name in existing_names:
        msg = f"Column '{name}' already exists"
        raise ValueError(msg)

    col = UMFColumn(name=name, data_type=data_type, **kwargs)
    new_columns = [c.model_copy() for c in umf.columns] + [col]
    return umf.model_copy(update={"columns": new_columns})


def remove_column(umf: UMF, name: str) -> UMF:
    """Remove a column. Raises ValueError if name not found."""
    existing_names = {col.name for col in umf.columns}
    if name not in existing_names:
        msg = f"Column '{name}' not found"
        raise ValueError(msg)

    new_columns = [c.model_copy() for c in umf.columns if c.name != name]
    return umf.model_copy(update={"columns": new_columns})


def modify_column(umf: UMF, name: str, **changes: Any) -> UMF:
    """Modify column attributes. Raises ValueError if name not found."""
    existing_names = {col.name for col in umf.columns}
    if name not in existing_names:
        msg = f"Column '{name}' not found"
        raise ValueError(msg)

    new_columns = []
    for col in umf.columns:
        if col.name == name:
            new_columns.append(col.model_copy(update=changes))
        else:
            new_columns.append(col.model_copy())
    return umf.model_copy(update={"columns": new_columns})


def remove_expectation(umf: UMF, expectation_type: str, column: str | None = None) -> tuple[UMF, int]:
    """Remove expectations matching type and optional column. Returns (new_umf, count_removed)."""
    removed = 0

    def _matches(exp: dict[str, Any]) -> bool:
        exp_type = exp.get("type", exp.get("expectation_type", ""))
        exp_col = exp.get("kwargs", {}).get("column")
        if exp_type != expectation_type:
            return False
        if column is not None and exp_col != column:
            return False
        return True

    updates: dict[str, Any] = {}

    # Filter validation_rules.expectations
    if umf.validation_rules and umf.validation_rules.expectations:
        original = umf.validation_rules.expectations
        filtered = [e for e in original if not _matches(e)]
        removed += len(original) - len(filtered)
        new_vr = umf.validation_rules.model_copy(update={"expectations": filtered})
        updates["validation_rules"] = new_vr

    # Filter quality_checks.checks
    if umf.quality_checks and umf.quality_checks.checks:
        original_checks = umf.quality_checks.checks
        filtered_checks = [c for c in original_checks if not _matches(c.expectation)]
        removed += len(original_checks) - len(filtered_checks)
        new_qc = umf.quality_checks.model_copy(update={"checks": filtered_checks})
        updates["quality_checks"] = new_qc

    return umf.model_copy(update=updates) if updates else umf, removed


def rename_column(umf: UMF, old_name: str, new_name: str, *, keep_alias: bool = False) -> UMF:
    """Rename a column. If keep_alias, adds old_name to aliases."""
    existing_names = {col.name for col in umf.columns}
    if old_name not in existing_names:
        msg = f"Column '{old_name}' not found"
        raise ValueError(msg)
    if new_name in existing_names:
        msg = f"Column '{new_name}' already exists"
        raise ValueError(msg)

    new_columns = []
    for col in umf.columns:
        if col.name == old_name:
            updates: dict[str, Any] = {"name": new_name}
            if keep_alias:
                current_aliases = list(col.aliases or [])
                current_aliases.append(old_name)
                updates["aliases"] = current_aliases
            new_columns.append(col.model_copy(update=updates))
        else:
            new_columns.append(col.model_copy())
    return umf.model_copy(update={"columns": new_columns})
