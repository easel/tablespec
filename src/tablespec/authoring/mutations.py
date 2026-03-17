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
