"""UMF Pydantic Models

Type-safe models for Universal Metadata Format (UMF) files.
Provides runtime validation and serialization/deserialization.
"""

from datetime import datetime
from pathlib import Path
from typing import Annotated, Any

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, field_validator


class Nullable(BaseModel):
    """Nullable configuration per Line of Business."""

    MD: bool | None = Field(description="Nullable for Medicaid")
    MP: bool | None = Field(description="Nullable for Medicare Part D")
    ME: bool | None = Field(description="Nullable for Medicare")


class UMFColumn(BaseModel):
    """UMF Column definition."""

    name: Annotated[
        str, StringConstraints(pattern=r"^[A-Za-z][A-Za-z0-9_]*$", max_length=128)
    ] = Field(description="Column name")
    data_type: str = Field(
        description="Column data type",
        pattern=r"^(VARCHAR|DECIMAL|INTEGER|DATE|DATETIME|BOOLEAN|TEXT|CHAR|FLOAT)$",
    )
    position: str | None = Field(
        default=None, description="Excel column position or identifier"
    )
    description: str | None = Field(default=None, description="Column description")
    nullable: Nullable | None = Field(default=None, description="Nullability by LOB")
    sample_values: list[str] | None = Field(
        default=None, description="Sample values for the column"
    )
    length: int | None = Field(
        default=None, ge=1, description="Maximum length for VARCHAR columns"
    )
    precision: int | None = Field(
        default=None, ge=1, description="Precision for DECIMAL columns"
    )
    scale: int | None = Field(
        default=None, ge=0, description="Scale for DECIMAL columns"
    )
    title: str | None = Field(default=None, description="Column title")
    format: str | None = Field(
        default=None,
        description="Unstructured format pattern or example from source specification. "
        "May contain date patterns (YYYY-MM-DD), value enumerations (M, F, U), "
        "structural patterns (State_LOB), or example values ('40', '1.85'). "
        "This field preserves vendor documentation as-is and requires "
        "context-aware interpretation based on the data_type and column purpose.",
    )
    notes: list[str] | None = Field(
        default=None,
        description="Additional notes or business rules from source specification. "
        "Contains unstructured documentation that provides context for the column.",
    )

    @field_validator("length")
    @classmethod
    def length_required_for_varchar(cls, v, info) -> int | None:
        """Validate that VARCHAR columns have length specified."""
        if info.data.get("data_type") == "VARCHAR" and v is None:
            # Warning only - not a hard error for backward compatibility
            pass
        return v

    @field_validator("precision")
    @classmethod
    def precision_recommended_for_decimal(cls, v, info) -> int | None:
        """Validate that DECIMAL columns should have precision specified."""
        if info.data.get("data_type") == "DECIMAL" and v is None:
            # Warning only - not a hard error for backward compatibility
            pass
        return v


class ValidationRule(BaseModel):
    """Individual validation rule."""

    rule_type: str = Field(description="Type of validation rule")
    description: str = Field(description="Rule description")
    severity: str = Field(
        description="Rule severity", pattern=r"^(error|warning|info)$"
    )
    parameters: dict[str, Any] | None = Field(
        default=None, description="Rule parameters"
    )


class ValidationRules(BaseModel):
    """Validation rules for UMF table."""

    table_level: list[ValidationRule] | None = Field(
        default=None, description="Table-level validation rules"
    )
    column_level: dict[str, list[ValidationRule]] | None = Field(
        default=None, description="Column-level validation rules"
    )


class ForeignKey(BaseModel):
    """Foreign key relationship."""

    column: str = Field(description="Source column name")
    references_table: str = Field(description="Referenced table name")
    references_column: str = Field(description="Referenced column name")
    confidence: float | None = Field(
        default=None, ge=0.0, le=1.0, description="Confidence score for relationship"
    )

    # Legacy field support
    references: str | None = Field(
        default=None, description="Legacy format: table.column"
    )
    detection_method: str | None = Field(
        default=None, description="Method used to detect relationship"
    )

    @field_validator("references_table", mode="before")
    @classmethod
    def parse_references_table(cls, v, info) -> str | None:
        """Parse table name from legacy references field."""
        if (
            v is None
            and info.data
            and "references" in info.data
            and info.data["references"]
        ):
            parts = info.data["references"].split(".")
            if len(parts) == 2:
                return parts[0]
        return v

    @field_validator("references_column", mode="before")
    @classmethod
    def parse_references_column(cls, v, info) -> str | None:
        """Parse column name from legacy references field."""
        if (
            v is None
            and info.data
            and "references" in info.data
            and info.data["references"]
        ):
            parts = info.data["references"].split(".")
            if len(parts) == 2:
                return parts[1]
        return v


class ReferencedBy(BaseModel):
    """Reverse foreign key relationship."""

    table: str = Field(description="Referencing table name")
    column: str = Field(description="Referenced column name")
    foreign_key_column: str = Field(description="Foreign key column name")
    confidence: float | None = Field(
        default=None, ge=0.0, le=1.0, description="Confidence score for relationship"
    )


class Index(BaseModel):
    """Database index definition."""

    name: str = Field(description="Index name")
    columns: list[str] = Field(description="Index columns")
    unique: bool = Field(default=False, description="Whether index is unique")
    description: str | None = Field(default=None, description="Index description")


class Relationships(BaseModel):
    """Table relationships."""

    foreign_keys: list[ForeignKey] | None = Field(
        default=None, description="Foreign key relationships"
    )
    referenced_by: list[ReferencedBy] | None = Field(
        default=None, description="Reverse foreign key relationships"
    )
    indexes: list[Index] | None = Field(default=None, description="Database indexes")


class UMFMetadata(BaseModel):
    """Additional UMF metadata."""

    updated_at: datetime | None = Field(
        default=None, description="Last update timestamp"
    )
    created_by: str | None = Field(default=None, description="Creator identifier")
    pipeline_phase: int | None = Field(
        default=None,
        ge=1,
        le=7,
        description="Pipeline phase that created/updated this file",
    )
    source_file_modified: datetime | None = Field(
        default=None, description="Last modified timestamp of the source Excel file"
    )


class UMF(BaseModel):
    """Universal Metadata Format model."""

    version: Annotated[str, StringConstraints(pattern=r"^\d+\.\d+$")] = Field(
        description="UMF format version"
    )
    table_name: Annotated[
        str, StringConstraints(pattern=r"^[A-Za-z][A-Za-z0-9_]*$", max_length=128)
    ] = Field(description="Database table name")
    source_file: str | None = Field(
        default=None, description="Original source file name"
    )
    sheet_name: str | None = Field(
        default=None, description="Excel sheet name if applicable"
    )
    description: str | None = Field(
        default=None, description="Human-readable table description"
    )
    table_type: str | None = Field(
        default=None,
        description="Table classification: data_table, lookup_table, or configuration",
    )
    columns: list[UMFColumn] = Field(
        min_length=1, description="Array of column definitions"
    )
    validation_rules: ValidationRules | None = Field(
        default=None, description="Validation rules added by Phase 4"
    )
    relationships: Relationships | None = Field(
        default=None, description="Table relationships added by Phase 4"
    )
    metadata: UMFMetadata | None = Field(
        default=None, description="Additional metadata"
    )
    config_data: dict[str, Any] | None = Field(
        default=None, description="Configuration data for configuration-type tables"
    )
    lookup_metadata: dict[str, Any] | None = Field(
        default=None,
        description="Lookup table metadata including sample data and structure",
    )

    @field_validator("columns")
    @classmethod
    def unique_column_names(cls, v) -> list[UMFColumn]:
        """Validate that column names are unique."""
        names = [col.name for col in v]
        if len(names) != len(set(names)):
            msg = "Column names must be unique"
            raise ValueError(msg)
        return v

    @field_validator("version")
    @classmethod
    def validate_version_format(cls, v) -> str:
        """Validate version format is numeric."""
        parts = v.split(".")
        try:
            [int(part) for part in parts]
        except ValueError as e:
            msg = f"Invalid version format: {v}"
            raise ValueError(msg) from e
        return v

    model_config = ConfigDict(
        validate_assignment=True,
        extra="forbid",
        json_schema_extra={
            "example": {
                "version": "1.0",
                "table_name": "Medical_Claims",
                "source_file": "Centene Outbound Outreach Data Layouts v2.5 2026.xlsx",
                "sheet_name": "Medical Claims",
                "description": "Healthcare claims and billing information",
                "columns": [
                    {
                        "name": "PBPTYPE",
                        "data_type": "VARCHAR",
                        "position": "A",
                        "description": "Line of Business",
                        "nullable": {"MD": False, "MP": False, "ME": False},
                        "sample_values": ["MEDICAID", "MEDICARE", "MARKETPLACE"],
                        "length": 20,
                    }
                ],
            }
        },
    )


def load_umf_from_yaml(yaml_path: str | Path) -> UMF:
    """Load and validate UMF from YAML file.

    Args:
        yaml_path: Path to UMF YAML file

    Returns:
        Validated UMF model

    Raises:
        ValidationError: If UMF data is invalid
        FileNotFoundError: If file doesn't exist

    """
    from pathlib import Path

    import yaml

    yaml_file = Path(yaml_path)
    if not yaml_file.exists():
        msg = f"UMF file not found: {yaml_file}"
        raise FileNotFoundError(msg)

    with yaml_file.open(encoding="utf-8") as f:
        data = yaml.safe_load(f)

    return UMF(**data)


def save_umf_to_yaml(umf: UMF, yaml_path: str | Path) -> None:
    """Save UMF model to YAML file.

    Args:
        umf: UMF model to save
        yaml_path: Output YAML file path

    """
    from pathlib import Path

    yaml_file = Path(yaml_path)
    yaml_file.parent.mkdir(parents=True, exist_ok=True)

    # Convert to dict and remove None values for cleaner output
    data = umf.model_dump(exclude_none=True)

    import yaml as yaml_lib

    with yaml_file.open("w", encoding="utf-8") as f:
        yaml_lib.dump(
            data, f, default_flow_style=False, allow_unicode=True, sort_keys=False
        )


# Re-export for convenience (valid names only)
__all__ = [
    "UMF",
    "ForeignKey",
    "ReferencedBy",
    "Relationships",
    "UMFColumn",
    "UMFMetadata",
    "ValidationRule",
    "ValidationRules",
    "load_umf_from_yaml",
    "save_umf_to_yaml",
]
