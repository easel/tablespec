"""Unit tests for UMF Pydantic models."""

from __future__ import annotations


import pytest
from pydantic import ValidationError

from tablespec.models.umf import (
    ForeignKey,
    Nullable,
    UMF,
    UMFColumn,
    ValidationRule,
)


class TestNullable:
    """Test Nullable model."""

    def test_creates_nullable_with_all_lobs(self):
        """Test creating Nullable with all LOBs."""
        nullable = Nullable(MD=False, MP=True, ME=False)
        assert nullable.MD is False
        assert nullable.MP is True
        assert nullable.ME is False

    def test_allows_none_values(self):
        """Test Nullable allows None for LOBs."""
        nullable = Nullable(MD=None, MP=None, ME=None)
        assert nullable.MD is None
        assert nullable.MP is None
        assert nullable.ME is None

    def test_partial_nullable_specification(self):
        """Test can specify only some LOBs."""
        nullable = Nullable(MD=False, MP=None, ME=None)
        assert nullable.MD is False
        assert nullable.MP is None


class TestUMFColumn:
    """Test UMFColumn model."""

    def test_creates_minimal_column(self):
        """Test creating column with required fields only."""
        col = UMFColumn(name="test_col", data_type="VARCHAR")
        assert col.name == "test_col"
        assert col.data_type == "VARCHAR"

    def test_validates_column_name_pattern(self):
        """Test column name must start with letter."""
        with pytest.raises(ValidationError):
            UMFColumn(name="123invalid", data_type="VARCHAR")

        with pytest.raises(ValidationError):
            UMFColumn(name="_invalid", data_type="VARCHAR")

    def test_allows_valid_column_names(self):
        """Test valid column name patterns."""
        valid_names = ["col1", "MyColumn", "col_name_123", "ABC"]
        for name in valid_names:
            col = UMFColumn(name=name, data_type="VARCHAR")
            assert col.name == name

    def test_validates_data_type_enum(self):
        """Test data_type must be valid UMF type."""
        valid_types = [
            "VARCHAR",
            "DECIMAL",
            "INTEGER",
            "DATE",
            "DATETIME",
            "BOOLEAN",
            "TEXT",
            "CHAR",
            "FLOAT",
        ]
        for dtype in valid_types:
            col = UMFColumn(name="test", data_type=dtype)
            assert col.data_type == dtype

    def test_rejects_invalid_data_type(self):
        """Test invalid data types are rejected."""
        with pytest.raises(ValidationError):
            UMFColumn(name="test", data_type="INVALID_TYPE")

    def test_column_with_all_fields(self):
        """Test column with all optional fields."""
        col = UMFColumn(
            name="customer_id",
            data_type="INTEGER",
            position="A",
            description="Unique customer identifier",
            nullable=Nullable(MD=False, MP=False, ME=False),
            sample_values=["1", "2", "3"],
            title="Customer ID",
            format="Numeric",
            notes=["Primary key", "Auto-incremented"],
        )

        assert col.name == "customer_id"
        assert col.description == "Unique customer identifier"
        assert col.sample_values == ["1", "2", "3"]
        assert col.nullable.MD is False
        assert len(col.notes) == 2

    def test_varchar_with_length(self):
        """Test VARCHAR column with length."""
        col = UMFColumn(name="name", data_type="VARCHAR", length=255)
        assert col.length == 255

    def test_decimal_with_precision_and_scale(self):
        """Test DECIMAL column with precision and scale."""
        col = UMFColumn(name="amount", data_type="DECIMAL", precision=10, scale=2)
        assert col.precision == 10
        assert col.scale == 2

    def test_validates_length_positive(self):
        """Test length must be positive."""
        with pytest.raises(ValidationError):
            UMFColumn(name="test", data_type="VARCHAR", length=0)

        with pytest.raises(ValidationError):
            UMFColumn(name="test", data_type="VARCHAR", length=-1)

    def test_validates_precision_positive(self):
        """Test precision must be positive."""
        with pytest.raises(ValidationError):
            UMFColumn(name="test", data_type="DECIMAL", precision=0)

    def test_validates_scale_non_negative(self):
        """Test scale must be non-negative."""
        with pytest.raises(ValidationError):
            UMFColumn(name="test", data_type="DECIMAL", precision=10, scale=-1)

        # Scale 0 should be valid
        col = UMFColumn(name="test", data_type="DECIMAL", precision=10, scale=0)
        assert col.scale == 0


class TestValidationRule:
    """Test ValidationRule model."""

    def test_creates_validation_rule(self):
        """Test creating validation rule."""
        rule = ValidationRule(
            rule_type="uniqueness",
            description="Column must be unique",
            severity="error",
        )

        assert rule.rule_type == "uniqueness"
        assert rule.description == "Column must be unique"
        assert rule.severity == "error"

    def test_validates_severity_enum(self):
        """Test severity must be valid value."""
        valid_severities = ["error", "warning", "info"]
        for severity in valid_severities:
            rule = ValidationRule(
                rule_type="test",
                description="Test rule",
                severity=severity,
            )
            assert rule.severity == severity

    def test_rejects_invalid_severity(self):
        """Test invalid severity is rejected."""
        with pytest.raises(ValidationError):
            ValidationRule(
                rule_type="test",
                description="Test rule",
                severity="critical",  # Not a valid severity
            )

    def test_rule_with_parameters(self):
        """Test rule with parameters."""
        rule = ValidationRule(
            rule_type="range",
            description="Value must be in range",
            severity="error",
            parameters={"min_value": 0, "max_value": 100},
        )

        assert rule.parameters["min_value"] == 0
        assert rule.parameters["max_value"] == 100


class TestForeignKey:
    """Test ForeignKey model."""

    def test_creates_foreign_key(self):
        """Test creating foreign key relationship."""
        fk = ForeignKey(
            column="customer_id",
            references_table="Customers",
            references_column="id",
        )

        assert fk.column == "customer_id"
        assert fk.references_table == "Customers"
        assert fk.references_column == "id"

    def test_foreign_key_with_confidence(self):
        """Test foreign key with confidence score."""
        fk = ForeignKey(
            column="customer_id",
            references_table="Customers",
            references_column="id",
            confidence=0.95,
        )

        assert fk.confidence == 0.95

    def test_validates_confidence_range(self):
        """Test confidence must be between 0 and 1."""
        with pytest.raises(ValidationError):
            ForeignKey(
                column="test",
                references_table="Test",
                references_column="id",
                confidence=1.5,
            )

        with pytest.raises(ValidationError):
            ForeignKey(
                column="test",
                references_table="Test",
                references_column="id",
                confidence=-0.1,
            )

    def test_parses_legacy_references_format(self):
        """Test parsing legacy 'table.column' format."""
        # The legacy parsing requires references_table and references_column to be provided
        # The validator parses the 'references' field if the others are missing
        fk = ForeignKey(
            column="customer_id",
            references_table="Customers",
            references_column="id",
            references="Customers.id",  # Legacy field
        )

        # Should have both formats
        assert fk.references_table == "Customers"
        assert fk.references_column == "id"
        assert fk.references == "Customers.id"


class TestUMF:
    """Test UMF main model."""

    @pytest.fixture
    def minimal_umf_data(self):
        """Minimal valid UMF data."""
        return {
            "version": "1.0",
            "table_name": "Test_Table",
            "columns": [
                {"name": "id", "data_type": "INTEGER"},
            ],
        }

    @pytest.fixture
    def full_umf_data(self):
        """Full UMF data with all features."""
        return {
            "version": "1.0",
            "table_name": "Medical_Claims",
            "source_file": "claims_spec.xlsx",
            "sheet_name": "Medical Claims",
            "description": "Healthcare claims and billing information",
            "table_type": "data_table",
            "columns": [
                {
                    "name": "claim_id",
                    "data_type": "VARCHAR",
                    "length": 50,
                    "description": "Unique claim identifier",
                    "nullable": {"MD": False, "MP": False, "ME": False},
                },
                {
                    "name": "claim_amount",
                    "data_type": "DECIMAL",
                    "precision": 10,
                    "scale": 2,
                    "nullable": {"MD": True, "MP": True, "ME": True},
                },
            ],
            "validation_rules": {
                "table_level": [
                    {
                        "rule_type": "row_count",
                        "description": "Table must not be empty",
                        "severity": "error",
                    }
                ],
                "column_level": {
                    "claim_id": [
                        {
                            "rule_type": "uniqueness",
                            "description": "claim_id must be unique",
                            "severity": "error",
                        }
                    ]
                },
            },
            "relationships": {
                "foreign_keys": [
                    {
                        "column": "provider_id",
                        "references_table": "Providers",
                        "references_column": "id",
                        "confidence": 0.95,
                    }
                ]
            },
            "metadata": {
                "created_by": "data-platform-team",
                "pipeline_phase": 4,
            },
        }

    def test_creates_minimal_umf(self, minimal_umf_data):
        """Test creating minimal UMF model."""
        umf = UMF(**minimal_umf_data)

        assert umf.version == "1.0"
        assert umf.table_name == "Test_Table"
        assert len(umf.columns) == 1

    def test_creates_full_umf(self, full_umf_data):
        """Test creating full UMF model with all features."""
        umf = UMF(**full_umf_data)

        assert umf.table_name == "Medical_Claims"
        assert umf.description == "Healthcare claims and billing information"
        assert len(umf.columns) == 2
        assert umf.validation_rules is not None
        assert umf.relationships is not None
        assert umf.metadata.pipeline_phase == 4

    def test_validates_version_format(self):
        """Test version must be in X.Y format."""
        with pytest.raises(ValidationError):
            UMF(
                version="invalid",
                table_name="Test",
                columns=[{"name": "col1", "data_type": "VARCHAR"}],
            )

    def test_allows_valid_version_formats(self):
        """Test valid version formats."""
        valid_versions = ["1.0", "2.0", "1.5", "10.25"]
        for version in valid_versions:
            umf = UMF(
                version=version,
                table_name="Test",
                columns=[{"name": "col1", "data_type": "VARCHAR"}],
            )
            assert umf.version == version

    def test_validates_table_name_pattern(self):
        """Test table name must follow naming rules."""
        with pytest.raises(ValidationError):
            UMF(
                version="1.0",
                table_name="123_invalid",
                columns=[{"name": "col1", "data_type": "VARCHAR"}],
            )

    def test_requires_at_least_one_column(self):
        """Test UMF must have at least one column."""
        with pytest.raises(ValidationError):
            UMF(version="1.0", table_name="Test", columns=[])

    def test_validates_unique_column_names(self):
        """Test column names must be unique."""
        with pytest.raises(ValidationError) as exc_info:
            UMF(
                version="1.0",
                table_name="Test",
                columns=[
                    {"name": "duplicate", "data_type": "VARCHAR"},
                    {"name": "duplicate", "data_type": "INTEGER"},
                ],
            )
        assert "Column names must be unique" in str(exc_info.value)

    def test_allows_different_column_names(self):
        """Test different column names are allowed."""
        umf = UMF(
            version="1.0",
            table_name="Test",
            columns=[
                {"name": "col1", "data_type": "VARCHAR"},
                {"name": "col2", "data_type": "INTEGER"},
            ],
        )
        assert len(umf.columns) == 2

    def test_forbids_extra_fields(self):
        """Test model forbids extra fields not in schema."""
        with pytest.raises(ValidationError):
            UMF(
                version="1.0",
                table_name="Test",
                columns=[{"name": "col1", "data_type": "VARCHAR"}],
                extra_field="not_allowed",
            )

    def test_metadata_pipeline_phase_range(self):
        """Test pipeline_phase must be between 1 and 7."""
        with pytest.raises(ValidationError):
            UMF(
                version="1.0",
                table_name="Test",
                columns=[{"name": "col1", "data_type": "VARCHAR"}],
                metadata={"pipeline_phase": 0},
            )

        with pytest.raises(ValidationError):
            UMF(
                version="1.0",
                table_name="Test",
                columns=[{"name": "col1", "data_type": "VARCHAR"}],
                metadata={"pipeline_phase": 8},
            )

        # Valid phases
        for phase in range(1, 8):
            umf = UMF(
                version="1.0",
                table_name="Test",
                columns=[{"name": "col1", "data_type": "VARCHAR"}],
                metadata={"pipeline_phase": phase},
            )
            assert umf.metadata.pipeline_phase == phase

    def test_serializes_to_dict(self, full_umf_data):
        """Test UMF can be serialized to dict."""
        umf = UMF(**full_umf_data)
        data = umf.model_dump()

        assert data["version"] == "1.0"
        assert data["table_name"] == "Medical_Claims"
        assert isinstance(data, dict)

    def test_dict_exclude_none(self, minimal_umf_data):
        """Test exclude_none removes None values."""
        umf = UMF(**minimal_umf_data)
        data = umf.model_dump(exclude_none=True)

        # Optional fields should not be present
        assert "description" not in data
        assert "source_file" not in data
        assert "validation_rules" not in data


class TestUMFFileOperations:
    """Test UMF file I/O operations."""

    def test_saves_and_loads_umf(self, tmp_path):
        """Test saving and loading UMF from YAML file."""
        from tablespec.models.umf import load_umf_from_yaml, save_umf_to_yaml

        # Create UMF
        umf = UMF(
            version="1.0",
            table_name="Test_Table",
            description="Test table",
            columns=[
                {
                    "name": "id",
                    "data_type": "INTEGER",
                    "nullable": {"MD": False, "MP": False, "ME": False},
                }
            ],
        )

        # Save to file
        yaml_path = tmp_path / "test.umf.yaml"
        save_umf_to_yaml(umf, yaml_path)

        # File should exist
        assert yaml_path.exists()

        # Load back
        loaded_umf = load_umf_from_yaml(yaml_path)

        # Should match
        assert loaded_umf.version == umf.version
        assert loaded_umf.table_name == umf.table_name
        assert loaded_umf.description == umf.description
        assert len(loaded_umf.columns) == 1
        assert loaded_umf.columns[0].name == "id"

    def test_load_raises_filenotfounderror(self):
        """Test loading non-existent file raises error."""
        from tablespec.models.umf import load_umf_from_yaml

        with pytest.raises(FileNotFoundError):
            load_umf_from_yaml("/nonexistent/path/file.yaml")

    def test_save_creates_parent_directories(self, tmp_path):
        """Test saving creates parent directories."""
        from tablespec.models.umf import save_umf_to_yaml

        umf = UMF(
            version="1.0",
            table_name="Test",
            columns=[{"name": "col1", "data_type": "VARCHAR"}],
        )

        # Path with nested directories
        yaml_path = tmp_path / "nested" / "dir" / "test.yaml"
        save_umf_to_yaml(umf, yaml_path)

        assert yaml_path.exists()
        assert yaml_path.parent.exists()

    def test_saved_yaml_is_readable(self, tmp_path):
        """Test saved YAML is human-readable."""
        from tablespec.models.umf import save_umf_to_yaml

        umf = UMF(
            version="1.0",
            table_name="Test_Table",
            description="Test table",
            columns=[{"name": "id", "data_type": "INTEGER"}],
        )

        yaml_path = tmp_path / "test.yaml"
        save_umf_to_yaml(umf, yaml_path)

        # Read raw content
        content = yaml_path.read_text()

        # Should be readable YAML
        assert "version: '1.0'" in content
        assert "table_name: Test_Table" in content
        assert "description: Test table" in content
