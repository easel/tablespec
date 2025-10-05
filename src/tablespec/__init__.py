"""Pure Python utilities for working with table schemas in UMF format."""

from tablespec.gx_baseline import BaselineExpectationGenerator, UmfToGxMapper
from tablespec.gx_constraint_extractor import GXConstraintExtractor
from tablespec.models import (
    UMF,
    ForeignKey,
    Index,
    Nullable,
    ReferencedBy,
    Relationships,
    UMFColumn,
    UMFMetadata,
    ValidationRule,
    ValidationRules,
    load_umf_from_yaml,
    save_umf_to_yaml,
)
from tablespec.profiling import ColumnProfile, DataFrameProfile, DeequToUmfMapper
from tablespec.prompts import (
    _generate_column_validation_prompt,
    _generate_documentation_prompt,
    _generate_relationship_prompt,
    _generate_survivorship_prompt,
    _generate_validation_prompt,
    _has_validation_rules,
    _should_generate_column_prompt,
)
from tablespec.schemas import (
    generate_json_schema,
    generate_pyspark_schema,
    generate_sql_ddl,
)
from tablespec.type_mappings import (
    map_to_gx_spark_type,
    map_to_json_type,
    map_to_pyspark_type,
)
from tablespec.validation import GXExpectationProcessor

__version__ = "0.1.0"

__all__ = [
    "UMF",
    "BaselineExpectationGenerator",
    "ColumnProfile",
    "DataFrameProfile",
    "DeequToUmfMapper",
    "ForeignKey",
    "GXConstraintExtractor",
    "GXExpectationProcessor",
    "Index",
    "Nullable",
    "ReferencedBy",
    "Relationships",
    "UMFColumn",
    "UMFMetadata",
    "UmfToGxMapper",
    "ValidationRule",
    "ValidationRules",
    "_generate_column_validation_prompt",
    "_generate_documentation_prompt",
    "_generate_relationship_prompt",
    "_generate_survivorship_prompt",
    "_generate_validation_prompt",
    "_has_validation_rules",
    "_should_generate_column_prompt",
    "generate_json_schema",
    "generate_pyspark_schema",
    "generate_sql_ddl",
    "load_umf_from_yaml",
    "map_to_gx_spark_type",
    "map_to_json_type",
    "map_to_pyspark_type",
    "save_umf_to_yaml",
]

# SparkToUmfMapper and TableValidator are available only if pyspark is installed (via tablespec[spark])
try:
    from tablespec.profiling import SparkToUmfMapper  # noqa: F401
    from tablespec.validation import VALIDATION_ERROR_SCHEMA, TableValidator  # noqa: F401

    __all__.extend(["VALIDATION_ERROR_SCHEMA", "SparkToUmfMapper", "TableValidator"])
except ImportError:
    # pyspark not available - Spark-dependent classes won't be exported
    pass
