"""Pure Python utilities for working with table schemas in UMF format."""

from tablespec.gx_baseline import BaselineExpectationGenerator, UmfToGxMapper
from tablespec.gx_constraint_extractor import GXConstraintExtractor
from tablespec.models import (
    INGESTED_QUALITY_CHECK_TYPES,
    RAW_VALIDATION_TYPES,
    REDUNDANT_VALIDATION_TYPES,
    UMF,
    Cardinality,
    DerivationCandidate,
    FileFormat,
    FileFormatSpec,
    FilenamePattern,
    ForeignKey,
    IncomingRelationship,
    Index,
    IngestionConfig,
    IngestionExclusionRule,
    JoinViaSpec,
    Nullable,
    OutgoingRelationship,
    OutputConfig,
    PostUpsertRule,
    QualityCheck,
    QualityChecks,
    ReferencedBy,
    RelationshipSummary,
    Relationships,
    Survivorship,
    UMFColumn,
    UMFColumnDerivation,
    UMFMetadata,
    ValidationRule,
    ValidationRules,
    classify_validation_type,
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
    generate_column_validation_prompt,
    generate_documentation_prompt,
    generate_filename_pattern_prompt,
    generate_relationship_prompt,
    generate_survivorship_prompt,
    generate_survivorship_prompt_per_column,
    generate_validation_prompt,
    generate_validation_prompt_per_column,
    has_validation_rules,
    should_generate_column_prompt,
)
from tablespec.schemas import (
    generate_json_schema,
    generate_pyspark_schema,
    generate_sql_ddl,
)
from tablespec.type_mappings import (
    VALID_PYSPARK_TYPES,
    map_pyspark_to_sql_type,
    map_to_gx_spark_type,
    map_to_json_type,
    map_to_pyspark_type,
)
from tablespec.validation import GXExpectationProcessor

__version__ = "0.1.0"

__all__ = [
    "INGESTED_QUALITY_CHECK_TYPES",
    "RAW_VALIDATION_TYPES",
    "REDUNDANT_VALIDATION_TYPES",
    "UMF",
    "BaselineExpectationGenerator",
    "Cardinality",
    "ColumnProfile",
    "DataFrameProfile",
    "DeequToUmfMapper",
    "DerivationCandidate",
    "FileFormat",
    "FileFormatSpec",
    "FilenamePattern",
    "ForeignKey",
    "GXConstraintExtractor",
    "GXExpectationProcessor",
    "IncomingRelationship",
    "Index",
    "IngestionConfig",
    "IngestionExclusionRule",
    "JoinViaSpec",
    "Nullable",
    "OutgoingRelationship",
    "OutputConfig",
    "PostUpsertRule",
    "QualityCheck",
    "QualityChecks",
    "ReferencedBy",
    "RelationshipSummary",
    "Relationships",
    "Survivorship",
    "UMFColumn",
    "UMFColumnDerivation",
    "UMFMetadata",
    "UmfToGxMapper",
    "ValidationRule",
    "ValidationRules",
    "classify_validation_type",
    "generate_column_validation_prompt",
    "generate_documentation_prompt",
    "generate_filename_pattern_prompt",
    "generate_json_schema",
    "generate_pyspark_schema",
    "generate_relationship_prompt",
    "generate_sql_ddl",
    "generate_survivorship_prompt",
    "generate_survivorship_prompt_per_column",
    "generate_validation_prompt",
    "generate_validation_prompt_per_column",
    "has_validation_rules",
    "load_umf_from_yaml",
    "VALID_PYSPARK_TYPES",
    "map_pyspark_to_sql_type",
    "map_to_gx_spark_type",
    "map_to_json_type",
    "map_to_pyspark_type",
    "save_umf_to_yaml",
    "should_generate_column_prompt",
]

# SparkToUmfMapper and TableValidator are available only if pyspark is installed (via tablespec[spark])
try:
    from tablespec.profiling import SparkToUmfMapper  # noqa: F401
    from tablespec.type_mappings import map_to_pyspark_type_obj  # noqa: F401
    from tablespec.validation import VALIDATION_ERROR_SCHEMA, TableValidator  # noqa: F401

    __all__.extend(["VALIDATION_ERROR_SCHEMA", "SparkToUmfMapper", "TableValidator", "map_to_pyspark_type_obj"])
except ImportError:
    # pyspark not available - Spark-dependent classes won't be exported
    pass

try:
    from tablespec.spark_factory import SparkSessionFactory, create_delta_spark_session  # noqa: F401

    __all__.extend(["SparkSessionFactory", "create_delta_spark_session"])
except ImportError:
    # pyspark not available - Spark factory won't be exported
    pass
