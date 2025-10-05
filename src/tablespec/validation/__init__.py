"""Great Expectations validation utilities."""

from .gx_processor import GXExpectationProcessor

# TableValidator requires pyspark - only available with tablespec[spark]
try:
    from .table_validator import VALIDATION_ERROR_SCHEMA, TableValidator

    __all__ = ["VALIDATION_ERROR_SCHEMA", "GXExpectationProcessor", "TableValidator"]
except ImportError:
    # pyspark not available
    __all__ = ["GXExpectationProcessor"]
