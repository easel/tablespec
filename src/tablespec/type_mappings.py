"""Type mapping utilities for converting between different type systems."""


def map_to_gx_spark_type(data_type: str) -> str:
    """Map UMF data type to Great Expectations Spark type name.

    These are the PySpark type names that Great Expectations expects when validating
    Spark DataFrames using expect_column_values_to_be_of_type.

    Args:
    ----
        data_type: UMF data type (e.g., "VARCHAR", "INTEGER", "DATE")

    Returns:
    -------
        PySpark type name (e.g., "StringType", "IntegerType", "TimestampType")

    """
    mapping = {
        "VARCHAR": "StringType",
        "STRING": "StringType",
        "INTEGER": "IntegerType",
        "INT": "IntegerType",
        "BIGINT": "LongType",
        "SMALLINT": "ShortType",
        "TINYINT": "ByteType",
        "DECIMAL": "DecimalType",
        "FLOAT": "FloatType",
        "DOUBLE": "DoubleType",
        "BOOLEAN": "BooleanType",
        "DATE": "StringType",  # Dates stored as YYYYMMDD strings
        "TIMESTAMP": "TimestampType",
    }
    return mapping.get(data_type.upper(), "StringType")


def map_to_pyspark_type(data_type: str) -> str:
    """Map UMF data type to PySpark type with instantiation.

    Returns type with parentheses for use in StructField definitions.

    Args:
    ----
        data_type: UMF data type (e.g., "VARCHAR", "INTEGER", "DATE")

    Returns:
    -------
        PySpark type with instantiation (e.g., "StringType()", "IntegerType()")

    """
    mapping = {
        "VARCHAR": "StringType()",
        "STRING": "StringType()",
        "INTEGER": "IntegerType()",
        "INT": "IntegerType()",
        "BIGINT": "LongType()",
        "SMALLINT": "ShortType()",
        "TINYINT": "ByteType()",
        "DECIMAL": "DecimalType()",
        "FLOAT": "FloatType()",
        "DOUBLE": "DoubleType()",
        "BOOLEAN": "BooleanType()",
        "DATE": "StringType()",  # Dates stored as YYYYMMDD strings
        "TIMESTAMP": "TimestampType()",
    }
    return mapping.get(data_type.upper(), "StringType()")


def map_to_json_type(data_type: str) -> str:
    """Map UMF data type to JSON schema type.

    Args:
    ----
        data_type: UMF data type (e.g., "VARCHAR", "INTEGER", "DATE")

    Returns:
    -------
        JSON schema type (e.g., "string", "integer", "number")

    """
    mapping = {
        "VARCHAR": "string",
        "STRING": "string",
        "INTEGER": "integer",
        "INT": "integer",
        "BIGINT": "integer",
        "DECIMAL": "number",
        "FLOAT": "number",
        "DOUBLE": "number",
        "BOOLEAN": "boolean",
        "DATE": "string",
        "TIMESTAMP": "string",
    }
    return mapping.get(data_type.upper(), "string")
