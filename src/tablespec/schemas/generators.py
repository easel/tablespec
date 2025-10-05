"""Schema Generators - SQL DDL, PySpark, and JSON Schema generation."""

from datetime import UTC, datetime
from typing import Any

from tablespec.type_mappings import map_to_json_type, map_to_pyspark_type


def generate_sql_ddl(umf_data: dict[str, Any]) -> str:
    """Generate SQL DDL from UMF data."""
    table_name = umf_data["table_name"]

    # Use source file modified time if available, otherwise use current time
    metadata = umf_data.get("metadata") or {}
    source_modified = metadata.get("source_file_modified") if metadata else None
    if source_modified:
        # Parse and format the ISO timestamp
        from datetime import datetime as dt

        timestamp = dt.fromisoformat(source_modified).strftime("%Y-%m-%d %H:%M:%S")
    else:
        timestamp = datetime.now(tz=UTC).strftime("%Y-%m-%d %H:%M:%S")

    ddl_lines = [
        f"-- DDL for {table_name}",
        "-- Generated from UMF specification",
        f"-- Source file modified: {timestamp}",
        "",
        f"CREATE TABLE {table_name} (",
    ]

    columns = []
    for col in umf_data["columns"]:
        col_name = col["name"]
        data_type = col.get("data_type", "VARCHAR")
        nullable = "" if col.get("nullable", True) else " NOT NULL"

        # Handle specific data types
        if data_type == "VARCHAR" and col.get("max_length"):
            data_type = f"VARCHAR({col['max_length']})"
        elif data_type == "VARCHAR":
            # Spark SQL requires size for VARCHAR; use STRING when unspecified
            data_type = "STRING"
        elif data_type == "DECIMAL" and col.get("precision"):
            precision = col["precision"]
            scale = col.get("scale", 0)
            data_type = f"DECIMAL({precision},{scale})"

        col_def = f"    {col_name} {data_type}{nullable}"

        # Add comment if description available
        if col.get("description"):
            escaped_desc = col["description"].replace("'", "''")[:255]
            col_def += f" COMMENT '{escaped_desc}'"

        columns.append(col_def)

    ddl_lines.append(",\n".join(columns))
    ddl_lines.append(")")

    # Add table comment
    if umf_data.get("description"):
        escaped_table_desc = umf_data["description"].replace("'", "''")[:255]
        ddl_lines.append(f"COMMENT '{escaped_table_desc}'")

    ddl_lines.append(";")

    # Add indexes if available
    relationships = umf_data.get("relationships") or {}
    if relationships.get("suggested_indexes"):
        ddl_lines.append("")
        ddl_lines.append("-- Suggested Indexes")

        for idx in umf_data["relationships"]["suggested_indexes"]:
            idx_name = idx["name"]
            idx_columns = ", ".join(idx["columns"])
            ddl_lines.append(
                f"CREATE INDEX {idx_name} ON {table_name} ({idx_columns});"
            )

    return "\n".join(ddl_lines)


def generate_pyspark_schema(umf_data: dict[str, Any]) -> str:
    """Generate PySpark schema from UMF data."""
    table_name = umf_data["table_name"]

    # Use source file modified time if available, otherwise use current time
    metadata = umf_data.get("metadata") or {}
    source_modified = metadata.get("source_file_modified") if metadata else None
    if source_modified:
        # Parse and format the ISO timestamp
        from datetime import datetime as dt

        timestamp = dt.fromisoformat(source_modified).strftime("%Y-%m-%d %H:%M:%S")
    else:
        timestamp = datetime.now(tz=UTC).strftime("%Y-%m-%d %H:%M:%S")

    schema_lines = [
        f"# PySpark Schema for {table_name}",
        "# Generated from UMF specification",
        f"# Source file modified: {timestamp}",
        "",
        "from pyspark.sql.types import StructType, StructField",
        "from pyspark.sql.types import StringType, IntegerType, LongType, DecimalType",
        "from pyspark.sql.types import FloatType, DoubleType, BooleanType, DateType, TimestampType",
        "",
        f"{table_name.lower()}_schema = StructType([",
    ]

    fields = []
    for col in umf_data["columns"]:
        col_name = col["name"]
        data_type = col.get("data_type", "VARCHAR")
        nullable = col.get("nullable", True)

        # Map data types to PySpark types
        pyspark_type = map_to_pyspark_type(data_type)

        field_def = f'    StructField("{col_name}", {pyspark_type}, {nullable})'
        fields.append(field_def)

    schema_lines.append(",\n".join(fields))
    schema_lines.append("])")

    return "\n".join(schema_lines)


def generate_json_schema(umf_data: dict[str, Any]) -> dict[str, Any]:
    """Generate JSON schema from UMF data."""
    table_name = umf_data["table_name"]

    schema = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": f"{table_name} Schema",
        "type": "object",
        "description": umf_data.get("description", f"Schema for {table_name} table"),
        "properties": {},
        "required": [],
    }

    for col in umf_data["columns"]:
        col_name = col["name"]
        col_desc = col.get("description", "")
        nullable = col.get("nullable", True)

        # Map data type to JSON schema type
        json_type = map_to_json_type(col.get("data_type", "VARCHAR"))

        prop = {"type": json_type, "description": col_desc}

        # Add additional constraints
        if col.get("max_length"):
            prop["maxLength"] = col["max_length"]

        if col.get("sample_values"):
            prop["examples"] = col["sample_values"][:3]

        schema["properties"][col_name] = prop

        # Add to required if not nullable
        if not nullable:
            schema["required"].append(col_name)

    return schema
