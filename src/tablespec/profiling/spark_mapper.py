"""Map Spark DataFrame schema to UMF base schema.

Note: This module requires pyspark. Install with: pip install tablespec[spark]
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, ClassVar

from pyspark.sql.types import DecimalType, StructField

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


class SparkToUmfMapper:
    """Maps Spark DataFrame schema to UMF base schema."""

    # Spark type → UMF data_type mapping
    TYPE_MAPPING: ClassVar[dict[str, str]] = {
        "StringType": "STRING",
        "IntegerType": "INTEGER",
        "LongType": "LONG",
        "DoubleType": "DOUBLE",
        "FloatType": "FLOAT",
        "BooleanType": "BOOLEAN",
        "DateType": "DATE",
        "TimestampType": "TIMESTAMP",
        "DecimalType": "DECIMAL",
    }

    def map_dataframe_to_umf(
        self,
        df: DataFrame,
        table_name: str,
        table_type: str = "inferred",
    ) -> dict[str, Any]:
        """Convert Spark DataFrame schema to UMF base structure.

        Args:
        ----
            df: Spark DataFrame
            table_name: Name of the table
            table_type: Type of table (default: "inferred")

        Returns:
        -------
            Dictionary representing base UMF schema

        """
        columns = []

        for field in df.schema.fields:
            column_dict = self._map_field_to_column(field)
            columns.append(column_dict)

        umf = {
            "table_name": table_name,
            "table_type": table_type,
            "columns": columns,
        }

        logger.info(f"Mapped Spark schema to UMF: {len(columns)} columns")
        return umf

    def _map_field_to_column(self, field: StructField) -> dict[str, Any]:
        """Map a Spark StructField to UMF column dict.

        Args:
        ----
            field: Spark StructField

        Returns:
        -------
            Dictionary representing UMF column

        """
        column: dict[str, Any] = {
            "name": field.name,
            "data_type": self._map_spark_type(field.dataType),
            "nullable": field.nullable,
            "description": f"{field.name} (inferred from Spark schema)",
        }

        # Add type-specific attributes
        if isinstance(field.dataType, DecimalType):
            column["precision"] = field.dataType.precision
            column["scale"] = field.dataType.scale

        return column

    def _map_spark_type(self, spark_type: Any) -> str:
        """Map Spark DataType to UMF data_type string.

        Args:
        ----
            spark_type: Spark DataType instance

        Returns:
        -------
            UMF data_type string

        """
        type_name = type(spark_type).__name__
        umf_type = self.TYPE_MAPPING.get(type_name, "STRING")

        if umf_type == "STRING":
            logger.debug(f"Unmapped Spark type {type_name}, defaulting to STRING")

        return umf_type
