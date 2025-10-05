"""Schema profiling and mapping utilities for tablespec.

This module provides tools for mapping profiling data and Spark schemas to UMF format.

Spark-dependent components (SparkToUmfMapper) require installing tablespec[spark]:
    pip install tablespec[spark]
"""

from tablespec.profiling.deequ_mapper import DeequToUmfMapper
from tablespec.profiling.types import ColumnProfile, DataFrameProfile

__all__ = [
    "ColumnProfile",
    "DataFrameProfile",
    "DeequToUmfMapper",
]

# SparkToUmfMapper is available only if pyspark is installed
try:
    from tablespec.profiling.spark_mapper import SparkToUmfMapper  # noqa: F401

    __all__.append("SparkToUmfMapper")
except ImportError:
    # pyspark not available - SparkToUmfMapper won't be exported
    pass
